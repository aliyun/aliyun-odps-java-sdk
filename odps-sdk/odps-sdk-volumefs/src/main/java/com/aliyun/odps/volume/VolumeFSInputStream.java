/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aliyun.odps.volume;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.VolumeException;
import com.aliyun.odps.fs.VolumeFileSystemConfigKeys;
import com.aliyun.odps.volume.protocol.VolumeFSConstants;

/**
 * VolumeFSInputStream
 *
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFSInputStream extends FSInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeFSInputStream.class);


  private VolumeFSClient volumeFSClient;

  private String path;

  private boolean closed = true;

  private Long fileLength;

  private long pos = 0;
  /**
   * unit:byte
   */
  private long blockSize;

  private String uuid;

  private Map<Long, Integer> cache = new ConcurrentHashMap<Long, Integer>();

  private InputStream in;

  private RandomAccessFile raf;

  private long curIndex = 0;

  private File buffer_block_dir;

  private boolean isSeeked = false;

  private boolean isCache = false;

  private boolean seekOptimization;

  private static final Integer STATUS_DOWNLOADING = 0;

  private static final Integer STATUS_DOWNLOADED = 1;

  private ExecutorService executorService;

  private boolean isPreLoaded = false;

  public VolumeFSInputStream(String path, VolumeFSClient volumeClient, Long fileLength,
      Configuration conf) throws IOException {
    this.path = path;
    this.volumeFSClient = volumeClient;
    this.seekOptimization =
        conf.getBoolean(VolumeFileSystemConfigKeys.ODPS_VOLUME_SEEK_OPTIMIZATION_ENABLED, false);
    if (this.seekOptimization) {
      this.blockSize =
          conf.getLong(VolumeFileSystemConfigKeys.ODPS_VOLUME_BLOCK_SIZE,
              VolumeFSConstants.DEFAULT_VOLUME_BLOCK_SIZE);
    }
    this.fileLength = fileLength;
    this.closed = false;
    this.uuid = UUID.randomUUID().toString();
    buffer_block_dir =
        new File(conf.get(VolumeFileSystemConfigKeys.ODPS_VOLUME_BLOCK_BUFFER_DIR,
            VolumeFSConstants.DEFAULT_VOLUME_BLOCK_BUFFER_DIR));
    if (!buffer_block_dir.exists() && !buffer_block_dir.mkdirs()) {
      throw new IOException("Cannot create Volume block buffer directory: " + buffer_block_dir);
    }
    if (seekOptimization) {
      executorService = Executors.newFixedThreadPool(1);
    }
  }



  @Override
  public synchronized void seek(long pos) throws IOException {
    checkClosed();
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    if (pos > fileLength) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
    }
    this.pos = pos;
    this.isSeeked = true;
  }

  @Override
  public synchronized long getPos() throws IOException {
    checkClosed();
    return pos;
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    checkClosed();
    seek(targetPos);
    try {
      in = getInputStream();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public int read() throws IOException {
    byte[] buf = new byte[1];
    int len = read(buf, 0, 1);
    if (len == 1) {
      return (int) buf[0] & 0xFF;
    }
    return VolumeFSConstants.EOF;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    // Inline to optimize performance
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    if (pos >= fileLength) {
      return VolumeFSConstants.EOF;
    }
    if (in == null || isSeeked || (seekOptimization && isCache && pos / blockSize != curIndex)) {
      in = getInputStream();
    }
    int avaliable = in.read(buf, off, len);
    pos = pos + avaliable;
    if (avaliable == len || pos >= fileLength) {
      return avaliable;
    } else {
      IOUtils.closeQuietly(in);
      in = null;
      return avaliable + read(buf, off + avaliable, len - avaliable);
    }
  }

  private synchronized InputStream getInputStream() throws IOException {
    if (seekOptimization) {
      long index = pos / blockSize;
      if (!isPreLoaded) {
        isPreLoaded = true;
        seekOptimization();
      }
      if (in != null && index == curIndex && isCache && isSeeked) {
        in = Channels.newInputStream(raf.getChannel().position(pos - curIndex * blockSize));
        return in;
      } else {
        IOUtils.closeQuietly(in);
        IOUtils.closeQuietly(raf);
        if (cache.get(index) != STATUS_DOWNLOADED) {
          curIndex = index;
          return getRemoteInputStream();
        } else {
          File blockFile = new File(buffer_block_dir, getBlockFileName(index));
          if (!blockFile.exists()) {
            cache.remove(index);
            curIndex = index;
            return getRemoteInputStream();
          }
          return getLocalInputStream(index);
        }
      }
    } else {
      IOUtils.closeQuietly(in);
      return getRemoteInputStream();
    }

  }


  private synchronized InputStream getLocalInputStream(long index) throws FileNotFoundException,
      IOException {
    if (!seekOptimization) {
      return null;
    }
    isCache = true;
    File blockFile = new File(buffer_block_dir, getBlockFileName(index));
    raf = new RandomAccessFile(blockFile, "r");
    return Channels.newInputStream(raf.getChannel().position(pos - curIndex * blockSize));
  }


  private synchronized InputStream getRemoteInputStream() throws IOException {
    isSeeked = false;
    isCache = false;
    try {
      return volumeFSClient.openInputStream(path, pos, fileLength - 1);
    } catch (VolumeException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  private void seekOptimization() {
    long totalBlockNum = (long) Math.ceil(fileLength.doubleValue() / blockSize);
    if (totalBlockNum > 2) {
      for (long i = 2l; i < totalBlockNum; i++) {
        if (!cache.containsKey(i)) {
          cache.put(i, STATUS_DOWNLOADING);
          executorService.submit(new SeekOptimizationWorker(i));
        }
      }
    }
    for (long i = totalBlockNum - 1; i >= 0; i--) {
      if (!cache.containsKey(i)) {
        cache.put(i, STATUS_DOWNLOADING);
        executorService.submit(new SeekOptimizationWorker(i));
      }
    }
  }

  @Override
  public synchronized int available() throws IOException {
    checkClosed();
    return (int) (fileLength - pos);
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      IOUtils.closeQuietly(in);
      IOUtils.closeQuietly(raf);
      if (executorService != null)
        executorService.shutdownNow();
    }
  }

  private synchronized void checkClosed() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  private String getBlockFileName(long index) {
    return "volumefs-input-block-" + uuid + "-" + index + ".tmp";
  }

  class SeekOptimizationWorker implements Runnable {

    private Long index;

    SeekOptimizationWorker(Long index) {
      this.index = index;
    }

    @Override
    public void run() {
      long start = index * blockSize;
      if (start > fileLength - 1)
        return;
      long end = Math.min((index + 1) * blockSize - 1, fileLength - 1);
      File blockFile = new File(buffer_block_dir, getBlockFileName(index));
      blockFile.deleteOnExit();
      try {
        volumeFSClient.downloadFile(path, start, end, blockFile, false);
        cache.put(index, STATUS_DOWNLOADED);
      } catch (VolumeException e) {
        cache.remove(index);
        LOG.error(e.getMessage(), e);
      }
    }
  }
}
