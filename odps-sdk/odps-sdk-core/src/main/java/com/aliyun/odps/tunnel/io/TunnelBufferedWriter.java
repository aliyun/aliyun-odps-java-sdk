/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.tunnel.io;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TunnelMetrics;

/**
 * <p>TunnelBufferedWriter 是一个<b>使用缓冲区</b>的、<b>容错</b>的 Tunnel 上传接口。</p>
 *
 * <p>通过调用 write 接口将 record 写入缓冲区，当缓冲区大小超过 bufferSize 时将触发上传动作。
 * 上传过程中如果发生错误将自动进行重试。</p>
 *
 * <h3>TunnelBufferedWriter 和 TunnelRecordWriter 有什么区别？</h3>
 *
 * <p>和 {@link TunnelRecordWriter} 的不同之处在于，在使用 {@link TunnelRecordWriter} 时用户
 * 需要先划分数据块，然后对每一个数据块分别：</p>
 *
 * <ol>
 *   <li>打开一个 {@link TunnelRecordWriter}</li>
 *   <li></li>写入记录
 *   <li>关闭 {@link TunnelRecordWriter}</li>
 *   <li>提交这个数据块</li>
 * </ol>
 *
 * <p>这个过程中用户需要自己来容错(例如记录下上传失败的 block，以便重新上传)。而 TunnelBufferedWriter 隐藏了数据块
 * 的细节，并将记录持久化在内存中，用户在会话中打开以后，就可以往里面写记录，TunnelBufferedWriter 会尽最大可能容错，
 * 保证数据上传上去。降低了使用的门槛。不过由于隐藏了数据块的细节，TunnelBufferedWriter 并不适合断点续传的场景。</p>
 *
 * <h3>代码示例</h3>
 *
 * <pre>
 * class UploadThread extends Thread {
 *   private UploadSession session;
 *   private static int RECORD_COUNT = 1200;
 *
 *   public UploadThread(UploadSession session) {
 *     this.session = session;
 *   }
 *
 *   @Override
 *   public void run() {
 *     RecordWriter writer = up.openBufferedWriter();
 *     Record r1 = up.newRecord();
 *     for (int i = 0; i < RECORD_COUNT; i++) {
 *       r1.setBigint(0, 1L);
 *       writer.write(r1);
 *     }
 *     writer.close();
 *   }
 * };
 *
 * public class Example {
 *   public static void main(String args[]) {
 *
 *    // 初始化 ODPS 和 tunnel 的代码
 *
 *    TableTunnel.UploadSession up = tunnel.createUploadSession(projectName, tableName);
 *    UploadThread t1 = new UploadThread(up);
 *    UploadThread t2 = new UploadThread(up);
 *
 *    t1.start();
 *    t2.start();
 *    t1.join();
 *    t2.join();
 *
 *    up.commit();
 *  }
 *}
 * </pre>
 *
 * @author onesuper(yichao.cheng@alibaba-inc.com)
 */
public class TunnelBufferedWriter implements RecordWriter {

  private ProtobufRecordPack bufferedPack;
  private TableTunnel.UploadSession session;

  private ProtobufRecordPack flushPack; // For async mode
  private Future<?> flushFuture; // For async mode
  private ExecutorService flushService; // For async mode

  private long bufferSize;
  private float flushThreshold;
  private long bytesWritten;
  private boolean isClosed;
  private long timeout;
  private TunnelMetrics metrics;
  private TableTunnel.BlockVersionProvider versionProvider;
  private final CompressOption compressOption;


  private static final long BUFFER_SIZE_DEFAULT = 64 * 1024 * 1024;
  private static final long BUFFER_SIZE_MIN = 1024 * 1024;
  private static final long BUFFER_SIZE_MAX = 1000 * 1024 * 1024;
  private static final float FLUSH_THRESHOLD_DEFAULT = 0.9F;
  private static final float FLUSH_THRESHOLD_MIN = 0.01F;
  private static final float FLUSH_THRESHOLD_MAX = 0.99F;

  /**
   * 构造此类对象，使用默认缓冲区大小为 64 MiB，和默认的回退策略：4s、8s、16s、32s、64s、128s
   *
   * @param session
   *    {@link  TableTunnel.UploadSession}
   * @param option
   *    {@link CompressOption}
   *
   * @throws IOException
   *    Signals that an I/O exception has occurred.
   */
  public TunnelBufferedWriter(TableTunnel.UploadSession session, CompressOption option)
      throws IOException {
    this.bufferedPack = (ProtobufRecordPack)session.newRecordPack(option);
    this.session = session;
    this.compressOption = option;
    this.bufferSize = BUFFER_SIZE_DEFAULT;
    this.flushThreshold = FLUSH_THRESHOLD_DEFAULT;
    this.bytesWritten = 0;
    this.isClosed = false;
    this.metrics = bufferedPack.getMetrics();
  }

  /**
   * 构造此类对象，使用默认缓冲区大小为 64 MiB，和默认的回退策略：4s、8s、16s、32s、64s、128s
   *
   * @param session
   *    {@link  TableTunnel.UploadSession}
   * @param option
   *    {@link CompressOption}
   * @param timeout
   *    超时时间 单位 ms <=0 代表无超时
   *
   * @throws IOException
   *    Signals that an I/O exception has occurred.
   */
  public TunnelBufferedWriter(TableTunnel.UploadSession session, CompressOption option, long timeout, TableTunnel.BlockVersionProvider versionProvider)
          throws IOException {
    this(session, option);
    this.timeout = timeout;
    this.versionProvider = versionProvider;
  }

  /**
   * 设置缓冲区大小
   *
   * @param bufferSize
   *     缓冲区大小字节，可以设置的最小值 1 MiB，最大值为 1000 MiB
   */
  public void setBufferSize(long bufferSize) {
    if (bufferSize < BUFFER_SIZE_MIN) {
      throw new IllegalArgumentException("buffer size must >= " + BUFFER_SIZE_MIN
                                         + ", now: " + bufferSize);
    }
    if (bufferSize > BUFFER_SIZE_MAX) {
      throw new IllegalArgumentException("buffer size must <= " + BUFFER_SIZE_MAX
                                         + ", now: " + bufferSize);
    }
    this.bufferSize = bufferSize;
  }

  /**
   * 设置flush阈值，当缓冲区已用百分比超过这个比值，下一次{@link #write(Record)}将触发flush，避免数据量超过缓冲区
   * 大小导致额外内存占用和内存拷贝。举例来说，如果flushThreshold设置为0.85，那么缓冲区用到85%后便会尝试flush。
   *
   * @param flushThreshold
   */
  public void setFlushThreshold(float flushThreshold) {
    if (flushThreshold < FLUSH_THRESHOLD_MIN) {
      throw new IllegalArgumentException("flush threshold must >= " + FLUSH_THRESHOLD_MIN
                                             + ", now" + flushThreshold);
    }
    if (flushThreshold > FLUSH_THRESHOLD_MAX) {
      throw new IllegalArgumentException("flush threshold must <=" + FLUSH_THRESHOLD_MAX
                                             + ", now: " + flushThreshold);
    }
    this.flushThreshold = flushThreshold;
  }

  /**
   * This method is no longer used, and the configured content will not take effect.
   * Please use {@link com.aliyun.odps.tunnel.Configuration#retryPolicy} to set Tunnel Retry Logic configuration.
   * <p>
   * The purpose of this method is for interface compatibility
   */
  @Deprecated
  public void setRetryStrategy(RetryStrategy strategy) {
    // do nothing
  }

  /**
   * 将 record 写入缓冲区，当其大小超过 bufferSize 时，上传缓冲区中的记录。过程中如果发生错误将
   * 进行自动重试，这个过程中 write 调用将一直阻塞，直到所有记录上传成功为止。
   *
   * @param r
   *     {@link Record}对象
   *
   * @throws IOException
   *     Signals that an I/O exception has occurred.
   */
  @Override
  public void write(Record r) throws IOException {
    checkStatusAndError();

    if (bufferedPack.getTotalBytes() > bufferSize * flushThreshold) {
      if (flushService != null) {
        flush(false);
      } else {
        flush();
      }
    }
    long time = System.currentTimeMillis();
    bufferedPack.append(r);
    bufferedPack.addLocalWallTimeMs(System.currentTimeMillis() - time);
  }

  private void checkStatusAndError() throws IOException {
    if (this.isClosed) {
      throw new IOException("Writer is closed.");
    }
    // Proactively check for background errors without waiting
    if (flushFuture != null && flushFuture.isDone()) {
      waitUntilFlushFinish(); // This will get the exception if there is one
    }
  }

  /**
   * 关闭这个 writer，并上传缓存中没有上传过的记录。
   *
   * @throws IOException
   *     Signals that an I/O exception has occurred.
   */
  @Override
  public void close() throws IOException {
    if (isClosed) {
      return;
    }
    sync();
    isClosed = true;
  }

  /**
   * 获得总共写的字节数（记录序列化）
   *
   * @return
   */
  public long getTotalBytes() throws IOException {
    sync();
    return bytesWritten;
  }

  public void flush() throws IOException {
    flushImpl(bufferedPack);
  }

  public synchronized void flush(boolean blocking) throws IOException {
    try {
      // Wait for the previous flush to finish. This provides backpressure.
      waitUntilFlushFinish();

      // Use getSize() to check if there's data, as getTotalBytesWritten() may return 0
      // if data is still in internal buffers (protobuf/deflater) and not yet flushed
      if (bufferedPack.getSize() == 0) {
        return;
      }

      // Swap buffers
      ProtobufRecordPack swap = flushPack;
      flushPack = bufferedPack;
      bufferedPack = swap;

      // Should always be null here, but for safety.
      if (bufferedPack == null) {
        bufferedPack = (ProtobufRecordPack) session.newRecordPack(compressOption);
      }
      bufferedPack.reset(); // Ensure the new buffer is clean

      // Submit the flush task
      flushFuture = flushService.submit(() -> {
        // Run the real flush logic in the background thread
        flushImpl(flushPack);
        return null; // For Callable
      });

      if (blocking) {
        waitUntilFlushFinish();
      }
    } catch (IOException e) {
      throw e; // rethrow directly
    } catch (Exception e) {
      // FIX: Wrap other exceptions in IOException
      throw new IOException("Flush operation failed.", e);
    }
  }

  private void flushImpl(ProtobufRecordPack pack) throws IOException {
    if (isClosed && pack.getTotalBytesWritten() == 0) {
      return;
    }

    long delta = pack.getTotalBytesWritten();
    if (delta > 0L) {
      Long blockId = this.session.getAvailBlockId();
      long version = 0L;
      if (this.versionProvider != null) {
        version = this.versionProvider.generateVersion(blockId);
      }

      try {
        if (this.versionProvider != null) {
          this.session.writeBlock(blockId, pack, this.timeout, version);
        } else {
          this.session.writeBlock(blockId, pack, this.timeout);
        }
      } catch (TunnelException e) {
        throw new IOException("Failed to write block " + blockId, e);
      }
      pack.reset();
      this.bytesWritten += delta;
    }
  }


  public TunnelMetrics getMetrics() {
    return metrics;
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  private void waitUntilFlushFinish() throws IOException {
    if (flushFuture != null) {
      try {
        flushFuture.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Flush wait was interrupted.", e);
      } catch (ExecutionException e) {
        // This is an error from the background thread. Unwrap and throw it.
        throw new IOException("Async flush failed in background.", e.getCause());
      } finally {
        flushFuture = null;
      }
    }
  }

  public void sync() throws IOException {
    if (flushService != null) {
      flush(true); // Call blocking flush
    } else {
      flush(); // Call normal sync flush
    }
  }


  public void setFlushService(ExecutorService flushService) throws IOException {
    this.flushService = flushService;
    if (flushService != null && this.flushPack == null) {
      this.flushPack = (ProtobufRecordPack) session.newRecordPack(compressOption);
    }
  }
}
