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

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.aliyun.odps.VolumeException;
import com.aliyun.odps.tunnel.io.VolumeOutputStream;

/**
 * 
 * VolumeFSOutputStream
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFSOutputStream extends OutputStream {

  private VolumeFSClient volumeClient;

  private String path;

  private OutputStream volumeOutputStream;

  private Progressable progress;

  private ExecutorService executorService = Executors.newFixedThreadPool(1);

  private long accumulationWithBlockSize = 0;

  private long blockSize;

  private boolean overwrite;

  /**
   * @param path
   * @param volumeClient
   * @param permission
   * @param overwrite
   * @param replication
   * @param blockSize
   * @param progress
   * @throws VolumeException
   */
  public VolumeFSOutputStream(String path, VolumeFSClient volumeClient, FsPermission permission,
      boolean overwrite, short replication, long blockSize, Progressable progress)
      throws VolumeException {
    this.path = path;
    this.volumeClient = volumeClient;
    this.volumeOutputStream = volumeClient.openOutputStream(path, overwrite, replication);
    this.blockSize = blockSize;
    this.overwrite = overwrite;
    this.progress = progress;
  }

  @Override
  public void write(int b) throws IOException {
    volumeOutputStream.write(b);
    progress(1);
  }


  @Override
  public void write(byte b[], int off, int len) throws IOException {
    volumeOutputStream.write(b, off, len);
    progress(len);
  }



  @Override
  public void close() throws IOException {
    try {
      volumeClient.commitUploadSession(path, (VolumeOutputStream) volumeOutputStream, overwrite);
    } catch (VolumeException e) {
      throw new IOException("Commit upload session fails:" + e.getMessage(), e);
    }
  }

  private void progress(int len) {
    if (progress != null) {
      accumulationWithBlockSize += len;
      if (accumulationWithBlockSize >= blockSize) {
        executorService.submit(new Runnable() {
          @Override
          public void run() {
            progress.progress();
          }
        });
        accumulationWithBlockSize = 0;
      }
    }
  }
}
