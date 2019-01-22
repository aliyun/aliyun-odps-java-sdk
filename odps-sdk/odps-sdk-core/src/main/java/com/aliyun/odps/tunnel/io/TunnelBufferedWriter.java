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

import com.aliyun.odps.commons.util.RetryExceedLimitException;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.commons.util.backoff.BackOffStrategy;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

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

  static class TunnelRetryStrategy extends RetryStrategy {

    private final static int limit = 6;
    private final static int interval = 4;

    TunnelRetryStrategy() {
      super(limit, interval, BackoffStrategy.EXPONENTIAL_BACKOFF);
    }

    TunnelRetryStrategy(int limit, BackOffStrategy strategy) {
      super(limit, strategy);
    }

    @Override
    protected boolean needRetry(Exception e) {
      TunnelException err = null;
      if (e.getCause() instanceof TunnelException) {
        err = (TunnelException) e.getCause();
      }

      if (e instanceof TunnelException) {
        err = (TunnelException) e;
      }

      if (err != null && err.getStatus() != null && err.getStatus() / 100 == 4) {
        return false;
      }

      return true;
    }
  }


  private ProtobufRecordPack bufferedPack;
  private TableTunnel.UploadSession session;
  private RetryStrategy retry;
  private long bufferSize;
  private long bytesWritten;

  private static final long BUFFER_SIZE_DEFAULT = 64 * 1024 * 1024;
  private static final long BUFFER_SIZE_MIN = 1024 * 1024;
  private static final long BUFFER_SIZE_MAX = 1000 * 1024 * 1024;

  /**
   * 构造此类对象，使用默认缓冲区大小为 10 MiB，和默认的回退策略：4s、8s、16s、32s、64s、128s
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
    this.bufferSize = BUFFER_SIZE_DEFAULT;
    this.retry = new TunnelRetryStrategy();
    this.bytesWritten = 0;
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
   * 设置重试策略
   *
   * @param strategy
   *     {@link RetryStrategy}
   */
  public void setRetryStrategy(RetryStrategy strategy) {
    this.retry = strategy;
  }

  /**
   * 将 record 写入缓冲区，当其大小超过 bufferSize 时，上传缓冲区中的记录过程中如果发生错误将
   * 进行自动重试，这个过程中 write 调用将一直阻塞，直到所有记录上传成功为止。
   *
   * @param r
   *     {@link Record}对象
   *
   * @throws IOException
   *     Signals that an I/O exception has occurred.
   */
  public void write(Record r) throws IOException {
    if (bufferedPack.getTotalBytes() > bufferSize) {
      flush();
    }
    bufferedPack.append(r);
  }

  /**
   * 关闭这个 writer，并上传缓存中没有上传过的记录。
   *
   * @throws IOException
   *     Signals that an I/O exception has occurred.
   */
  public void close() throws IOException {
    flush();
  }

  /**
   * 获得总共写的字节数（记录序列化）
   *
   * @return
   */
  public long getTotalBytes() throws IOException {
    flush();
    return bytesWritten;
  }

  public void flush() throws IOException {
    // 每一个block的上传单独计算重试次数
    retry.reset();
    // 得到实际序列化的的字节数，如果等于 0，说明没有写，跳过即可
    long delta = bufferedPack.getTotalBytesWritten();
    if (delta > 0) {
      Long blockId = session.getAvailBlockId();
      while (true) {
        try {
          session.writeBlock(blockId, bufferedPack);
          bufferedPack.reset();
          bytesWritten += delta;
          return;
        } catch (IOException e) {
          try {
            retry.onFailure(e);
          } catch (RetryExceedLimitException ignore) {
            throw e;
          }
        }
      }
    }
  }
}
