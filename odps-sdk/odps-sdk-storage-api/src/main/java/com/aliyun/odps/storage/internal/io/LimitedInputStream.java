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

package com.aliyun.odps.storage.internal.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * 一个 InputStream 包装器，它只允许从底层流中读取固定数量的字节。
 * 它的 close() 方法不会关闭底层流。
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public final class LimitedInputStream extends InputStream {

  private final InputStream underlyingStream;
  private long remainingBytes;
  private boolean isClosed = false;

  public LimitedInputStream(InputStream underlyingStream, long limit) {
    this.underlyingStream = Objects.requireNonNull(underlyingStream);
    this.remainingBytes = limit;
  }

  @Override
  public int read() throws IOException {
    if (remainingBytes <= 0) {
      return -1; // 到达限制，表现为流结束
    }
    int byteRead = underlyingStream.read();
    if (byteRead != -1) {
      remainingBytes--;
    } else {
      // 底层流意外结束
      remainingBytes = 0;
    }
    return byteRead;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (remainingBytes <= 0) {
      return -1;
    }
    int bytesToRead = (int) Math.min(len, remainingBytes);
    int bytesRead = underlyingStream.read(b, off, bytesToRead);
    if (bytesRead != -1) {
      remainingBytes -= bytesRead;
    } else {
      remainingBytes = 0;
    }
    return bytesRead;
  }


  /**
   * 返回可从此输入流读取（或跳过）而不会阻塞的字节数的估计值。
   * 该值是底层流的可用字节数和此流剩余限制之间的较小者。
   *
   * @return 可以在不阻塞的情况下从此输入流读取的字节数的估计值，作为 int 返回。
   * @throws IOException 如果发生 I/O 错误。
   */
  @Override
  public int available() throws IOException {
    long available = Math.min(underlyingStream.available(), remainingBytes);
    return (int) available;
  }

  @Override
  public void close() throws IOException {
    if (isClosed) {
      return;
    }
    isClosed = true;
    // 跳过所有剩余字节
    while (remainingBytes > 0) {
      long skipped = underlyingStream.skip(remainingBytes);
      if (skipped <= 0) {
        // 回退到 read()
        if (read() == -1) {
          break;
        }
      } else {
        remainingBytes -= skipped;
      }
    }
  }

  public boolean isClosed() {
    return isClosed;
  }
}
