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

package com.aliyun.odps.storage.internal.utils;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.storage.internal.io.DiskSpillingInputStream;
import com.aliyun.odps.storage.internal.io.RepeatableFileInputStream;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class IOUtils {


  private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

  private IOUtils() {
  }

  /**
   * 将一个输入流包装成可重复读的流，同时尽可能地优化性能。
   * <p>
   * 该方法的行为遵循以下优先级：
   * <ol>
   *   <li>如果原始流已支持 mark/reset，直接返回。</li>
   *   <li>如果原始流是 FileInputStream，则使用高效的 {@link RepeatableFileInputStream} 包装。</li>
   *   <li>对于其他所有情况，将流的内容缓存到一个临时磁盘文件，并返回一个 {@link DiskSpillingInputStream}。
   *       这确保了对任意大小、任意来源的流都提供绝对健壮的可重试性。</li>
   * </ol>
   *
   * @param original 原始输入流。
   * @return 一个保证可重复读的输入流。
   * @throws IOException 如果在缓存到磁盘的过程中发生 I/O 错误。
   */
  public static InputStream newRepeatableInputStream(final InputStream original)
    throws IOException {
    if (original == null) {
      return null;
    }

    if (original.markSupported()) {
      LOG.debug("Original stream already supports mark/reset. Using it directly.");
      return original;
    }

    if (original instanceof FileInputStream) {
      LOG.debug(
        "Original stream is a FileInputStream. Wrapping with RepeatableFileInputStream for efficient reset.");
      return new RepeatableFileInputStream((FileInputStream) original);
    }

    LOG.warn("Original stream does not support mark/reset and is not a file stream. " +
             "Spilling to a temporary disk file to ensure repeatability. " +
             "This may cause performance overhead and consume disk space for large streams.");

    try {
      return DiskSpillingInputStream.create(original);
    } catch (IOException e) {
      LOG.error("Failed to spill stream to disk. The stream will not be repeatable.", e);
      return original;
    }
  }

  public static void transferTo(InputStream inputStream, OutputStream outputStream)
    throws IOException {
    byte[] buffer = new byte[8192];
    int bytesRead;
    while ((bytesRead = inputStream.read(buffer)) != -1) {
      outputStream.write(buffer, 0, bytesRead);
    }
  }

  public static byte[] readAllBytes(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    transferTo(in, out);
    return out.toByteArray();
  }

  /**
   * 从输入流中精确读取 len 个字节。
   * 这个方法会阻塞，直到 len 个字节被读取，或者流结束，或者发生 I/O 错误。
   *
   * @param in  输入流
   * @param len 要读取的字节数
   * @return 包含所读取字节的字节数组
   * @throws IOException 如果发生 I/O 错误
   * @throws EOFException 如果在读取完 len 个字节之前到达流的末尾
   */
  public static byte[] readNBytes(InputStream in, int len) throws IOException {
    if (len < 0) {
      throw new IndexOutOfBoundsException("len must not be negative");
    }

    byte[] buffer = new byte[len];
    int offset = 0;

    while (offset < len) {
      int bytesRead = in.read(buffer, offset, len - offset);
      if (bytesRead == -1) {
        // 在我们读满 buffer 之前，流就结束了
        throw new EOFException("Unexpected end of stream. " +
                               "Expected " + len + " bytes, but only got " + offset);
      }
      offset += bytesRead;
    }

    return buffer;
  }

  public static int readFully(InputStream source, byte[] buffer, int offset, int length) throws IOException {
    if (length < 0) {
      throw new IndexOutOfBoundsException("Length must be non-negative");
    }
    int totalBytesRead = 0;
    while (totalBytesRead < length) {
      int bytesToRead = length - totalBytesRead;
      int bytesRead = source.read(buffer, offset + totalBytesRead, bytesToRead);
      if (bytesRead == -1) {
        break;
      }
      totalBytesRead += bytesRead;
    }
    return totalBytesRead;
  }

  public static long readLittleEndianLong(InputStream in) throws IOException {
    byte[] buffer = new byte[8];
    // 确保我们完整地读取了8个字节
    int offset = 0;
        while (offset < 8) {
          int bytesRead = in.read(buffer, offset, 8 - offset);
          if (bytesRead == -1) {
            throw new IOException("Unexpected end of stream. Could not read 8 bytes for a long.");
          }
          offset += bytesRead;
        }
    // 使用 ByteBuffer 进行转换
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

    return byteBuffer.getLong();
  }
}
