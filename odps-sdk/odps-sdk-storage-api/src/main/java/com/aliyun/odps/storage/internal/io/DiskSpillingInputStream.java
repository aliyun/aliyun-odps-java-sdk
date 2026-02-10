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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.aliyun.odps.storage.ClientException;

/**
 * 一个将输入流缓存到临时磁盘文件的可重复读流。
 * <p>
 * 它在构造时将一个源输入流的全部内容写入一个临时文件。
 * 然后，所有读取操作都从这个临时文件进行。
 * 最重要的是，当这个流被 {@link #close()} 时，它会自动删除底层的临时文件。
 * <p>
 * 这个实现兼容 Java 8。
 */
public class DiskSpillingInputStream extends RepeatableFileInputStream {

  /**
   * 定义一个合理的缓冲区大小用于流拷贝。
   * 8KB 是一个非常常见且通常性能良好的值。
   */
  private static final int BUFFER_SIZE = 8192;

  private final Path tempFilePath;

  private DiskSpillingInputStream(Path tempFilePath) throws IOException {
    super(tempFilePath.toFile());
    this.tempFilePath = tempFilePath;
  }

  /**
   * 工厂方法，用于创建一个 DiskSpillingInputStream。
   *
   * @param source 原始的、不可重置的输入流。
   * @return 一个新的、基于磁盘缓存的可重置流。
   * @throws IOException 如果创建临时文件或写入时发生错误。
   */
  public static DiskSpillingInputStream create(InputStream source) throws IOException {
    Path tempFile = Files.createTempFile("sdk-spill-", ".tmp");
    try {
      // 将源流的全部内容写入临时文件
      try (OutputStream os = Files.newOutputStream(tempFile)) {
        // ========= Java 8 兼容的流拷贝逻辑 =========
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;
        while ((bytesRead = source.read(buffer)) != -1) {
          os.write(buffer, 0, bytesRead);
        }
        // ===========================================
      }
      return new DiskSpillingInputStream(tempFile);
    } catch (IOException e) {
      // 如果创建过程中失败，确保删除不完整的临时文件
      Files.deleteIfExists(tempFile);
      throw new ClientException(e);
    } finally {
      // 确保源输入流被关闭
      if (source != null) {
        try {
          source.close();
        } catch (IOException e) {
          // Log or ignore the exception on closing the source stream
        }
      }
    }
  }

  /**
   * 关闭流并删除底层的临时文件。
   */
  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      // 确保临时文件总是被尝试删除
      Files.deleteIfExists(this.tempFilePath);
    }
  }
}

