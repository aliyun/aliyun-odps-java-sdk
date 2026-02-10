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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RepeatableFileInputStreamTest {

  private static final String
    TEST_DATA =
    "Hello, World! This is a test for RepeatableFileInputStream.";

  @TempDir
  Path tempDir; // JUnit 5 会自动创建和清理这个临时目录

  private Path testFile;

  @BeforeEach
  void setUp() throws IOException {
    testFile = tempDir.resolve("test.txt");
    Files.write(testFile, TEST_DATA.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * 测试完整读取流，然后重置，并再次完整读取。
   */
  @Test
  void testFullReadResetAndFullRead() throws IOException {
    try (FileInputStream fis = new FileInputStream(testFile.toFile());
         RepeatableFileInputStream rfis = new RepeatableFileInputStream(fis)) {

      String firstRead = readStreamToString(rfis);
      Assertions.assertEquals(TEST_DATA, firstRead);

      rfis.reset();

      String secondRead = readStreamToString(rfis);
      Assertions.assertEquals(TEST_DATA, secondRead);
    }
  }

  /**
   * 测试流在创建后是否立即支持重置。
   */
  @Test
  void testIsResettableFromBirth() throws IOException {
    try (FileInputStream fis = new FileInputStream(testFile.toFile());
         RepeatableFileInputStream rfis = new RepeatableFileInputStream(fis)) {

      byte[] buffer = new byte[5];
      rfis.read(buffer);
      Assertions.assertEquals("Hello", new String(buffer));

      rfis.reset();

      String fullRead = readStreamToString(rfis);
      Assertions.assertEquals(TEST_DATA, fullRead);
    }
  }

  /**
   * 测试在流中间标记并重置。
   */
  @Test
  void testPartialReadMarkResetAndReadAgain() throws IOException {
    try (FileInputStream fis = new FileInputStream(testFile.toFile());
         RepeatableFileInputStream rfis = new RepeatableFileInputStream(fis)) {

      byte[] buffer = new byte[7];
      rfis.read(buffer);

      rfis.mark(0);

      byte[] buffer2 = new byte[6];
      rfis.read(buffer2);
      Assertions.assertEquals("World!", new String(buffer2));

      rfis.reset();

      String remaining = readStreamToString(rfis);
      Assertions.assertEquals("World! This is a test for RepeatableFileInputStream.", remaining);
    }
  }

  /**
   * 测试关闭 RepeatableFileInputStream 是否会关闭底层的 FileInputStream。
   */
  @Test
  void testCloseAlsoClosesUnderlyingStream() throws IOException {
    FileInputStream fis = new FileInputStream(testFile.toFile());
    RepeatableFileInputStream rfis = new RepeatableFileInputStream(fis);

    // 关闭包装流
    rfis.close();

    // 尝试从原始流中读取，应该会抛出异常
    IOException thrown = Assertions.assertThrows(
      IOException.class,
      fis::read,
      "Reading from the original stream after the wrapper is closed should throw IOException."
    );
    Assertions.assertTrue(thrown.getMessage().toLowerCase().contains("stream closed"));
  }

  // 辅助方法，与上一个测试类相同
  private String readStreamToString(InputStream in) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;
    while ((length = in.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toString(StandardCharsets.UTF_8.name());
  }
}

