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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DiskSpillingInputStreamTest {

  private static final String
    TEST_DATA =
    "Hello, World! This is a test for DiskSpillingInputStream.";
  private InputStream source;

  @BeforeEach
  void setUp() {
    source = new ByteArrayInputStream(TEST_DATA.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * 测试完整读取流，然后重置，并再次完整读取。这是重试的最核心场景。
   */
  @Test
  void testFullReadResetAndFullRead() throws IOException {
    try (DiskSpillingInputStream dsis = DiskSpillingInputStream.create(source)) {
      // 第一次完整读取
      String firstRead = readStreamToString(dsis);
      Assertions.assertEquals(TEST_DATA, firstRead);
      Assertions.assertEquals(-1, dsis.read(), "Stream should be at the end after full read.");

      // 重置流
      dsis.reset();

      // 第二次完整读取
      String secondRead = readStreamToString(dsis);
      Assertions.assertEquals(TEST_DATA, secondRead,
                              "Stream content should be the same after reset.");
    }
  }

  /**
   * 测试流在创建后是否立即支持重置，无需显式调用 mark()。
   */
  @Test
  void testIsResettableFromBirth() throws IOException {
    try (DiskSpillingInputStream dsis = DiskSpillingInputStream.create(source)) {
      // 读取一部分数据
      byte[] buffer = new byte[5];
      dsis.read(buffer);
      Assertions.assertEquals("Hello", new String(buffer));

      // 直接重置，应该回到流的起点
      dsis.reset();

      // 再次完整读取
      String fullRead = readStreamToString(dsis);
      Assertions.assertEquals(TEST_DATA, fullRead,
                              "Should read the full content after resetting from birth.");
    }
  }

  /**
   * 测试经典的 mark/reset 流程：在流的中间标记并重置。
   */
  @Test
  void testPartialReadMarkResetAndReadAgain() throws IOException {
    try (DiskSpillingInputStream dsis = DiskSpillingInputStream.create(source)) {
      // 读取 "Hello, " (7 字节)
      byte[] buffer = new byte[7];
      dsis.read(buffer);

      // 在 "World!" 的位置标记
      dsis.mark(0);

      // 读取 "World!" (6 字节)
      byte[] buffer2 = new byte[6];
      dsis.read(buffer2);
      Assertions.assertEquals("World!", new String(buffer2));

      // 重置到标记点
      dsis.reset();

      // 从标记点开始完整读取
      String remaining = readStreamToString(dsis);
      Assertions.assertEquals("World! This is a test for DiskSpillingInputStream.", remaining);
    }
  }

  /**
   * 测试 close() 方法是否能成功删除底层的临时文件。
   */
  @Test
  void testCloseDeletesTempFile() throws Exception {
    DiskSpillingInputStream dsis = DiskSpillingInputStream.create(source);

    // 使用反射来获取私有字段 tempFilePath 的值，以验证其存在
    Field pathField = DiskSpillingInputStream.class.getDeclaredField("tempFilePath");
    pathField.setAccessible(true);
    Path tempFilePath = (Path) pathField.get(dsis);

    Assertions.assertTrue(Files.exists(tempFilePath), "Temp file should exist before close().");

    // 关闭流
    dsis.close();

    Assertions.assertFalse(Files.exists(tempFilePath),
                           "Temp file should be deleted after close().");
  }

  // 辅助方法，用于将输入流读取为字符串
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

