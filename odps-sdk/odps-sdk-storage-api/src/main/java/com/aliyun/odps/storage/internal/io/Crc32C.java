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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.zip.Checksum;

import org.apache.commons.codec.digest.PureJavaCrc32C;

/**
 * A class that can be used to compute the CRC32C (Castagnoli) of a ByteBuffer or array of bytes.
 * <p>
 * We use java.util.zip.CRC32C in Java 9 and higher and use org.apache.commons.codec.digest.PureJavaCrc32C in Java 8
 * java.util.zip.CRC32C is significantly faster on reasonably modern CPUs as it uses the CRC32 instruction introduced
 * in SSE4.2.
 * <p>
 * NOTE: This class is intended for INTERNAL usage only within MaxCompute Java SDK.
 */
final class Crc32C {

  private static MethodHandle CRC32C_CONSTRUCTOR;

  static {
    try {
      Class<?> cls = Class.forName("java.util.zip.CRC32C");
      CRC32C_CONSTRUCTOR =
        MethodHandles.publicLookup().findConstructor(cls, MethodType.methodType(void.class));
    } catch (ReflectiveOperationException e) {
      CRC32C_CONSTRUCTOR = null;
    }
  }

  private Crc32C() {
  }

  /**
   * Compute the CRC32C (Castagnoli) of the segment of the byte array given by the specified size and offset
   *
   * @param bytes  The bytes to checksum
   * @param offset the offset at which to begin the checksum computation
   * @param size   the number of bytes to checksum
   * @return The CRC32C
   */
  public static long compute(byte[] bytes, int offset, int size) {
    Checksum crc = create();
    crc.update(bytes, offset, size);
    return crc.getValue();
  }


  public static Checksum create() {
    if (CRC32C_CONSTRUCTOR == null) {
      return new PureJavaCrc32C();
    }
    try {
      return (Checksum) CRC32C_CONSTRUCTOR.invoke();
    } catch (Throwable throwable) {
      return new PureJavaCrc32C();
    }
  }
}