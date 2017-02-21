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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.xerial.snappy.PureJavaCrc32C;

/**
 * CRC32 checksum util
 */
public class Checksum {

  private static byte[] TRUE = {1};
  private static byte[] FALSE = {0};

  private PureJavaCrc32C crc = new PureJavaCrc32C();
  private ByteBuffer buf;

  public Checksum() {
    buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
  }

  public void update(int v) {
    buf.clear();
    buf.putInt(v);
    crc.update(buf.array(), 0, 4);
  }

  public void update(long v) {
    buf.clear();
    buf.putLong(v);
    crc.update(buf.array(), 0, 8);
  }

  public void update(double v) {
    buf.clear();
    buf.putDouble(v);
    crc.update(buf.array(), 0, 8);
  }

  public void update(float v) {
    buf.clear();
    buf.putFloat(v);
    crc.update(buf.array(), 0, 4);
  }

  public void update(boolean v) {
    crc.update(v ? TRUE : FALSE, 0, 1);
  }

  public void update(byte[] b, int off, int len) {
    crc.update(b, off, len);
  }

  public long getValue() {
    return crc.getValue();
  }

  public void reset() {
    crc.reset();
  }

}
