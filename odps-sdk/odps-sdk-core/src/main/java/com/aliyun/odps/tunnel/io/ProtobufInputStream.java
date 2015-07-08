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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author chao.liu
 * @author shenggong.wang@alibaba-inc.com
 */
class ProtobufInputStream extends FilterInputStream {

  private long totalBytes;

  public ProtobufInputStream(InputStream in) {
    super(in);
  }

  public int readFieldNumber() throws IOException {
    return WireFormat.getTagFieldNumber(readTag());
  }

  /**
   * Attempt to read a field tag, returning zero if we have reached EOF.
   * Protocol message parsers use this to read tags, since a protocol message
   * may legally end wherever a tag occurs, and zero is not a valid tag number.
   */
  private int readTag() throws IOException {

    int tag = 0;
    tag = readRawVarint32();
    if (WireFormat.getTagFieldNumber(tag) == 0) {
      // If we actually read zero (or any tag number corresponding to
      // field
      // number zero), that's not a valid tag.
      throw new IOException("Invalid protobuf tag.");
    }
    return tag;
  }

  /**
   * Read a {@code bool} field value from the stream.
   */
  public boolean readBoolean() throws IOException {
    return readRawVarint32() != 0;
  }

  /**
   * Read an {@code sint32} field value from the stream.
   */
  public int readSInt32() throws IOException {
    return decodeZigZag32(readRawVarint32());
  }

  /**
   * Read a {@code uint32} field value from the stream.
   */
  public int readUInt32() throws IOException {
    return readRawVarint32();
  }

  public long readLong() throws IOException {
    return decodeZigZag64(readRawVarint64());
  }

  /**
   * Read a {@code double} field value from the stream.
   */
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readRawLittleEndian64());
  }

  /**
   * Read a {@code byte[]} field value from the stream.
   */
  public byte[] readRawBytes() throws IOException {
    final int size = readRawVarint32();

    byte[] bytes = new byte[size];
    int len = 0;
    while (len < size) {
      len += read(bytes, len, size - len);
    }
    return bytes;
  }

  /**
   * Read a {@code string} field value from the stream.
   */
  public String readString() throws IOException {
    return new String(readRawBytes(), "UTF-8");
  }

  private int readRawVarint32() throws IOException {
    byte tmp = (byte) read();
    if (tmp >= 0) {
      return tmp;
    }
    int result = tmp & 0x7f;
    if ((tmp = (byte) read()) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = (byte) read()) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = (byte) read()) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = (byte) read()) << 28;
          if (tmp < 0) {
            // Discard upper 32 bits.
            /*
             * for (int i = 0; i < 5; i++) { if (read() >= 0) { return result; }
             * } throw InvalidProtocolBufferException.malformedVarint();
             */
            throw new IOException("MalformedVarint");
          }
        }
      }
    }
    return result;
  }

  /**
   * Read a raw Varint from the stream.
   */
  private long readRawVarint64() throws IOException {
    int shift = 0;
    long result = 0;
    while (shift < 64) {
      final byte b = (byte) read();
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new IOException("Protobuf malformed.");
  }

  /**
   * Read a 64-bit little-endian integer from the stream.
   */
  private long readRawLittleEndian64() throws IOException {
    final int b1 = read();
    final int b2 = read();
    final int b3 = read();
    final int b4 = read();
    final int b5 = read();
    final int b6 = read();
    final int b7 = read();
    final int b8 = read();
    return (((long) b1 & 0xff)) | (((long) b2 & 0xff) << 8)
           | (((long) b3 & 0xff) << 16) | (((long) b4 & 0xff) << 24)
           | (((long) b5 & 0xff) << 32) | (((long) b6 & 0xff) << 40)
           | (((long) b7 & 0xff) << 48) | (((long) b8 & 0xff) << 56);
  }

  /**
   * Decode a ZigZag-encoded 64-bit value. ZigZag encodes signed integers into
   * values that can be efficiently encoded with varint. (Otherwise, negative
   * values must be sign-extended to 64 bits to be varint encoded, thus always
   * taking 10 bytes on the wire.)
   *
   * @param n
   *     An unsigned 64-bit integer, stored in a signed int because Java
   *     has no explicit unsigned support.
   * @return A signed 64-bit integer.
   */
  private static long decodeZigZag64(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }

  private static int decodeZigZag32(final int n) {
    return (n >>> 1) ^ -(n & 1);
  }

  public int read() throws IOException {
    int b = super.read();
    if (-1 == b) {
      return b;
    }

    totalBytes++;
    return b;
  }

  public int read(byte[] buf, int off, int len) throws IOException {
    len = super.read(buf, off, len);
    if (-1 == len) {
      return len;
    }

    totalBytes += len;
    return len;
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  public long getTotalBytes() {
    return totalBytes;
  }
}
