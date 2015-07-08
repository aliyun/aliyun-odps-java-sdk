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

/**
 *
 */
package com.aliyun.odps.tunnel.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author chao.liu
 * @author shenggong.wang@alibaba-inc.com
 */
class ProtobufOutputStream extends FilterOutputStream {

  private byte[] buf;
  private int count;
  private long totalBytes = 0;

  /**
   * Creates a new protobuf output stream to write coded data to the specified
   * underlying output stream.
   *
   * @param out
   *     the underlying output stream, to be saved for later use.
   */
  public ProtobufOutputStream(OutputStream out) {
    super(out);
    buf = new byte[8192];
  }

  private void flushBuffer() throws IOException {
    if (count > 0) {
      out.write(buf, 0, count);
      totalBytes += count;
      count = 0;
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (count >= buf.length) {
      flushBuffer();
    }
    buf[count++] = (byte) b;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len >= buf.length) {
      flushBuffer();
      out.write(b, off, len);
      totalBytes += len;
      return;
    }
    if (len > buf.length - count) {
      flushBuffer();
    }
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  @Override
  public void flush() throws IOException {
    flushBuffer();
    out.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  /**
   * Encode and write a tag.
   */
  public void writeTag(final int fieldNumber, final int wireType)
      throws IOException {
    writeRawVarint32(WireFormat.makeTag(fieldNumber, wireType));
  }

  /**
   * Writes a boolean to the underlying output stream as a protobuf varint.
   *
   * @param v
   *     a boolean value to be written.
   * @throws IOException
   *     if an I/O error occurs.
   */
  public void writeBoolean(int id, boolean v) throws IOException {
    writeTag(id, WireFormat.WIRETYPE_VARINT);
    writeBoolNoTag(v);
  }

  /**
   * Write an {@code int32} field, including tag, to the stream.
   */
  public void writeInt32(final int fieldNumber, final int value)
      throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
    writeInt32NoTag(value);
  }

  /**
   * Write an {@code sint32} field, including tag, to the stream.
   */
  public void writeSInt32(final int fieldNumber, final int value)
      throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
    writeSInt32NoTag(value);
  }

  /**
   * Write a {@code uint32} field, including tag, to the stream.
   */
  public void writeUInt32(final int fieldNumber, final int value)
      throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
    writeUInt32NoTag(value);
  }

  /**
   * Writes a int to the underlying output stream as a protobuf varint.
   *
   * @param v
   *     a long value to be written.
   * @throws IOException
   *     if an I/O error occurs.
   */
  public void writeLong(int id, long v) throws IOException {
    writeTag(id, WireFormat.WIRETYPE_VARINT);
    writeSInt64NoTag(v);
  }

  /**
   * Writes a double to the underlying output stream as a protobuf double.
   *
   * @param v
   *     a double value to be written.
   * @throws IOException
   *     if an I/O error occurs.
   */
  public void writeDouble(int id, double v) throws IOException {
    writeTag(id, WireFormat.WIRETYPE_FIXED64);
    writeDoubleNoTag(v);
  }

  /**
   * Write a {@code string} field, including tag, to the stream.
   */
  public void writeRawBytes(final int fieldNumber, final byte[] value)
      throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    writeRawBytesNoTag(value);
  }

  /**
   * Write a {@code string} field, including tag, to the stream.
   */
  public void writeString(final int fieldNumber, final String value)
      throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    writeStringNoTag(value);
  }

  /**
   * Encode and write a varint. {@code value} is treated as unsigned, so it
   * won't be sign-extended if negative.
   */
  private void writeRawVarint32(int value) throws IOException {
    while (true) {
      if ((value & ~0x7F) == 0) {
        write(value);
        return;
      } else {
        write((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  }

  /**
   * Encode and write a varint.
   */
  private void writeRawVarint64(long value) throws IOException {
    while (true) {
      if ((value & ~0x7FL) == 0) {
        write((int) value);
        return;
      } else {
        write(((int) value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  }

  /**
   * Write a {@code bool} field to the stream.
   */
  private void writeBoolNoTag(final boolean value) throws IOException {
    write(value ? 1 : 0);
  }

  /**
   * Write an {@code sint64} field to the stream.
   */
  private void writeSInt64NoTag(final long value) throws IOException {
    writeRawVarint64(encodeZigZag64(value));
  }

  /**
   * Write an {@code int32} field to the stream.
   */
  private void writeInt32NoTag(final int value) throws IOException {
    if (value >= 0) {
      writeRawVarint32(value);
    } else {
      // Must sign-extend.
      writeRawVarint64(value);
    }
  }

  /**
   * Write a {@code uint32} field to the stream.
   */
  private void writeUInt32NoTag(final int value) throws IOException {
    writeRawVarint32(value);
  }

  /**
   * Write an {@code sint32} field to the stream.
   */
  private void writeSInt32NoTag(final int value) throws IOException {
    writeRawVarint32(encodeZigZag32(value));
  }

  /**
   * Write a little-endian 64-bit integer.
   */
  private void writeRawLittleEndian64(final long value) throws IOException {
    write((int) (value) & 0xFF);
    write((int) (value >> 8) & 0xFF);
    write((int) (value >> 16) & 0xFF);
    write((int) (value >> 24) & 0xFF);
    write((int) (value >> 32) & 0xFF);
    write((int) (value >> 40) & 0xFF);
    write((int) (value >> 48) & 0xFF);
    write((int) (value >> 56) & 0xFF);
  }

  /**
   * Write a {@code double} field to the stream.
   */
  private void writeDoubleNoTag(final double value) throws IOException {
    writeRawLittleEndian64(Double.doubleToRawLongBits(value));
  }

  /**
   * Write a {@code string} field to the stream.
   */
  private void writeRawBytesNoTag(final byte[] value) throws IOException {
    writeRawVarint32(value.length);
    write(value);
  }

  /**
   * Write a {@code string} field to the stream.
   */
  private void writeStringNoTag(final String value) throws IOException {
    // Unfortunately there does not appear to be any way to tell Java to
    // encode
    // UTF-8 directly into our buffer, so we have to let it create its own
    // byte
    // array and then copy.
    final byte[] bytes = value.getBytes("UTF-8");
    writeRawVarint32(bytes.length);
    write(bytes);
  }

  /**
   * Encode a ZigZag-encoded 64-bit value. ZigZag encodes signed integers into
   * values that can be efficiently encoded with varint. (Otherwise, negative
   * values must be sign-extended to 64 bits to be varint encoded, thus always
   * taking 10 bytes on the wire.)
   *
   * @param n
   *     A signed 64-bit integer.
   * @return An unsigned 64-bit integer, stored in a signed int because Java has
   * no explicit unsigned support.
   */
  private static long encodeZigZag64(final long n) {
    // Note: the right-shift must be arithmetic
    return (n << 1) ^ (n >> 63);
  }

  private static int encodeZigZag32(final int n) {
    // Note: the right-shift must be arithmetic
    return (n << 1) ^ (n >> 31);
  }

  /**
   * 返回已经写出的 protobuf 序列化后的字节数。
   *
   * 这个数字不包含已经存在于 buffer 中，但是尚未 flush 的内容。
   * 如果需要全部序列化过的字节数，需要在调用本方法前先调用 flush()
   *
   * @return 字节数
   */
  public long getTotalBytes() {
    return totalBytes;
  }
}
