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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;
import java.util.zip.InflaterInputStream;

import com.aliyun.odps.commons.transport.Connection;

public class VolumeInputStream extends InputStream {

  private Connection conn = null;
  private long totalBytes;
  private InputStream in;
  static final int CHECKSUM_SIZE = 4;
  private byte[] buffer;
  private int bufferSize;
  private int dataSize;
  private int pos;
  private boolean eof;
  private boolean init;
  private CRC32 crc = new CRC32();
  private int chunkSize;
  private final int MAX_CHUNKSIZE = 256 * 1024 * 1024;
  private final int MIN_CHUNKSIZE = 1;

  public VolumeInputStream(Connection conn, CompressOption option) throws IOException {

    if (option != null) {
      if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
        this.in = new InflaterInputStream(conn.getInputStream());
      } else {
        throw new IOException("invalid compression option.");
      }
    } else {
      this.in = conn.getInputStream();
    }

    this.conn = conn;
    pos = 0;
    eof = false;
    init = false;
  }

  @Override
  public void close() throws IOException {
    in.close();
    conn.disconnect();
  }

  int getInt(byte[] buffer, int offset) throws IOException {
    try {
      // read 4 bytes start with offset to format a int number
      ByteBuffer bb = ByteBuffer.wrap(buffer, offset, 4);
      bb.order(ByteOrder.BIG_ENDIAN);
      return bb.getInt();
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public int read() throws IOException {
    byte[] buf = new byte[1];
    int len = read(buf, 0, 1);
    if (len == 1) {
      return (int) buf[0] & 0xFF;
    }

    return -1;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    if (!init) {
      init = true;
      byte[] intBuffer = new byte[4];
      int count = readInternal(intBuffer);
      if (count == 4) {
        crc.update(intBuffer, 0, 4);
        chunkSize = getInt(intBuffer, 0);
        if (chunkSize > MAX_CHUNKSIZE || chunkSize < MIN_CHUNKSIZE) {
          String errMsg = "ChunkSize should be in [" + MIN_CHUNKSIZE + " ," + MAX_CHUNKSIZE
                          + " ], now is : " + chunkSize;
          throw new IOException(errMsg);
        }
        bufferSize = chunkSize + CHECKSUM_SIZE;
        buffer = new byte[bufferSize];
      } else if (count > 0) {
        throw new IOException("Invalid VolumeInputStream.");
      }
    }

    if (buf == null || buf.length == 0) {
      throw new IOException("Invalid Protobuf Data Buffer!");
    }
    int length = readBuffer(buf, off, len);
    if (length > 0) {
      totalBytes += length;
      return length;
    } else {
      return -1;
    }
  }

  public long getTotalBytes() {
    return this.totalBytes;
  }

  public int fillBuffer() throws IOException {
    if (dataSize > pos) {
      return 0;
    }
    // fill data
    int length = readInternal(buffer);

    if (length >= CHECKSUM_SIZE) {
      dataSize = length - CHECKSUM_SIZE;
      pos = 0;

      crc.update(buffer, 0, dataSize);

      int checksum = getInt(buffer, dataSize);
      if (checksum != (int) crc.getValue()) {
        throw new IOException("crc check error in VolumeInputStream. ");
      }
    } else if (length > 0) {
      throw new IOException("Invalid VolumeInputStream.");
    }
    return length;
  }

  public int readBuffer(byte[] buf, int off, int size) throws IOException {
    if (buf == null) {
      throw new NullPointerException();
    } else if (off < 0 || size < 0 || size > buf.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (size == 0) {
      return 0;
    }

    int length = size;
    while (length > 0) {
      if (dataSize <= pos) {
        if (eof || fillBuffer() == 0) {
          break;
        }
      }
      int availData = dataSize - pos;
      int readSize = availData < length ? availData : length;
      System.arraycopy(buffer, pos, buf, size - length + off, readSize);
      pos += readSize;
      length -= readSize;
    }

    return size - length;
  }

  private int readInternal(byte[] buf) throws IOException {
    int length = 0;
    int size = buf.length;
    while (length < size) {
      int len = in.read(buf, length, size - length);
      if (len <= 0) {
        break;
      }
      length += len;
    }
    if (length == 0) {
      eof = true;
    }
    return length;
  }
}
