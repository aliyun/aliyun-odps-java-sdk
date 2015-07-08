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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.tunnel.TunnelException;

public class VolumeOutputStream extends OutputStream {

  private Connection conn = null;
  private long totalBytes;
  private OutputStream out;
  private CRC32 crc = new CRC32();
  private final int CHUNK_SIZE = 512 * 1024;
  private int chunkOffset;
  private boolean init;
  private Deflater def;

  public VolumeOutputStream(Connection conn, CompressOption option) throws IOException {

    if (option != null) {
      if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
        def = new Deflater();
        def.setLevel(option.level);
        def.setStrategy(option.strategy);

        this.out = new DeflaterOutputStream(conn.getOutputStream(), def);
      } else {
        throw new IOException("invalid compression option.");
      }
    } else {
      this.out = conn.getOutputStream();
    }

    this.conn = conn;
    init = false;
  }

  @Override
  public void write(int b) throws IOException {
    byte[] bytes = ByteBuffer.allocate(1).array();
    bytes[0] = (byte) (b & 0xFF);
    write(bytes);
  }

  @Override
  public void close() throws IOException {
    if (chunkOffset != 0) {
      int checkSum = (int) crc.getValue();
      writeInt(checkSum);
    }

    out.close();

    // handle response
    try {
      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        TunnelException err = new TunnelException(conn.getInputStream());
        err.setRequestId(resp.getHeader("x-odps-request-id"));

        throw new IOException(err);
      }
    } finally {
      if (def != null) {
        def.end();
      }
      conn.disconnect();
    }
  }

  @Override
  public void write(byte[] buf, int off, int len) throws IOException {
    if (!init) {
      init = true;
      writeInt(CHUNK_SIZE);
      crcInt(CHUNK_SIZE);
      chunkOffset = 0;
    }

    if (buf == null || buf.length == 0) {
      throw new IOException("Invalid Protobuf Data Buffer!");
    }
    int bytes = 0;
    while (bytes < len) {
      if (chunkOffset == CHUNK_SIZE) {
        int checkSum = (int) crc.getValue();
        writeInt(checkSum);
        chunkOffset = 0;
      } else {
        int size = len - bytes > CHUNK_SIZE - chunkOffset ? CHUNK_SIZE - chunkOffset : len - bytes;
        out.write(buf, off + bytes, size);
        crc.update(buf, off + bytes, size);
        bytes += size;
        chunkOffset += size;
      }
    }
    totalBytes += len;
  }

  private void writeInt(int value) throws IOException {
    byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    out.write(bytes);
  }

  private void crcInt(int value) throws IOException {
    byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    crc.update(bytes);
  }

  public long getBytes() {
    return this.totalBytes;
  }
}
