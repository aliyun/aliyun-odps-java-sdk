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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import com.aliyun.odps.storage.internal.utils.IOUtils;

/**
 * Strips CRC32C checksums from a stream formatted as:
 *   [4096 bytes data][4 bytes CRC] ... [N bytes data (1<=N<=4096)][4 bytes CRC]
 *
 * <p>Assumes:
 * <ul>
 *   <li>All full blocks are exactly 4096 bytes of data + 4 bytes CRC (4100 total)</li>
 *   <li>The last block has 1~4096 bytes of data + 4 bytes CRC</li>
 *   <li>No padding is applied to the last block</li>
 * </ul>
 */
public class CrcStrippedInputStream extends InputStream {

  private static final int FULL_BLOCK_TOTAL_SIZE = 4100; // 4096 data + 4 CRC
  private static final int MAX_DATA_SIZE = 4096;
  private static final int CRC_SIZE = 4;

  private final InputStream source;
  private final byte[] dataBuffer = new byte[MAX_DATA_SIZE]; // holds only data (no CRC)
  private int pos = 0;      // current read position in dataBuffer
  private int limit = 0;    // number of valid bytes in dataBuffer
  private boolean eof = false;

  public CrcStrippedInputStream(InputStream source) {
    this.source = Objects.requireNonNull(source, "source");
  }

  @Override
  public int read() throws IOException {
    if (pos >= limit) {
      if (!fillBuffer()) {
        return -1;
      }
    }
    return dataBuffer[pos++] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException("b");
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return 0;
    }

    int totalRead = 0;
    while (totalRead < len) {
      if (pos >= limit) {
        if (!fillBuffer()) {
          break;
        }
      }

      int toCopy = Math.min(limit - pos, len - totalRead);
      System.arraycopy(dataBuffer, pos, b, off + totalRead, toCopy);
      pos += toCopy;
      totalRead += toCopy;
    }

    return totalRead == 0 ? -1 : totalRead;
  }

  /**
   * Fills dataBuffer with the next block of data (without CRC).
   * Returns true if data is available, false if EOF.
   */
  private boolean fillBuffer() throws IOException {
    if (eof) {
      return false;
    }

    // Allocate a temporary buffer to read data+CRC together
    byte[] blockBuffer = new byte[FULL_BLOCK_TOTAL_SIZE];

    // Read as much as possible in one go (typically 4100 for full blocks)
    int totalRead = IOUtils.readFully(source, blockBuffer, 0, FULL_BLOCK_TOTAL_SIZE);

    // Handle EOF at block boundary
    if (totalRead == -1) {
      eof = true;
      return false;
    }

    // Case 1: Full block (4100 bytes)
    if (totalRead == FULL_BLOCK_TOTAL_SIZE) {
      // Verify CRC for full block
      long computedCrc = Crc32C.compute(blockBuffer, 0, MAX_DATA_SIZE);
      long expectedCrc = ByteBuffer.wrap(blockBuffer, MAX_DATA_SIZE, CRC_SIZE)
                           .order(ByteOrder.LITTLE_ENDIAN)
                           .getInt() & 0xFFFFFFFFL;
      if (computedCrc != expectedCrc) {
        throw new IOException(String.format(
          "CRC32C mismatch in full block! Computed: 0x%08x, Expected: 0x%08x",
          computedCrc, expectedCrc));
      }

      // Copy data part to output buffer
      System.arraycopy(blockBuffer, 0, dataBuffer, 0, MAX_DATA_SIZE);
      pos = 0;
      limit = MAX_DATA_SIZE;
      return true;
    }

    // Case 2: Tail block (must be between 5 and 4099 bytes: 1~4095 data + 4 CRC)
    if (totalRead < CRC_SIZE + 1) {
      throw new IOException(
        "Invalid tail block: expected at least 5 bytes (1 data + 4 CRC), got " + totalRead);
    }
    if (totalRead > FULL_BLOCK_TOTAL_SIZE) {
      // Should never happen due to read() contract, but be defensive
      throw new IOException("Read more than " + FULL_BLOCK_TOTAL_SIZE + " bytes");
    }

    int dataLength = totalRead - CRC_SIZE;

    // Verify CRC for tail block
    long computedCrc = Crc32C.compute(blockBuffer, 0, dataLength);
    long expectedCrc = ByteBuffer.wrap(blockBuffer, dataLength, CRC_SIZE)
                         .order(ByteOrder.LITTLE_ENDIAN)
                         .getInt() & 0xFFFFFFFFL;
    if (computedCrc != expectedCrc) {
      throw new IOException(String.format(
        "CRC32C mismatch in tail block! Computed: 0x%08x, Expected: 0x%08x",
        computedCrc, expectedCrc));
    }

    // Copy data part to output buffer
    System.arraycopy(blockBuffer, 0, dataBuffer, 0, dataLength);
    pos = 0;
    limit = dataLength;
    eof = true; // No more blocks after tail
    return true;
  }

  @Override
  public void close() throws IOException {
    source.close();
  }
}
