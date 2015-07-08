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

package com.aliyun.odps.volume;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * DataInputStream with seek and getPos.
 */
public abstract class FSDataInputStream extends DataInputStream {

  public FSDataInputStream(InputStream in)
      throws IOException {
    super(in);
  }

  /**
   * Seek to the given offset.
   *
   * @param desired
   *     offset to seek to
   */
  public abstract void seek(long desired) throws IOException;

  /**
   * Get the current position in the input stream.
   *
   * @return current position in the input stream
   */
  public abstract long getPos() throws IOException;

  /**
   * Read bytes from the given position in the stream to the given buffer.
   *
   * @param position
   *     position in the input stream to seek
   * @param buffer
   *     buffer into which data is read
   * @param offset
   *     offset into the buffer in which data is written
   * @param length
   *     maximum number of bytes to read
   * @return total number of bytes read into the buffer, or <code>-1</code>
   * if there is no more data because the end of the stream has been
   * reached
   */
  public abstract int read(long position, byte[] buffer, int offset, int length)
      throws IOException;

  /**
   * Read bytes from the given position in the stream to the given buffer.
   * Continues to read until <code>length</code> bytes have been read.
   *
   * @param position
   *     position in the input stream to seek
   * @param buffer
   *     buffer into which data is read
   * @param offset
   *     offset into the buffer in which data is written
   * @param length
   *     the number of bytes to read
   * @throws EOFException
   *     If the end of stream is reached while reading.
   *     If an exception is thrown an undetermined number
   *     of bytes in the buffer may have been written.
   */
  public abstract void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException;

  /**
   * See {@link #readFully(long, byte[], int, int)}.
   */
  public void readFully(long position, byte[] buffer)
      throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

}


