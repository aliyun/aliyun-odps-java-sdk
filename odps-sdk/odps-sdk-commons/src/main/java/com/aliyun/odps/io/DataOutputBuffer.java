/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * DataOutputBuffer 提供一个可复用的 {@link DataOutput} 实现.
 *
 * <p>
 * DataOutputBuffer 继承自 {@link DataOutputStream}，提供了 {@link DataOutput}
 * 的实现，DataOutputBuffer 使用内存作为数据输出的缓冲区，并提供 {@link #reset()} 方法支持对象复用。
 *
 * <p>
 * 代码示例：
 *
 * <pre>
 * DataOutputBuffer buffer = new DataOutputBuffer();
 * while (... loop condition ...) {
 *   buffer.reset();
 *   ... write buffer using DataOutput methods ...
 *   byte[] data = buffer.getData();
 *   int dataLength = buffer.getLength();
 *   ... write data to its ultimate destination ...
 * }
 * </pre>
 *
 * @see Writable#write(DataOutput)
 * @see DataInputBuffer
 */
public class DataOutputBuffer extends DataOutputStream {

  private static class Buffer extends ByteArrayOutputStream {

    public byte[] getData() {
      return buf;
    }

    public int getLength() {
      return count;
    }

    public Buffer() {
      super();
    }

    public Buffer(int size) {
      super(size);
    }

    public void write(DataInput in, int len) throws IOException {
      int newcount = count + len;
      if (newcount > buf.length) {
        byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
      }
      in.readFully(buf, count, len);
      count = newcount;
    }
  }

  private Buffer buffer;

  /**
   * 构造一个空数据输出缓冲区
   */
  public DataOutputBuffer() {
    this(new Buffer());
  }

  /**
   * 构造一个给定大小的数据输出缓冲区
   *
   * @param size
   */
  public DataOutputBuffer(int size) {
    this(new Buffer(size));
  }

  private DataOutputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /**
   * 返回输出缓冲区中的数据.
   *
   * <p>
   * 注意：返回字节数组中[0, {@link #getLength()}-1] 中的数据才是有效的。
   *
   * @return 输出缓冲区中的数据
   */
  public byte[] getData() {
    return buffer.getData();
  }

  /**
   * 返回数据缓冲区中的有效数据长度
   *
   * @return 数据缓冲区中的有效数据长度，单位：字节
   */
  public int getLength() {
    return buffer.getLength();
  }

  /**
   * 重置缓冲区内容为空并返回
   *
   * @return
   */
  public DataOutputBuffer reset() {
    this.written = 0;
    buffer.reset();
    return this;
  }

  /**
   * 从给定 {@link DataInput} 中读出数据然后输出到缓冲区.
   *
   * @param in
   *     数据输入 {@link DataInput}
   * @param length
   *     读取长度
   * @throws IOException
   */
  public void write(DataInput in, int length) throws IOException {
    buffer.write(in, length);
  }

  /**
   * 将输出缓冲区中的内容输出到给定的 {@link OutputStream}.
   *
   * @param out
   * @throws IOException
   */
  public void writeTo(OutputStream out) throws IOException {
    buffer.writeTo(out);
  }
}
