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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

/**
 * DataInputBuffer 提供一个可复用的 {@link DataInput} 实现.
 *
 * <p>
 * DataInputBuffer 继承自 {@link DataInputStream} ，提供了 {@link DataInput}
 * 的实现，可以方便的将内存数据作为输入，并提供 reset 系列方法支持对象复用。
 *
 * <p>
 * 代码示例：
 *
 * <pre>
 * DataInputBuffer buffer = new DataInputBuffer();
 * while (... loop condition ...) {
 *   byte[] data = ... get data ...;
 *   int dataLength = ... get data length ...;
 *   buffer.reset(data, dataLength); //重用 buffer，无需再new 对象
 *   ... read buffer using DataInput methods ...
 * }
 * </pre>
 *
 * @see Writable#readFields(DataInput)
 * @see DataOutputBuffer
 */
public class DataInputBuffer extends DataInputStream {

  private static class Buffer extends ByteArrayInputStream {

    public Buffer() {
      super(new byte[]{});
    }

    public void reset(byte[] input, int start, int length) {
      this.buf = input;
      this.count = start + length;
      this.mark = start;
      this.pos = start;
    }

    public byte[] getData() {
      return buf;
    }

    public int getPosition() {
      return pos;
    }

    public int getLength() {
      return count;
    }
  }

  private Buffer buffer;

  /**
   * 构建一个空 DataInputBuffer
   */
  public DataInputBuffer() {
    this(new Buffer());
  }

  private DataInputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /**
   * 重置内存数据输入.
   *
   * <p>
   * 将 input[0,...,length-1] 作为数据输入
   *
   * @param input
   *     字节数组
   * @param length
   *     数据内容长度
   */
  public void reset(byte[] input, int length) {
    buffer.reset(input, 0, length);
  }

  /**
   * 重置内存数据输入.
   *
   * <p>
   * 将 input[start,...,start+length-1] 作为数据输入
   *
   * @param input
   *     字节数组
   * @param start
   *     数据起始位置
   * @param length
   *     数据内容长度
   */
  public void reset(byte[] input, int start, int length) {
    buffer.reset(input, start, length);
  }

  /**
   * 获取当前数据内容的字节数组
   *
   * @return 当前数据内容的字节数组
   */
  public byte[] getData() {
    return buffer.getData();
  }

  /**
   * 获取数据输入流的当前位置
   *
   * @return 数据输入流的当前位置
   */
  public int getPosition() {
    return buffer.getPosition();
  }

  /**
   * 获取数据输入的内容长度
   *
   * @return 数据输入的内容长度
   */
  public int getLength() {
    return buffer.getLength();
  }

}
