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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * BytesWritable 提供了 byte[] 的 {@link Writable} 和 {@link WritableComparable} 的实现.
 */
public class BytesWritable extends BinaryComparable implements
                                                    WritableComparable<BinaryComparable> {

  private static final int LENGTH_BYTES = 4;
  private static final byte[] EMPTY_BYTES = {};

  private int size;
  private byte[] bytes;

  /**
   * 默认构造 空 byte[] 的 BytesWritable.
   */
  public BytesWritable() {
    this(EMPTY_BYTES);
  }

  /**
   * 构造值为给定 byte 数组的 BytesWritable.
   *
   * @param bytes
   */
  public BytesWritable(byte[] bytes) {
    this.bytes = bytes;
    this.size = bytes.length;
  }

  /**
   * 返回此 byte 数组的内容
   *
   * @return 此 byte 数组的内容，数据只有在 bytes[0, getLength() - 1]直接有效
   */
  public byte[] getBytes() {
    return bytes;
  }

  /**
   * 返回内容长度.
   */
  public int getLength() {
    return size;
  }

  /**
   * 更改数组内容长度.
   *
   * <p>
   * 注意：更改后，有效数据内容只由bytes[0, 更改前 getLength() -1]， 新增加的数组内容是未定义的，数组的容量按需增加。
   *
   * @param size
   *     新内容长度
   */
  public void setSize(int size) {
    if (size > getCapacity()) {
      setCapacity(size * 3 / 2);
    }
    this.size = size;
  }

  /**
   * 获取字节数组容量.
   *
   * @return 字节数组容量，即：bytes.length.
   */
  public int getCapacity() {
    return bytes.length;
  }

  /**
   * 更该字节数组容量
   *
   * @param new_cap
   *     新容量大小
   */
  public void setCapacity(int new_cap) {
    if (new_cap != getCapacity()) {
      byte[] new_data = new byte[new_cap];
      if (new_cap < size) {
        size = new_cap;
      }
      if (size != 0) {
        System.arraycopy(bytes, 0, new_data, 0, size);
      }
      bytes = new_data;
    }
  }

  /**
   * 更新数据内容为给定 BytesWritable 的数据.
   *
   * <p>
   * 等价于：set(newData.getBytes(), 0, newData.getLength())
   *
   * @param newData
   *     新数据
   */
  public void set(BytesWritable newData) {
    set(newData.bytes, 0, newData.size);
  }

  /**
   * 更新数据内容为给定字节数组中的数据.
   *
   * @param newData
   *     源数组
   * @param offset
   *     数据内容在 newData 中的偏移量
   * @param length
   *     数据内容长度
   */
  public void set(byte[] newData, int offset, int length) {
    setSize(0);
    setSize(length);
    System.arraycopy(newData, offset, bytes, 0, size);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    setSize(0); // clear the old data
    setSize(in.readInt());
    in.readFully(bytes, 0, size);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
    out.write(bytes, 0, size);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * 判断两个 BytesWritable 内容是否相同.
   *
   * @param right_obj
   * @return 如果两个都是 BytesWritable 且内容相同，返回true，否则返回false
   */
  @Override
  public boolean equals(Object right_obj) {
    if (right_obj instanceof BytesWritable) {
      return super.equals(right_obj);
    }
    return false;
  }

  /**
   * 输出数据的十六进制内容.
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer(3 * size);
    for (int idx = 0; idx < size; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        sb.append(' ');
      }
      String num = Integer.toHexString(0xff & bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        sb.append('0');
      }
      sb.append(num);
    }
    return sb.toString();
  }

  /**
   * BytesWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(BytesWritable.class);
    }

    /**
     * 基于二进制内容的 LongWritable 对象比较函数.
     *
     * <p>
     * 直接使用 {@link #compareBytes(byte[], int, int, byte[], int, int)} 进行二进制内容的比较
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, b2, s2
                                                                        + LENGTH_BYTES,
                          l2 - LENGTH_BYTES);
    }
  }

  static { // register this comparator
    WritableComparator.define(BytesWritable.class, new Comparator());
  }

}
