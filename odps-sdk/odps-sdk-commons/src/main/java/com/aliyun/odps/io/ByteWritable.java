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
 * ByteWritable 提供了 tinyint 的 {@link Writable} 和 {@link WritableComparable} 的实现
 */
public class ByteWritable implements WritableComparable<ByteWritable> {

  private byte value;

  /*
  * 默认构造值为零的 ByteWritable
  */
  public ByteWritable() {
  }

  /*
  * 构造tinyint 值为给定的value 的 ByteWritable
  *
  * @param value
  */
  public ByteWritable(byte value) {
    this.value = value;
  }


  /**
   * 序列化到指定的 {@link DataOutput} out.
   *
   * @param out
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(this.value);
  }

  /**
   * 从指定的 {@link DataInput} in 反序列化.
   *
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.value = in.readByte();
  }

  /**
   * 判断两个 tinyintWritable 是否相等.
   *
   * 如果两个都是 tinyintWritable 且值相等，返回true，否则返回false
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) {
      return false;
    }

    return this.value == ((ByteWritable) o).value;
  }

  /**
   * 计算哈希值，直接返回此 ByteWritable 的值.
   */
  @Override
  public int hashCode() {
    return value;
  }

  @Override
  public String toString() {
    return Byte.toString(value);
  }

  /**
   * 比较两个 tinyintWritable 的值.
   *
   * @param o
   *     比较对象
   * @return (this < o ? -1 : (this == o) ? 0 ：1))
   */
  @Override
  public int compareTo(ByteWritable o) {
    return (this.value < o.value ? -1 : (this.value == o.value ? 0 : 1));
  }

  /*
  * 返回ByteWritable的值
  *
  * @return
  */
  public byte get() {
    return value;
  }

  /*
  * 设置ByteWritable的值
  *
  * @param value
  */
  public void set(byte value) {
    this.value = value;
  }

  /**
   * ByteWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(ByteWritable.class);
    }

    /**
     * 基于二进制内容的 ByteWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      byte thisValue = readByte(b1, s1);
      byte thatValue = readByte(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static { // register this comparator
    WritableComparator.define(ByteWritable.class, new Comparator());
  }
}
