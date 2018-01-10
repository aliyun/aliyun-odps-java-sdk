/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
 * ShortWritable 提供了 smallint 的 {@link Writable} 和 {@link WritableComparable} 的实现
 */
public class ShortWritable implements WritableComparable<ShortWritable> {

  private short value;

  /*
  * 默认构造值为零的 ShortWritable
  */
  public ShortWritable() {
  }

  /*
  * 构造 smallint 值为给定的value 的 ShortWritable
  *
  * @param value
  */
  public ShortWritable(short value) {
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
    out.writeShort(this.value);
  }

  /**
   * 从指定的 {@link DataInput} in 反序列化.
   *
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.value = in.readShort();
  }

  /**
   * 判断两个 ShortWritable 是否相等.
   *
   * 如果两个都是 ShortWritable 且值相等，返回true，否则返回false
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (o.getClass() != getClass()) {
      return false;
    }

    return this.value == ((ShortWritable) o).value;
  }

  /**
   * 计算哈希值，直接返回此 ShortWritable 的值.
   */
  @Override
  public int hashCode() {
    return value;
  }

  @Override
  public String toString() {
    return Short.toString(value);
  }

  /**
   * 比较两个 ShortWritable 的值.
   *
   * @param o
   *     比较对象
   * @return (this < o ? -1 : (this == o) ? 0 ：1))
   */
  @Override
  public int compareTo(ShortWritable o) {
    return (this.value < o.value ? -1 : (this.value == o.value ? 0 : 1));
  }

  /*
  * 返回 ShortWritable 的值
  *
  * @return
  */
  public short get() {
    return value;
  }

  /*
  * 设置 ShortWritable 的值
  *
  * @param value
  */
  public void set(short value) {
    this.value = value;
  }

  /**
   * ShortWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(ShortWritable.class);
    }

    /**
     * 基于二进制内容的 ShortWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      short thisValue = readShort(b1, s1);
      short thatValue = readShort(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static { // register this comparator
    WritableComparator.define(ShortWritable.class, new Comparator());
  }
}
