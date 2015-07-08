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
 * LongWritable 提供了 long 的 {@link Writable} 和 {@link WritableComparable} 的实现
 */
@SuppressWarnings("rawtypes")
public class LongWritable implements WritableComparable {

  private long value;

  /**
   * 默认构造 long 值为0的 LongWritable.
   */
  public LongWritable() {
  }

  /**
   * 构造 long 值为给定 value 的 LongWritable.
   *
   * @param value
   */
  public LongWritable(long value) {
    set(value);
  }

  /**
   * 设置 LongWritable 的值
   *
   * @param value
   */
  public void set(long value) {
    this.value = value;
  }

  /**
   * 返回此 LongWritable 的值
   *
   * @return
   */
  public long get() {
    return value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(value);
  }

  /**
   * 判断两个 LongWritable 是否相等.
   *
   * <p>
   * 如果两个都是 LongWritable 且值相等，返回true，否则返回false
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof LongWritable)) {
      return false;
    }
    LongWritable other = (LongWritable) o;
    return this.value == other.value;
  }

  /**
   * 计算哈希值，直接返回此 LongWritable 的值.
   */
  @Override
  public int hashCode() {
    return (int) value;
  }

  /**
   * 比较两个 LongWritable 的值.
   *
   * @param o
   *     比较对象
   * @return (this < o ? -1 : (this == o) ? 0 ：1))
   */
  @Override
  public int compareTo(Object o) {
    long thisValue = this.value;
    long thatValue = ((LongWritable) o).value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }

  /**
   * LongWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(LongWritable.class);
    }

    /**
     * 基于二进制内容的 LongWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      long thisValue = readLong(b1, s1);
      long thatValue = readLong(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  /**
   * LongWritable 对象的 {@link WritableComparator} 降序实现.
   */
  public static class DecreasingComparator extends Comparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return -super.compare(a, b);
    }

    /**
     * 基于二进制内容的 LongWritable 对象比较函数.
     *
     * @return 对象1>对象2，返回-1，对象1=对象2，返回0，对象1<对象2，返回1
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  static { // register default comparator
    WritableComparator.define(LongWritable.class, new Comparator());
  }

}
