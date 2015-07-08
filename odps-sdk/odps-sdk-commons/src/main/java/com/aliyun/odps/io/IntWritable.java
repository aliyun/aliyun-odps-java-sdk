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
 * IntWritable 提供了 int 的 {@link Writable} 和 {@link WritableComparable} 的实现
 */
@SuppressWarnings("rawtypes")
public class IntWritable implements WritableComparable {

  private int value;

  /**
   * 默认构造int 值为0的 IntWritable.
   */
  public IntWritable() {
  }

  /**
   * 构造int 值为给定 value 的 IntWritable.
   *
   * @param value
   */
  public IntWritable(int value) {
    set(value);
  }

  /**
   * 设置 IntWritable 的值
   *
   * @param value
   */
  public void set(int value) {
    this.value = value;
  }

  /**
   * 返回此 IntWritable 的值
   *
   * @return
   */
  public int get() {
    return value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(value);
  }

  /**
   * 判断两个 IntWritable 是否相等.
   *
   * <p>
   * 如果两个都是 IntWritable 且值相等，返回true，否则返回false
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IntWritable)) {
      return false;
    }
    IntWritable other = (IntWritable) o;
    return this.value == other.value;
  }

  /**
   * 计算哈希值，直接返回此 IntWritable 的值.
   */
  @Override
  public int hashCode() {
    return value;
  }

  /**
   * 比较两个 IntWritable 的值.
   *
   * @param o
   *     比较对象
   * @return (this < o ? -1 : (this == o) ? 0 ：1))
   */
  @Override
  public int compareTo(Object o) {
    int thisValue = this.value;
    int thatValue = ((IntWritable) o).value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  @Override
  public String toString() {
    return Integer.toString(value);
  }

  /**
   * IntWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(IntWritable.class);
    }

    /**
     * 基于二进制内容的 IntWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int thisValue = readInt(b1, s1);
      int thatValue = readInt(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static { // register this comparator
    WritableComparator.define(IntWritable.class, new Comparator());
  }
}
