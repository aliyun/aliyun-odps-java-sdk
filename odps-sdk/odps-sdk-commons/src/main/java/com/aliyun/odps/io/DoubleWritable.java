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
 * DoubleWritable 提供了 double 的 {@link Writable} 和 {@link WritableComparable} 的实现
 */
@SuppressWarnings("rawtypes")
public class DoubleWritable implements WritableComparable {

  private double value = 0.0;

  /**
   * 默认构造 double 值为0.0的 DoubleWritable.
   */
  public DoubleWritable() {

  }

  /**
   * 构造 double 值为给定 value 的 DoubleWritable.
   *
   * @param value
   */
  public DoubleWritable(double value) {
    set(value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readDouble();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(value);
  }

  /**
   * 设置 DoubleWritable 的值
   *
   * @param value
   */
  public void set(double value) {
    this.value = value;
  }

  /**
   * 返回此 DoubleWritable 的值
   *
   * @return
   */
  public double get() {
    return value;
  }

  /**
   * 判断两个 DoubleWritable 是否相等.
   *
   * <p>
   * 如果两个都是 DoubleWritable 且值相等，返回true，否则返回false
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DoubleWritable)) {
      return false;
    }
    DoubleWritable other = (DoubleWritable) o;
    return this.value == other.value;
  }

  /**
   * 计算哈希值，直接返回(int) Double.doubleToLongBits.
   */
  @Override
  public int hashCode() {
    return (int) Double.doubleToLongBits(value);
  }

  /**
   * 比较两个 DoubleWritable 的值.
   *
   * @param o
   *     比较对象
   * @return (this < o ? -1 : (this == o) ? 0 ：1))
   */
  @Override
  public int compareTo(Object o) {
    DoubleWritable other = (DoubleWritable) o;
    return (value < other.value ? -1 : (value == other.value ? 0 : 1));
  }

  @Override
  public String toString() {
    return Double.toString(value);
  }

  /**
   * DoubleWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(DoubleWritable.class);
    }

    /**
     * 基于二进制内容的 DoubleWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      double thisValue = readDouble(b1, s1);
      double thatValue = readDouble(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static { // register this comparator
    WritableComparator.define(DoubleWritable.class, new Comparator());
  }

}
