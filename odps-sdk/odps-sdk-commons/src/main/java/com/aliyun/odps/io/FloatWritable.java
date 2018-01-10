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
 * FloatWritable 提供了 float 的 {@link Writable} 和 {@link WritableComparable} 的实现
 */
public class FloatWritable implements WritableComparable<FloatWritable> {

  private float value = 0.0f;

  /**
   * 默认构造 float 值为0.0的 FloatWritable.
   */
  public FloatWritable() {

  }

  /**
   * 构造 float 值为给定 value 的 FloatWritable.
   *
   * @param value
   */
  public FloatWritable(float value) {
    set(value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readFloat();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(value);
  }

  /**
   * 设置 FloatWritable 的值
   *
   * @param value
   */
  public void set(float value) {
    this.value = value;
  }

  /**
   * 返回此 FloatWritable 的值
   *
   * @return
   */
  public float get() {
    return value;
  }

  /**
   * 判断两个 FloatWritable 是否相等.
   *
   * <p>
   * 如果两个都是 FloatWritable 且值相等，返回true，否则返回false
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (o.getClass() != getClass()) {
      return false;
    }

    return this.value == ((FloatWritable) o).value;
  }

  /**
   * 计算哈希值，直接返回(int) Float.floatToIntBits
   */
  @Override
  public int hashCode() {
    return (int) Float.floatToIntBits(this.value);
  }

  /**
   * 比较两个 FloatWritable 的值.
   *
   * @param o
   *     比较对象
   * @return (this < o ? -1 : (this == o) ? 0 ：1))
   */
  @Override
  public int compareTo(FloatWritable o) {
    return (value < o.value ? -1 : (value == o.value ? 0 : 1));
  }

  @Override
  public String toString() {
    return Float.toString(value);
  }

  /**
   * FloatWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(FloatWritable.class);
    }

    /**
     * 基于二进制内容的 FloatWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      float thisValue = readFloat(b1, s1);
      float thatValue = readFloat(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static { // register this comparator
    WritableComparator.define(FloatWritable.class, new Comparator());
  }
}
