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
 * BooleanWritable 提供了 boolean 的 {@link Writable} 和 {@link WritableComparable}
 * 的实现
 */
@SuppressWarnings("rawtypes")
public class BooleanWritable implements WritableComparable {

  private boolean value;

  /**
   * 默认构造 boolean 值为 false 的 BooleanWritable.
   */
  public BooleanWritable() {
  }

  ;

  /**
   * 构造 boolean 值为给定 value 的 BooleanWritable.
   *
   * @param value
   */
  public BooleanWritable(boolean value) {
    set(value);
  }

  /**
   * 设置 BooleanWritable 的值
   *
   * @param value
   */
  public void set(boolean value) {
    this.value = value;
  }

  /**
   * 返回此 BooleanWritable 的值
   *
   * @return
   */
  public boolean get() {
    return value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(value);
  }

  /**
   * 判断两个 BooleanWritable 是否相等.
   *
   * <p>
   * 如果两个都是 BooleanWritable 且值相等，返回true，否则返回false
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BooleanWritable)) {
      return false;
    }
    BooleanWritable other = (BooleanWritable) o;
    return this.value == other.value;
  }

  /**
   * 计算哈希值，返回(value ? 0 : 1).
   */
  @Override
  public int hashCode() {
    return value ? 0 : 1;
  }

  /**
   * 比较两个 BooleanWritable 的值.
   *
   * @param o
   *     比较对象
   * @return ((this == o) ? 0 : (this == false) ? -1 : 1)
   */
  @Override
  public int compareTo(Object o) {
    boolean a = this.value;
    boolean b = ((BooleanWritable) o).value;
    return ((a == b) ? 0 : (a == false) ? -1 : 1);
  }

  @Override
  public String toString() {
    return Boolean.toString(get());
  }

  /**
   * BooleanWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(BooleanWritable.class);
    }

    /**
     * 基于二进制内容的 BooleanWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      boolean a = (b1[s1] == 1) ? true : false;
      boolean b = (b2[s2] == 1) ? true : false;
      return ((a == b) ? 0 : (a == false) ? -1 : 1);
    }
  }

  static {
    WritableComparator.define(BooleanWritable.class, new Comparator());
  }
}
