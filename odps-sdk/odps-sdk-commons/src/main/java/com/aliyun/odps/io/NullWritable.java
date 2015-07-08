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
 * NullWritable 提供了 null （空值）的 {@link Writable} 和 {@link WritableComparable}
 * 的实现.
 *
 * <p>
 * NullWritable 使用单子模式，不允许自行创建，只能通过 {@link #get()} 方法取得。
 */
@SuppressWarnings("rawtypes")
public class NullWritable implements WritableComparable {

  private static final NullWritable THIS = new NullWritable();

  private NullWritable() {
  } // no public ctor

  /**
   * 获取 NullWritable 实例.
   *
   * <p>
   * NullWritable 使用单子模式，不允许自行创建，只能通过本方法取得。
   *
   * @return NullWritable 实例
   */
  public static NullWritable get() {
    return THIS;
  }

  @Override
  public String toString() {
    return "(null)";
  }

  /**
   * 返回哈希值 0.
   */
  @Override
  public int hashCode() {
    return 0;
  }

  /**
   * 比较两个 NullWritable 的值.
   *
   * @param other
   *     比较对象
   * @return 如果 other 也为 NullWritable，返回0，否则抛 {@link ClassCastException} 异常
   */
  @Override
  public int compareTo(Object other) {
    if (!(other instanceof NullWritable)) {
      throw new ClassCastException("can't compare "
                                   + other.getClass().getName() + " to NullWritable");
    }
    return 0;
  }

  /**
   * 判断两个对象是否相等.
   *
   * @param other
   *     比较对象
   * @return 如果 other 也为 NullWritable，返回true，否则，返回false
   */
  @Override
  public boolean equals(Object other) {
    return other instanceof NullWritable;
  }

  /**
   * NullWritable 不读入任何内容
   */
  @Override
  public void readFields(DataInput in) throws IOException {
  }

  /**
   * NullWritable 不输出任何内容
   */
  @Override
  public void write(DataOutput out) throws IOException {
  }

  /**
   * NullWritable 对象的 {@link WritableComparator} 实现.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(NullWritable.class);
    }

    /**
     * 基于二进制内容的 NullWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      assert 0 == l1;
      assert 0 == l2;
      return 0;
    }
  }

  static { // register this comparator
    WritableComparator.define(NullWritable.class, new Comparator());
  }
}
