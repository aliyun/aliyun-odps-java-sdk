/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.io;

import com.aliyun.odps.data.IntervalYearMonth;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * IntervalYearMonthWritable 提供了 interval_year_month 的 {@link Writable} 和 {@link WritableComparable}
 * 的实现
 */
public class IntervalYearMonthWritable implements WritableComparable<IntervalYearMonthWritable> {

  private final static int MONTHS_PER_YEAR = 12;
  private int totalMonths;

  /**
   * 默认构造 interval 值为0的 IntervalYearMonthWritable.
   */
  public IntervalYearMonthWritable() {
  }

  /**
   * 根据给定的 IntervalYearMonth 对象构造 IntervalYearMonthWritable
   *
   * @param value
   */
  public IntervalYearMonthWritable(IntervalYearMonth value) {
    this(value.getTotalMonths());
  }

  /**
   * 构造总月数为totalMonths的interval writable
   *
   * @param totalMonths 该interval的总月数
   */
  public IntervalYearMonthWritable(int totalMonths) {
    set(totalMonths);
  }

  /**
   * 返回此 IntervalYearMonthWritable 的值
   *
   * @return 这个interval对应的IntervalYearMonth对象
   */
  public IntervalYearMonth get() {
    return new IntervalYearMonth(totalMonths);
  }

  /**
   * 设置该IntervalYearMonthWritable的值
   *
   * @param totalMonths 总月数
   */
  public void set(int totalMonths) {
    this.totalMonths = totalMonths;
  }

  /**
   * 返回此 IntervalYearMonthWritable 的总月数
   *
   * @return 这个interval对应的总月数
   */
  public int getTotalMonths() {
    return totalMonths;
  }

  /**
   * 使用IntervalYearMonth对象设置该writable的值
   */
  public void set(IntervalYearMonth interval) {
    set(interval.getTotalMonths());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(totalMonths);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    totalMonths = in.readInt();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IntervalYearMonthWritable that = (IntervalYearMonthWritable) o;

    return totalMonths == that.totalMonths;

  }

  @Override
  public int hashCode() {
    return totalMonths;
  }

  @Override
  public int compareTo(IntervalYearMonthWritable o) {
    return totalMonths < o.totalMonths ? -2 : totalMonths == o.totalMonths ? 0 : 2;
  }

  /**
   * IntervalYearMonthWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(IntervalYearMonthWritable.class);
    }

    /**
     * 基于二进制内容的 IntervalYearMonthWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      long thisValue = readLong(b1, s1);
      long thatValue = readLong(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  /**
   * IntervalYearMonthWritable 对象的 {@link WritableComparator} 降序实现.
   */
  public static class DecreasingComparator extends Comparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return -super.compare(a, b);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  static { // register default comparator
    WritableComparator.define(IntervalYearMonthWritable.class, new Comparator());
  }

}
