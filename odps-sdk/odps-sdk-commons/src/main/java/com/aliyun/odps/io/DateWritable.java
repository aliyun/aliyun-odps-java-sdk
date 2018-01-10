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

/**
 * DateWritable 提供了 date 的 {@link Writable} 和 {@link WritableComparable}
 * 的实现。
 */
public class DateWritable extends AbstractDatetimeWritable<DateWritable> {

  /**
   * 默认构造 datetime 值为0的 DatetimeWritable.
   */
  public DateWritable() {
  }

  /**
   * 构造 long 值为给定 value 的 DatetimeWritable.
   *
   * @param value
   */
  public DateWritable(long value) {
    super(value);
  }

  /**
   * 获取该writable对应的 java.sql.Date 对象
   */
  public java.sql.Date getDate() {
    return new java.sql.Date(get());
  }

  /**
   * DatetimeWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(DateWritable.class);
    }

    /**
     * 基于二进制内容的 DatetimeWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      long thisValue = readLong(b1, s1);
      long thatValue = readLong(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  /**
   * DatetimeWritable 对象的 {@link WritableComparator} 降序实现.
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
    WritableComparator.define(DateWritable.class, new Comparator());
  }

}
