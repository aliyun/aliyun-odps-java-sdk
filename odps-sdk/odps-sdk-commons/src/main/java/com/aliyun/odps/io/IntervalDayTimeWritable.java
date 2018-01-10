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

import com.aliyun.odps.data.IntervalDayTime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * IntervalDayTimeWritable 提供了 interval_day_time 的 {@link Writable} 和 {@link WritableComparable}
 * 的实现
 */
public class IntervalDayTimeWritable implements WritableComparable<IntervalDayTimeWritable> {

  private long seconds = 0;
  private int nanos = 0;

  /**
   * 默认构造 interval 值为0的 IntervalDayTimeWritable.
   */
  public IntervalDayTimeWritable() {
    super();
  }

  /**
   * 根据给定的IntervalDayTime对象构造IntervalDayTimeWritable
   *
   * @param value 给定的IntervalDayTime对象
   */
  public IntervalDayTimeWritable(IntervalDayTime value) {
    set(value);
  }

  /**
   * 根据给定的秒和纳秒值构造IntervalDayTimeWritable
   * @param totalSeconds 总秒数
   * @param nanos 纳秒值
   */
  public IntervalDayTimeWritable(long totalSeconds, int nanos) {
    set(totalSeconds, nanos);
  }

  /**
   * 获取该writable对应的IntervalDayTime对象
   */
  public IntervalDayTime get() {
    return new IntervalDayTime(seconds, nanos);
  }

  /**
   * 获取该writable对应的种秒数间隔
   */
  public long getTotalSeconds() {
    return seconds;
  }

  /**
   * 获取该writable对应的时间间隔中秒以下精确到纳秒的部分
   */
  public int getNanos() {
    return nanos;
  }

  /**
   * 根据给定的秒和纳秒值设置IntervalDayTimeWritable的值
   * @param totalSeconds 总秒数
   * @param nanos 纳秒值
   */
  public void set(long totalSeconds, int nanos) {
    this.seconds = totalSeconds;
    this.nanos = nanos;
  }

  /**
   * 根据给定的 IntervalDayTime 对象构造IntervalDayTimeWritable
   */
  public void set(IntervalDayTime interval) {
    set(interval.getTotalSeconds(), interval.getNanos());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    seconds = in.readLong();
    nanos = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(seconds);
    out.writeInt(nanos);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IntervalDayTimeWritable that = (IntervalDayTimeWritable) o;

    if (seconds != that.seconds) return false;
    return nanos == that.nanos;

  }

  @Override
  public int hashCode() {
    int result = (int) (seconds ^ (seconds >>> 32));
    result = 31 * result + nanos;
    return result;
  }

  @Override
  public int compareTo(IntervalDayTimeWritable o) {
    if (seconds == o.seconds) {
      return nanos < o.nanos ? -2 : (nanos == o.nanos ? 0 : 2);
    }
    return seconds < o.seconds ? -1 : 1;
  }

  /**
   * IntervalDayTimeWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(IntervalDayTimeWritable.class);
    }

    /**
     * 基于二进制内容的 IntervalDayTimeWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      long thisValue = readLong(b1, s1);
      long thatValue = readLong(b2, s2);
      if (thisValue == thatValue) {
        int thisNano = readInt(b1, s1 + 8);
        int thatNano = readInt(b2, s2 + 8);
        return thisNano < thatNano ? -2 : thisNano == thatNano ? 0 : 2;
      }
      return thisValue < thatValue ? -1 : 1;
    }
  }

  /**
   * IntervalDayTimeWritable 对象的 {@link WritableComparator} 降序实现.
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
    WritableComparator.define(IntervalDayTimeWritable.class, new Comparator());
  }

}
