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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;

/**
 * TimestampWritable 提供了 datetime 的 {@link Writable} 和 {@link WritableComparable}
 * 的实现
 */
public class TimestampWritable extends AbstractDatetimeWritable<TimestampWritable> {

  protected int nanos = 0;

  /**
   * 默认构造 datetime 值为0的 TimestampWritable.
   */
  public TimestampWritable() {
    super();
  }

  /**
   * 构造毫秒值为给定 value 的 TimestampWritable.
   *
   * @param time  精度为毫秒的时间刻度
   */
  public TimestampWritable(long time) {
    this(time / 1000, (int)(time % 1000) * 1000000);
  }

  /**
   * 根据给定的秒以及纳秒刻度构造 TimestampWritable
   *
   * @param seconds  精度为秒的时间刻度
   * @param nanos    秒以下精确到纳秒的时间刻度
   */
  public TimestampWritable(long seconds, int nanos) {
    set(seconds, nanos);
  }

  @Override
  public long get() {
    return value * 1000 + nanos / 1000000;
  }

  @Override
  public void set(long value) {
    super.set(value / 1000);
    this.nanos = (int)(value % 1000) * 1000000;
  }

  public long getTotalSeconds() {
    return value;
  }

  public int getNanos() {
    return nanos;
  }

  public Timestamp getTimestamp() {
    Timestamp ts = new Timestamp(value * 1000);
    ts.setNanos(nanos);
    return ts;
  }

  public void setTimestamp(Timestamp ts) {
    set(ts.getTime() / 1000, ts.getNanos());
  }

  public void set(long seconds, int nanos) {
    this.value = seconds + nanos / 1000000000;
    this.nanos = nanos % 1000000000;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readLong();
    nanos = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(value);
    out.writeInt(nanos);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    TimestampWritable that = (TimestampWritable) o;

    return nanos == that.nanos;

  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + nanos;
    return result;
  }

  @Override
  public int compareTo(TimestampWritable o) {
    int ret = super.compareTo(o);
    if (ret != 0) {
      return ret;
    }
    return nanos < o.nanos ? -2 : (nanos == o.nanos ? 0 : 2);
  }

  @Override
  public String toString() {
    return getTimestamp().toString();
  }

  /**
   * TimestampWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(TimestampWritable.class);
    }

    /**
     * 基于二进制内容的 TimestampWritable 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      long thisValue = readLong(b1, s1);
      int thisNano = readInt(b1, s1 + 8);
      long thatValue = readLong(b2, s2);
      int thatNano = readInt(b2, s2 + 8);
      if (thisValue == thatValue) {
        return thisNano < thatNano ? -2 : thisNano == thatNano ? 0 : 2;
      }
      return (thisValue < thatValue ? -1 : 1);
    }
  }

  /**
   * TimestampWritable 对象的 {@link WritableComparator} 降序实现.
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
    WritableComparator.define(TimestampWritable.class, new Comparator());
  }

}
