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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * TimestampNtzWritable 提供了 LocalDateTime 的 {@link Writable} 和 {@link WritableComparable}
 * 的实现
 */
public class TimestampNtzWritable implements WritableComparable<TimestampNtzWritable> {

  private long seconds = 0;
  private int nanos = 0;

  /**
   * 默认构造值为0的 TimestampNtzWritable.
   */
  public TimestampNtzWritable() {
    super();
  }

  /**
   * 根据给定的LocalDateTime对象构造TimestampNtzWritable
   *
   * @param value 给定的 LocalDateTime 对象
   */
  public TimestampNtzWritable(LocalDateTime value) {
    set(value);
  }

  /**
   * 根据给定的秒和纳秒值构造TimestampNtzWritable
   * @param seconds 总秒数
   * @param nanos 纳秒值
   */
  public TimestampNtzWritable(long seconds, int nanos) {
    set(seconds, nanos);
  }

  /**
   * 获取该writable对应的LocalDateTime对象
   */
  public LocalDateTime get() {
    return LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC);
  }

  /**
   * 获取该writable对应的秒数
   */
  public long getSeconds() {
    return seconds;
  }

  /**
   * 获取该writable对应的纳秒的部分
   */
  public int getNanos() {
    return nanos;
  }

  /**
   * 根据给定的秒和纳秒值设置TimestampNtzWritable的值
   * @param seconds 秒数
   * @param nanos 纳秒值
   */
  public void set(long seconds, int nanos) {
    java.time.temporal.ChronoField.NANO_OF_SECOND.checkValidValue(nanos);
    this.seconds = seconds;
    this.nanos = nanos;
  }

  /**
   * 根据给定的 LocalDateTime 对象构造TimestampNtzWritable
   */
  public void set(LocalDateTime ld) {
    Instant instant = ld.atOffset(ZoneOffset.UTC).toInstant();
    set(instant.getEpochSecond(), instant.getNano());
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

    TimestampNtzWritable that = (TimestampNtzWritable) o;

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
  public int compareTo(TimestampNtzWritable o) {
    if (seconds == o.seconds) {
      return nanos < o.nanos ? -2 : (nanos == o.nanos ? 0 : 2);
    }
    return seconds < o.seconds ? -1 : 1;
  }

  /**
   * TimestampNtzWritable 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(TimestampNtzWritable.class);
    }

    /**
     * 基于二进制内容的 TimestampNtzWritable 对象比较函数.
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
   * TimestampNtzWritable 对象的 {@link WritableComparator} 降序实现.
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
    WritableComparator.define(TimestampNtzWritable.class, new Comparator());
  }

}
