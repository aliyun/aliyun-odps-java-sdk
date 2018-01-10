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

/**
 * AbstractDatetimeWritable 提供了 datetime 的 {@link Writable} 和 {@link WritableComparable}
 * 的实现
 */
public abstract class AbstractDatetimeWritable<T extends AbstractDatetimeWritable>
        implements WritableComparable<T> {

  protected long value;

  /**
   * 默认构造 datetime 值为0的 AbstractDatetimeWritable.
   */
  AbstractDatetimeWritable() {
  }

  /**
   * 构造时间刻度值为给定 value 的 AbstractDatetimeWritable.
   *
   * @param value 给定时间刻度，精确到毫秒
   */
  AbstractDatetimeWritable(long value) {
    set(value);
  }

  /**
   * 设置 AbstractDatetimeWritable 的值
   *
   * @param value 要设置的时间刻度值，精确到毫秒
   */
  public void set(long value) {
    this.value = value;
  }

  /**
   * 返回此 AbstractDatetimeWritable 的值
   *
   * @return 这个writable对应的时间刻度，精确到毫秒
   */
  public long get() {
    return value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(value);
  }

  /**
   * 判断两个 AbstractDatetimeWritable 是否相等.
   *
   * <p>
   * 如果两个都是 AbstractDatetimeWritable 且值相等，返回true，否则返回false
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) {
      return false;
    }
    T other = (T) o;
    return this.value == other.value;
  }

  /**
   * 计算哈希值，直接返回此 AbstractDatetimeWritable 的值.
   */
  @Override
  public int hashCode() {
    return (int) value;
  }

  /**
   * 比较两个 AbstractDatetimeWritable 的值.
   *
   * @param o
   *     比较对象
   * @return (this < o ? -1 : (this == o) ? 0 ：1))
   */
  @Override
  public int compareTo(T o) {
    return this.value < o.value ? -1 : (this.value == o.value ? 0 : 1);
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }
}
