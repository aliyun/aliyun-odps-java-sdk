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
import java.util.List;

/**
 * StructWritable 提供了 struct 类型数据 的 {@link Writable}
 * 的实现
 */
public class StructWritable implements Writable {

  private List<Writable> values;

  /**
   * 构造 struct schema 值为给定 values 的 StructWritable.
   *
   * @param values
   */
  public StructWritable(List<Writable> values) {
    set(values);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    for (Writable val : this.values) {
      val.readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    for (Writable val : this.values) {
      val.write(out);
    }
  }

  /**
   * 设置 StructWritable 的值
   *
   * @param values
   */
  public void set(List<Writable> values) {
    this.values = values;
  }

  /**
   * 返回此 StructWritable 的值
   *
   * @return
   */
  public List<Writable> get() {
    return this.values;
  }

  /**
   * 判断两个 StructWritable 是否相等.
   *
   * <p>
   * 如果两个都是 StructWritable 且值相等，返回true，否则返回false
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (o.getClass() != getClass()) {
      return false;
    }

    StructWritable other = (StructWritable) o;
    if (this.values.size() != other.values.size()) {
      return false;
    }

    for (int ind = 0; ind < this.values.size(); ind++) {
      if ( (this.values.get(ind) == null && other.values.get(ind) != null) ||
              (this.values.get(ind) != null && other.values.get(ind) == null) ||
              !this.values.get(ind).equals(other.values.get(ind))) {
        return false;
      }
    }

    return true;
  }

  /**
   * 计算哈希值，直接返回(int) StructWritable.values.hashCode
   */
  @Override
  public int hashCode() {
    return this.values.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{");
    for (Writable val : this.values) {
      sb.append(val.toString() + ",");
    }
    sb.append("}");

    return sb.toString();
  }
}
