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

import com.aliyun.odps.data.Char;

/**
 * CharWritable 提供了 char 的 {@link Writable} 和 {@link WritableComparable} 的实现
 */
public class CharWritable extends Text {

  public CharWritable() {
  }

  /**
   * 根据给定字符串构造 CharWritable
   */
  public CharWritable(String value) {
    super(value);
  }

  /**
   * 根据给定Char对象构造 CharWritable
   */
  public CharWritable(Char charVal) {
    this(charVal.getValue());
  }

  /**
   * 返回此 writable 对应的Char对象
   */
  public Char get() {
    return new Char(toString());
  }

  /**
   * 使用 Char 对象设置 writable 值
   */
  public void set(Char chr) {
    set(chr.getValue());
  }

  static { // register this comparator
    WritableComparator.define(CharWritable.class, new Text.Comparator(CharWritable.class));
  }
}
