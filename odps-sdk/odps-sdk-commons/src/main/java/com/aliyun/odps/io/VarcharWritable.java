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

import com.aliyun.odps.data.Varchar;

/**
 * VarcharWritable 提供了 varchar 的 {@link Writable} 和 {@link WritableComparable} 的实现
 */
public class VarcharWritable extends Text {

  public VarcharWritable() {
  }

  /**
   * 根据给定字符串构造 VarcharWritable
   */
  public VarcharWritable(String value) {
    super(value);
  }

  /**
   * 根据给定Varchar对象构造 VarcharWritable
   */
  public VarcharWritable(Varchar varcharVal) {
    this(varcharVal.getValue());
  }

  public Varchar get() {
    return new Varchar(getText());
  }

  public String getText() {
    return super.toString();
  }

  public void set(Varchar varchar) {
    set(varchar.getValue());
  }

  static { // register this comparator
    WritableComparator.define(VarcharWritable.class, new Text.Comparator(VarcharWritable.class));
  }
}
