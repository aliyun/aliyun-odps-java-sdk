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

package com.aliyun.odps.data;

import java.io.Closeable;
import java.io.IOException;

/**
 * <code>RecordWriter</code>用来写入一条记录
 */
public interface RecordWriter extends Closeable {

  /**
   * 写入一条记录
   *
   * @param r
   *     {@link Record}对象
   * @throws IOException
   *     写入过程发生异常，不可重试
   */
  void write(Record r) throws IOException;

  /**
   * 删除一条记录，仅主键表支持，其他表将抛出 RuntimeException
   *
   * @param r
   *     {@link Record}对象
   * @throws IOException
   *     写入过程发生异常，不可重试
   */
  default void delete(Record r) throws IOException {
    throw new IllegalStateException(
      "Delete operation is only supported for tables with a primary key (Delta Table).");
  }

  /**
   * 创建一个可被写入的 Record
   * @param caseSensitive Record setByName 时是否区分大小写
   * @return ArrayRecord
   */
  default Record newRecord(boolean caseSensitive) {
    throw new UnsupportedOperationException("Class " + this.getClass().getName() + " not support `newRecord` method.");
  }
}
