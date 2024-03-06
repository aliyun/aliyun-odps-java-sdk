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
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * <code>RecordReader</code>用来读取记录
 */
public interface RecordReader extends Closeable, Iterable<Record> {

  /**
   * 读取一条记录
   *
   * @return 成功读到记录返回{@link Record}对象, 读取完成返回null
   * @throws IOException
   *     读取过程发生异常, 发生异常后不可重试
   */
  public Record read() throws IOException;

  /**
   * 提供一个迭代器来遍历所有的记录。
   * <p>
   * 默认实现会创建一个匿名的 {@link Iterator} 对象，
   * 该对象的 {@code hasNext} 和 {@code next} 方法依赖于 {@code read} 方法。
   *
   * @return 一个新的 {@link Iterator} 对象，用于遍历记录。
   */
  @Override
  default Iterator<Record> iterator() {
    return new Iterator<Record>() {
      private Record nextRecord;

      {
        // 初始化时尝试读取第一条记录。
        try {
          nextRecord = read();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public boolean hasNext() {
        // 如果 nextRecord 不为 null，则表示还有更多记录。
        return nextRecord != null;
      }

      @Override
      public Record next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Record current = nextRecord;
        // 在返回当前记录的同时，尝试读取下一条记录。
        try {
          nextRecord = read();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return current;
      }
    };
  }
}
