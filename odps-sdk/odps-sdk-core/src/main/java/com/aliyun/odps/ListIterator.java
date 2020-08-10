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

package com.aliyun.odps;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * 支持通过{@link #list()}自定义的Iterator
 */
public abstract class ListIterator<E> implements Iterator<E> {

  /**
   * 获取数据
   *
   * @return 返回null表示迭代结束
   */
  protected abstract List<E> list();

  private LinkedList<E> cache = new LinkedList<E>();

  @Override
  public boolean hasNext() {
    while (cache.size() == 0) {
      List<E> list = list();
      if (list == null) {
        return false;
      } else {
        cache.addAll(list);
      }
    }
    return cache.size() > 0;
  }

  @Override
  public E next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return cache.pollFirst();
  }

  @Override
  public void remove() {
    throw new RuntimeException("Method not supported.");
  }

}
