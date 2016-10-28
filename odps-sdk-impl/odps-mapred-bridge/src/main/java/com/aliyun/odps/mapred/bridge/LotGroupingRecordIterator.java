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

package com.aliyun.odps.mapred.bridge;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.bridge.LotReducerUDTF.ReduceContextImpl;

public class LotGroupingRecordIterator implements Iterator<Record> {

  // key 和 value将被复用，减少重复创建对象的开销
  private WritableRecord key;
  private WritableRecord value;

  private Object[] prefetch;
  private boolean hasNext;

  private Comparator<Object[]> keyGroupingComparator;
  private ReduceContextImpl ctx;

  public LotGroupingRecordIterator(ReduceContextImpl ctx,
                                   Comparator<Object[]> keyGroupingComparator, Object[] prefetch,
                                   WritableRecord key,
                                   WritableRecord value) {

    if (prefetch == null) {
      throw new IllegalArgumentException("prefetch can't be null");
    }

    this.ctx = ctx;
    this.keyGroupingComparator = keyGroupingComparator;
    this.prefetch = prefetch;
    this.key = key;
    this.value = value;
    // 初始化能够保证有数据，否则是不会创建该对象的
    hasNext = true;
  }

  private void fillKeyValue(Object[] objs) {
    key.set(Arrays.copyOf(objs, key.getColumnCount()));
    value.set(Arrays.copyOfRange(objs, key.getColumnCount(), objs.length));
  }

  @Override
  public boolean hasNext() {
    if (prefetch != null) {
      return hasNext;
    } else {
      prefetch = ctx.getData();
    }
    if (prefetch == null || keyGroupingComparator.compare(key.toWritableArray(), prefetch) != 0) {
      hasNext = false;
    } else {
      hasNext = true;
      fillKeyValue(prefetch);
    }
    return hasNext;
  }

  public boolean reset() {
    if (prefetch != null) {
      hasNext = true;
      fillKeyValue(prefetch);
      return true;
    }
    return false;
  }

  @Override
  public Record next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    prefetch = null;
    return value;
  }

  @Override
  public void remove() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    prefetch = null;
  }

}
