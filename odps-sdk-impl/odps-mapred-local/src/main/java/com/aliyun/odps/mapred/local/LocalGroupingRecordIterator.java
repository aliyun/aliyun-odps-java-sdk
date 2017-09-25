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

package com.aliyun.odps.mapred.local;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

import com.aliyun.odps.Column;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.bridge.WritableRecord;

/**
 * @author tedxu
 */
public class LocalGroupingRecordIterator implements Iterator<Record> {

  private WritableRecord key;
  private WritableRecord value;
  private Object[] prefetch; // Prefetched Objects
  private boolean hasNext; // True if next object is fetched and compared as
  // still the same group as
  // key.
  private Comparator<Object[]> keyGroupingComparator;
  private Queue<Object[]> queue;

  private boolean incInputRecordCount;
  private Counters counters;

  private Column[] columns;

  public LocalGroupingRecordIterator(Queue<Object[]> queue, WritableRecord key,
                                     Column[] columns,
                                     Comparator<Object[]> keyGroupingComparator,
                                     boolean incInputRecordCount, Counters counters) {
    this.queue = queue;
    this.key = key;
    this.columns = columns;
    this.keyGroupingComparator = keyGroupingComparator;
    this.incInputRecordCount = incInputRecordCount;
    this.counters = counters;
  }

  private void fillKeyValue(Object[] objs) {
    key.set(Arrays.copyOf(objs, key.getColumnCount()));

    value = new WritableRecord(columns);
    value.set(Arrays.copyOfRange(objs, key.getColumnCount(),
                                 key.getColumnCount() + value.getColumnCount()));
    if (incInputRecordCount) {
      counters.findCounter(JobCounter.__EMPTY_INPUT_RECORD_COUNT).increment(1);
    }
  }

  @Override
  public boolean hasNext() {
    if (prefetch != null) {
      // If prefetched, it must has been compared, return compare result
      // immediately.
      return hasNext;
    } else {
      prefetch = queue.poll();
    }
    if (prefetch == null
        || keyGroupingComparator.compare(key.toWritableArray(), prefetch) != 0) {
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
