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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import com.aliyun.odps.data.Record;

/**
 * @author zhe.oyz
 */
public class NonGroupingRecordIterator implements Iterator<Record> {

  private WritableRecord value;
  private Object[] prefetch; // Prefetched Objects
  private int curIndex = 0;
  private List<Object[]> valueList;

  public NonGroupingRecordIterator(List<Object[]> curValuelist, WritableRecord value) {
    this.valueList = curValuelist;
    this.value = value;
  }

  private void fillKeyValue(Object[] objs) {
    value.set(Arrays.copyOf(objs, value.getColumnCount()));
  }

  @Override
  public boolean hasNext() {
    if (curIndex < valueList.size()) {
      return true;
    } else {
      return false;
    }
  }

  public boolean reset() {
    if (prefetch != null) {
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
    fillKeyValue(valueList.get(curIndex));
    curIndex++;
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
