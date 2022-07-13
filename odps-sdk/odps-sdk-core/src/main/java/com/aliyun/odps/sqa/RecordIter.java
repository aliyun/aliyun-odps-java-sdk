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

package com.aliyun.odps.sqa;

import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.type.TypeInfo;

class RecordIter<T> implements Iterator<Record> {

  Iterator<T> iter;
  Column[] columns;
  long offset = 0L;
  long countLimit = -1;

  RecordIter(Iterator<T> tableIterator, List<String> headers, List<TypeInfo> typeInfos) {
    this.iter = tableIterator;
    initColumns(headers, typeInfos);
  }

  @Override
  public boolean hasNext() {
    if (countLimit <= -1) {
      return iter.hasNext();
    } else {
      return iter.hasNext() && countLimit > 0;
    }
  }

  @Override
  public Record next() {
    T record = iter.next();
    countLimit--;
    return toRecord(record);
  }


  void setCountLimit(long countLimit) {
    this.countLimit = countLimit;
  }

  void setOffset(long offset) {
    if (offset < this.offset) {
      throw new IllegalArgumentException("The offset to set cannot be less than the current offset");
    } else if (offset == this.offset) {
      return;
    }

    long count = offset - this.offset;
    while (count > 0 && iter.hasNext()) {
      count--;
      iter.next();
    }
    this.offset = offset;
  }

  Column[] getColumns() {
    return columns;
  }

  private void initColumns(List<String> headers, List<TypeInfo> typeInfos) {
    if (headers == null || typeInfos == null) {
      throw new IllegalArgumentException("headers and types should not be null.");
    }
    if (headers.size() != typeInfos.size()) {
      throw new IllegalArgumentException("The length of headers and types should be equal.");
    }
    columns = new Column[headers.size()];
    for (int i = 0; i < headers.size(); i++) {
      columns[i] = new Column(headers.get(i), typeInfos.get(i));
    }
  }

  Record toRecord(T object) {
    return (Record) object;
  }

}
