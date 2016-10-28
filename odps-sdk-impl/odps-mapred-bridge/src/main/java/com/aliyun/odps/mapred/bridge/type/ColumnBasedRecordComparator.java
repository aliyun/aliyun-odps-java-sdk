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

package com.aliyun.odps.mapred.bridge.type;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Column;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.JobConf.SortOrder;

/**
 * 可以指定排序列的RecordComparator。可以指定列排序方式ASC或者DESC，默认排序策略是自然序（ASC）。
 */
public class ColumnBasedRecordComparator extends NaturalRecordComparator {

  int[] selIdxs;
  SortOrder[] sortOrder;

  /**
   * ColumnBasedRecordComparator的构造方法，指定排序列的下标索引。例如指定排序列的下标索引为0，3，则比较时先用第0列比较，
   * 第0列相同时使用第3列比较。
   *
   * @param selIdxs
   *     排序列的下标索引
   * @param schema
   *     整个Record的行属性
   */
  public ColumnBasedRecordComparator(int[] selIdxs, Column[] schema) {
    super(schema);
    this.selIdxs = selIdxs;
    this.sortOrder = null;
  }

  /**
   * ColumnBasedRecordComparator的构造方法，指定排序列的列名。例如指定排序列的下标索引为“word”，“count”，则比较时先用word列比较，
   * word列相同时使用count列比较。
   *
   * @param selCols
   * @param schema
   */
  public ColumnBasedRecordComparator(String[] selCols, Column[] schema) {
    this(selCols, schema, null);
  }

  public ColumnBasedRecordComparator(String[] selCols, Column[] schema, SortOrder[] sortOrder) {
    super(schema);
    this.sortOrder = sortOrder;
    this.selIdxs = new int[selCols.length];
    Map<String, Integer> reverseLookupMap = new HashMap<String, Integer>();
    int i = 0;
    for (Column c : schema) {
      reverseLookupMap.put(c.getName(), i);
      i++;
    }
    i = 0;
    for (String col : selCols) {
      selIdxs[i] = reverseLookupMap.get(col);
      i++;
    }
  }

  @Override
  public int compare(Object[] l, Object[] r) {
    int result = 0;
    for (int i : selIdxs) {
      if (r.length < i) {
        return -1;
      }
      if (l.length < i) {
        return 1;
      }
      if (sortOrder != null && sortOrder[i] == JobConf.SortOrder.DESC) {
        result = compare(r[i], l[i], comparators[i]);
      } else {
        result = compare(l[i], r[i], comparators[i]);
      }
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

}
