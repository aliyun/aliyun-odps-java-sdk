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

import java.util.Comparator;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;

/**
 * {@link com.aliyun.odps.data.Record}内容的自然序比较器
 */
public class NaturalRecordComparator implements Comparator<Object[]> {

  @SuppressWarnings("rawtypes")
  protected Comparator[] comparators;

  public NaturalRecordComparator(Column[] schema) {
    super();
    comparators = new Comparator[schema.length];
    for (int i = 0; i < schema.length; i++) {
      OdpsType type = schema[i].getType();
      comparators[i] = ComparatorFactory.getComparator(type);
    }
  }

  @Override
  public int compare(Object[] l, Object[] r) {
    int result = 0;
    for (int i = 0; i < l.length; i++) {
      if (r.length < i) {
        return -1;
      }
      result = compare(l[i], r[i], comparators[i]);
      if (result != 0) {
        return result;
      }
    }
    if (r.length > l.length) {
      return 1;
    }
    return 0;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected int compare(Object l, Object r, Comparator comparator) {
    return comparator.compare(l, r);
  }

}
