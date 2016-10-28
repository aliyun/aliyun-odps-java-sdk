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

package com.aliyun.odps.mapred.unittest;

import java.util.Comparator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.mapred.bridge.WritableRecord;

/**
 * RecordComparator 用于对 {@link Record} 进行比较.
 * 
 * <p>
 * {@link #compare(Record, Record)} 比较规则：
 * <ol>
 * <li>比较列数： (o1.size() == o2.size()) ? 0 : ((o1.size() < o2.size()) ? -1 : 1)
 * <li>如果列数相等，调用每列的 {@link WritableComparable#compareTo(Object)} 依次进行比较
 * </ol>
 * 
 * 
 */
public class LocalRecordComparator implements Comparator<Record> {

  /**
   * 比较两个 {@link Record}.
   * 
   * <p>
   * 比较规则：
   * <ol>
   * <li>比较列数： (o1.size() == o2.size()) ? 0 : ((o1.size() < o2.size()) ? -1 : 1)
   * <li>如果列数相等，调用每列的 {@link WritableComparable#compareTo(Object)} 依次进行比较
   * </ol>
   * 
   * @param o1
   *          记录1
   * @param o2
   *          记录2
   * @return 比较结果
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public int compare(Record o1, Record o2) {
    WritableRecord wo1 = (WritableRecord)o1;
    WritableRecord wo2 = (WritableRecord)o2;
    int comp = (wo1.getColumnCount() == wo2.getColumnCount()) ? 0
        : ((wo1.getColumnCount() < wo2.getColumnCount()) ? -1 : 1);

    for (int i = 0; comp == 0 && i < wo1.getColumnCount(); i++) {
      boolean b1 = wo1.get(i) == null;
      boolean b2 = wo2.get(i) == null;
      if (!b1 && !b2) {
        comp = ((WritableComparable) wo1.toWritableArray()[i])
            .compareTo((WritableComparable) wo2.toWritableArray()[i]);
      } else {
        comp = (b1 == b2) ? 0 : (b1 ? 1 : -1);
      }
    }

    return comp;
  }

}

