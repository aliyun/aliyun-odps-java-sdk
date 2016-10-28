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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.lang.ArrayUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.bridge.WritableRecord;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.JobConf.SortOrder;
import com.aliyun.odps.mapred.local.utils.LocalColumnBasedRecordComparator;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;

public class MapOutputBuffer {

  int[] partColIdxs;
  int numReduce;
  private List<PriorityQueue<Object[]>> buffers;
  Comparator<Object[]> comparator;

  public MapOutputBuffer(JobConf conf, int reduceNum) {

    Column[] key = conf.getMapOutputKeySchema();
    if (key != null) {
      String[] partCols = conf.getPartitionColumns();
      this.partColIdxs = new int[partCols.length];
      Map<String, Integer> reverseLookupMap = new HashMap<String, Integer>();
      int i = 0;
      for (Column c : key) {
        reverseLookupMap.put(c.getName(), i);
        i++;
      }
      i = 0;
      for (String col : partCols) {
        partColIdxs[i] = reverseLookupMap.get(col);
      }

      numReduce = reduceNum;
      String[] sortColumns = conf.getOutputKeySortColumns();
      SortOrder[] sortOrders = conf.getOutputKeySortOrder();
      comparator = new LocalColumnBasedRecordComparator(sortColumns, key, sortOrders);
      buffers = new ArrayList<PriorityQueue<Object[]>>(numReduce);
      for (i = 0; i < numReduce; i++) {
        buffers.add(new PriorityQueue<Object[]>(16, comparator));
      }
    }

  }

  public MapOutputBuffer(JobConf conf, Pipeline pipeline, String taskId, int reduceNum) {
    int pipeIndex = Integer.parseInt(taskId.split("_")[0].substring(1)) - 1;
    TransformNode pipeNode = pipeline.getNode(pipeIndex);
    Column[] key = pipeNode.getOutputKeySchema();
    if (key != null) {
      String[] partCols = pipeNode.getPartitionColumns();
      this.partColIdxs = new int[partCols.length];
      Map<String, Integer> reverseLookupMap = new HashMap<String, Integer>();
      int i = 0;
      for (Column c : key) {
        reverseLookupMap.put(c.getName(), i);
        i++;
      }
      i = 0;
      for (String col : partCols) {
        partColIdxs[i] = reverseLookupMap.get(col);
      }

      // reduce copy count
      numReduce = reduceNum;
      String[] sortColumns = pipeNode.getOutputKeySortColumns();
      SortOrder[] sortOrders = pipeNode.getOutputKeySortOrder();
      comparator = new LocalColumnBasedRecordComparator(sortColumns, key, sortOrders);
      buffers = new ArrayList<PriorityQueue<Object[]>>(numReduce);
      for (i = 0; i < numReduce; i++) {
        buffers.add(new PriorityQueue<Object[]>(16, comparator));
      }
    }

  }


  /**
   * Hard coded partition strategy. Should be the same as FUXI hash function.
   *
   * @return
   */
  protected int getPartition(Record key) {
    int partition = 0;
    for (int i : partColIdxs) {
      Object o = key.get(i);
      if (o != null) {
        partition = partition * 32 + o.hashCode();
      } else {
        partition = partition * 32;
      }
    }
    return Math.abs(partition) % numReduce;
  }

  public void add(Record key, Record value) {
    int partition = getPartition(key);
    buffers.get(partition).offer(
        ArrayUtils.addAll(((WritableRecord) key).toWritableArray().clone(),
                          ((WritableRecord) value).toWritableArray().clone()));
  }

  public void add(Record key, Record value, int partition) {
    buffers.get(partition).offer(
        ArrayUtils.addAll(((WritableRecord) key).toWritableArray().clone(),
                          ((WritableRecord) value).toWritableArray().clone()));
  }

  public Queue<Object[]> getPartitionQueue(int partition) {
    return buffers.get(partition);
  }

  public Comparator<? super Object[]> getComparator() {
    return comparator;
  }

  public long getTotalRecordCount() {
    if (buffers == null) {
      return 0;
    }
    long totalCount = 0;
    for (PriorityQueue<Object[]> item : buffers) {
      if (item == null) {
        continue;
      }
      totalCount += item.size();
    }
    return totalCount;
  }

  public void add(Record record, String label) {
    // do nothing
  }
}
