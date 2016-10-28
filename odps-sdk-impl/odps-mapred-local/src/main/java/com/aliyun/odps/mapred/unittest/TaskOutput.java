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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.MapOutputBuffer;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.counter.Counters;

/**
 * Mapper/Reducer的输出.
 */
public class TaskOutput extends MapOutputBuffer {

  public TaskOutput(JobConf conf, int reduceNum) {
    super(conf, reduceNum);
  }

  public TaskOutput(JobConf job, Pipeline pipeline, String taskId, int reduceCopyNum) {
    super(job, pipeline, taskId, reduceCopyNum);
  }

  private Counters counters;

  private Map<Integer, List<KeyValue<Record, Record>>> outputKeyValues =
      new LinkedHashMap<Integer, List<KeyValue<Record, Record>>>();
  private Map<String, List<Record>> outputs = new HashMap<String, List<Record>>();


  /**
   * 返回 {@link Mapper} 最终输出到 {@link Reducer} 的键值对列表.
   * 
   * @return {@link Mapper} 输出到 {@link Reducer} 的键值对列表
   */
  public List<KeyValue<Record, Record>> getOutputKeyValues() {
    List<KeyValue<Record, Record>> list = new ArrayList<KeyValue<Record, Record>>();
    for (Map.Entry<Integer, List<KeyValue<Record, Record>>> entry : outputKeyValues
        .entrySet()) {
      list.addAll(entry.getValue());
    }
    return list;
  }

  /**
   * 返回 {@link Mapper} 输出到给定 reduceId 的键值对列表.
   * 
   * @param reduceId
   *          {@link Reducer} 序号，从 0 开始计数。
   * @return 输出到给定 reduceId 的键值对列表
   */
  public List<KeyValue<Record, Record>> getOutputKeyValues(int reduceId) {
    return outputKeyValues.get(reduceId);
  }

  void setOutputKeyValues(int reduceId, List<KeyValue<Record, Record>> outputKeyValues) {
    this.outputKeyValues.put(reduceId, outputKeyValues);
  }

  /**
   * 获取默认输出结果，返回 List<{@link Record}>.
   * 
   * @return 默认输出结果
   */
  public List<Record> getOutputRecords() {
    return getOutputRecords("__default__");
  }

  /**
   * 获取给定标签的输出结果，返回 List<{@link Record}>.
   * 
   * @param label
   *          输出标签
   * @return 给定标签的输出结果
   */
  public List<Record> getOutputRecords(String label) {
    return getOutputRecords(label, true);
  }

  /**
   * 获取默认输出结果，返回 List<{@link Record}>.
   * 
   * @return 默认输出结果
   */
  public List<Record> getOutputRecords(boolean sort) {
    return getOutputRecords("__default__", sort);
  }

  /**
   * 获取给定标签的输出结果，返回 List<{@link Record}>.
   * 
   * @param label
   *          输出标签
   * @return 给定标签的输出结果
   */
  public List<Record> getOutputRecords(String label, boolean sort) {
    List<Record> records = outputs.get(label);
    if (records == null) {
      records = new ArrayList<Record>();
    }
    List<Record> sortRecords = new ArrayList<Record>(records);
    LocalRecordComparator comparator = new LocalRecordComparator();
    if (sort) {
      Collections.sort(sortRecords, comparator);
    }
    return sortRecords;
  }

  /**
   * 获取作业 {@link Counters}.
   * 
   * @return 作业 {@link Counters}.
   */
  public Counters getCounters() {
    return counters;
  }

  void setCounters(Counters counters) {
    this.counters = counters;
  }

  void setOutputRecords(String label, List<Record> records) {
    outputs.put(label, records);
  }

  @Override
  public void add(Record key, Record value) {
    int partition = getPartition(key);
    this.add(key, value, partition);
  }

  @Override
  public void add(Record key, Record value, int partition) {
    super.add(key, value, partition);
    KeyValue<Record, Record> kv = new KeyValue<Record, Record>(key.clone(), value.clone());

    List<KeyValue<Record, Record>> kvs = this.getOutputKeyValues(partition);
    if (kvs == null) {
      kvs = new ArrayList<KeyValue<Record, Record>>();
      outputKeyValues.put(partition, kvs);
    }
    kvs.add(kv);
  }

  @Override
  public void add(Record record, String label) {
    List<Record> records = outputs.get(label);
    if (records == null) {
      records = new ArrayList<Record>();
      outputs.put(label, records);
    }
    records.add(record.clone());
  }
  
  @Override
  public long getTotalRecordCount() {
    long numOutputRecords = 0;
    for (List<Record> records : outputs.values()) {
      numOutputRecords += records.size();
    }
    return numOutputRecords + super.getTotalRecordCount();
  }

}
