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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.bridge.WritableRecord;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;

/**
 * {@link Reducer} 单元测试的上下文.
 * 
 * <p>
 * 编写 {@link Reducer} 单元测试需要用到此上下文。
 * 
 * @see MRUnitTest#runReducer(com.aliyun.odps.mapreduce.JobConf,
 *      ReduceUTContext)
 * 
 * @param <K>
 *          {@link Reducer} 输入的 Key 类型
 * @param <V>
 *          {@link Reducer} 输入的 Value 类型
 */
public class ReduceUTContext extends UTContext {

  private List<KeyValue<Record,Record>> inputKeyVals = new ArrayList<KeyValue<Record,Record>>();
  private String inputKeySchema = "";
  private String inputValueSchema = "";
  // for pipeline, reducer index start with 0
  private int reducerIndex;
  
  private void getInputKeyValueSchema(Configuration conf) {
    BridgeJobConf job = new BridgeJobConf(conf);
    Pipeline pipeline = Pipeline.fromJobConf(job);
    if (pipeline != null) {
      TransformNode node = pipeline.getNode(reducerIndex + 1);
      inputKeySchema = SchemaUtils.toString(node.getInputKeySchema());
      inputValueSchema = SchemaUtils.toString(node.getInputValueSchema());
    } else {
      inputKeySchema = SchemaUtils.toString(job.getMapOutputKeySchema());
      inputValueSchema = SchemaUtils.toString(job.getMapOutputValueSchema());
    }
  }
  
  /**
   * 创建 Reduce 输入的key记录对象.
   * 
   * <p>
   * 记录的 schema 通过 {@link #setInputSchema(String)} 设置。
   * 
   * @return Map 输入的记录对象
   * @throws IOException
   */
  public Record createInputKeyRecord(Configuration conf) throws IOException {
    getInputKeyValueSchema(conf);
    if (inputKeySchema == null) {
      throw new IOException("input key schema is not set.");
    }
    return MRUnitTest.createRecord(inputKeySchema);
  }

  /**
   * 创建 Reduce 输入的value记录对象.
   * 
   * <p>
   * 记录的 schema 通过 {@link #setInputSchema(String)} 设置。
   * 
   * @return Map 输入的记录对象
   * @throws IOException
   */
  public Record createInputValueRecord(Configuration conf) throws IOException {
    getInputKeyValueSchema(conf);
    if (inputValueSchema == null) {
      throw new IOException("input value schema is not set.");
    }
    return MRUnitTest.createRecord(inputValueSchema);
  }

  /**
   * 增加 {@link Reducer} 输入的键值对.
   * 
   * @param k
   *          {@link Reducer} 输入的 Key 对象
   * @param v
   *          {@link Reducer} 输入的 Value 对象
   */
  public void addInputKeyValue(Record k, Record v) {
    inputKeyVals.add(new KeyValue<Record,Record>(k.clone(), v.clone()));
  }

  /**
   * 给定键值对列表，增加 {@link Reducer} 输入的键值对.
   * 
   * @param kvs
   *          {@link Reducer} 输入的键值对列表
   */
  public void addInputKeyValues(List<KeyValue<Record,Record>> kvs) {
    inputKeyVals.addAll(kvs);
  }

  /**
   * 给定 {@link MapOutput}，增加 {@link Reducer} 输入的键值对.
   * 
   * <p>
   * 将 {@link Mapper} 的结果键值对作为 {@link Reducer} 的输入，等价于：<br/>
   * 
   * <pre>
   * addInputKeyValues(mapOutput.getOutputKeyValues());
   * </pre>
   * 
   * @param mapOutput
   */
  public void addInputKeyValues(TaskOutput mapOutput) {
    addInputKeyValues(mapOutput.getOutputKeyValues());
  }

  /**
   * 给定 {@link MapOutput} 和 reduceId，增加 {@link Reducer} 输入的键值对.
   * 
   * <p>
   * 将 {@link Mapper} 写给 reduceId 的结果键值对作为 {@link Reducer} 的输入，等价于：<br/>
   * 
   * <pre>
   * addInputKeyValues(mapOutput.getOutputKeyValues(reduceId));
   * </pre>
   * 
   * @param mapOutput
   * @param reduceId
   */
  public void addInputKeyValues(TaskOutput mapOutput, int reduceId) {
    addInputKeyValues(mapOutput.getOutputKeyValues(reduceId));
  }

  public List<KeyValue<Record,Record>> getInputKeyVals() {
    return inputKeyVals;
  }

  public int getReducerIndex() {
    return reducerIndex;
  }

  public void setReducerIndex(int reducerIndex) {
    this.reducerIndex = reducerIndex;
  }

}
