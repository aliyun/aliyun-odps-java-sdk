/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps.mapred.unittest;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.pipeline.PipeLine;
import com.aliyun.odps.mapred.unittest.*;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.Builder;

/**
 * This is an example ODPS Map/Reduce application. It reads the input table, map each column into
 * words and counts them. The output is a locally sorted list of words and the count of how often
 * they occurred.
 * <p>
 * To run: mapreduce -libjars mapreduce-examples.jar com.aliyun.odps.mapreduce.examples.WordCount
 * <i>in-tbl</i> <i>out-tbl</i>
 */
public class WordCountPipelineTest extends MRUnitTest {
  private final static String INPUT_SCHEMA = "a:string,b:string";
  private final static String OUTPUT_SCHEMA = "k:string,v:bigint";
  JobConf job = new JobConf();
  
  public WordCountPipelineTest() throws Exception {

    Builder builder = Pipeline.builder();

    // mapper
    builder.addMapper(PipeLine.TokenizerMapper.class)
        .setOutputKeySchema(new Column[] {new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[] {new Column("count", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[] {"word"})
        .setOutputGroupingColumns(new String[] {"word"});

    job.setCombinerClass(PipeLine.SumCombiner.class);

    // reducer1
    builder.addReducer(PipeLine.SumIReducer.class)
        .setOutputKeySchema(new Column[] {new Column("word", OdpsType.STRING),new Column("cnt", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[] {new Column("count", OdpsType.BIGINT)});

    // reducer2
    builder.addReducer(PipeLine.IdentityReducer.class);

    Pipeline pipeline = builder.createPipeline();

    PipeLine.addInputTable(job, "wc_in");
    PipeLine.addOutputTable(job, "wc_out");
    Pipeline.toJobConf(job, pipeline);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void TestMap() throws IOException, ClassNotFoundException, InterruptedException {

    // prepare test data
    MapUTContext context = new MapUTContext();
    context.setInputSchema(INPUT_SCHEMA);
    context.setOutputSchema(OUTPUT_SCHEMA, job);

    Record record = context.createInputRecord();
    record.set(new Text[] {new Text("hello"), new Text("c")});
    context.addInputRecord(record);

    record = context.createInputRecord();
    record.set(new Text[] {new Text("hello"), new Text("java")});
    context.addInputRecord(record);

    // run mapper
    TaskOutput output = runMapper(job, context);

    // verify mapper outputs
    List<KeyValue<Record, Record>> kvs = output.getOutputKeyValues();
    Assert.assertEquals(3, kvs.size());
    Assert.assertEquals(new KeyValue<String, Long>(new String("c"), new Long(1)),
        new KeyValue<String, Long>((String) (kvs.get(0).getKey().get(0)), (Long) (kvs.get(0)
            .getValue().get(0))));
    Assert.assertEquals(new KeyValue<String, Long>(new String("hello"), new Long(2)),
        new KeyValue<String, Long>((String) (kvs.get(1).getKey().get(0)), (Long) (kvs.get(1)
            .getValue().get(0))));
    Assert.assertEquals(new KeyValue<String, Long>(new String("java"), new Long(1)),
        new KeyValue<String, Long>((String) (kvs.get(2).getKey().get(0)), (Long) (kvs.get(2)
            .getValue().get(0))));
  }

  @Test
  public void TestReduce0() throws IOException, ClassNotFoundException, InterruptedException {

    ReduceUTContext context = new ReduceUTContext();
    context.setReducerIndex(0);
    context.setOutputSchema(OUTPUT_SCHEMA, job);

    Record key = context.createInputKeyRecord(job);
    Record value = context.createInputValueRecord(job);
    key.set(0, "world");
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);
    key.set(0, "hello");
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);
    key.set(0, "hello");
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);
    key.set(0, "odps");
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);
    TaskOutput output = runReducer(job, context);

    List<KeyValue<Record, Record>> kvs = output.getOutputKeyValues();
    Assert.assertEquals(3, kvs.size());
    Assert.assertEquals(new KeyValue<String, Long>(new String("hello"), new Long(2)),
        new KeyValue<String, Long>((String) (kvs.get(0).getKey().get(0)), (Long) (kvs.get(0)
            .getValue().get(0))));
    Assert.assertEquals(new KeyValue<String, Long>(new String("odps"), new Long(1)),
        new KeyValue<String, Long>((String) (kvs.get(1).getKey().get(0)), (Long) (kvs.get(1)
            .getValue().get(0))));
    Assert.assertEquals(new KeyValue<String, Long>(new String("world"), new Long(1)),
        new KeyValue<String, Long>((String) (kvs.get(2).getKey().get(0)), (Long) (kvs.get(2)
            .getValue().get(0))));
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void TestReduce1() throws IOException, ClassNotFoundException, InterruptedException {

    ReduceUTContext context = new ReduceUTContext();
    context.setReducerIndex(1);
    context.setOutputSchema(OUTPUT_SCHEMA, job);

    Record key = context.createInputKeyRecord(job);
    Record value = context.createInputValueRecord(job);
    key.set(0, "world");
    key.set(1, new Long(1));
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);
    key.set(0, "hello");
    key.set(1, new Long(2));
    value.set(0, new Long(2));
    context.addInputKeyValue(key, value);
    key.set(0, "odps");
    key.set(1, new Long(1));
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);
    TaskOutput output = runReducer(job, context);

    List<Record> records = output.getOutputRecords(true);
    Assert.assertEquals(3, records.size());
    Assert.assertEquals(new String("hello"), records.get(0).get(0));
    Assert.assertEquals(new Long(2), records.get(0).get(1));
    Assert.assertEquals(new String("odps"), records.get(1).get(0));
    Assert.assertEquals(new Long(1), records.get(1).get(1));
    Assert.assertEquals(new String("world"), records.get(2).get(0));
    Assert.assertEquals(new Long(1), records.get(2).get(1));
  }
}
