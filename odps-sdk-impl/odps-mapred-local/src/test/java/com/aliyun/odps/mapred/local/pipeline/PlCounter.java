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

package com.aliyun.odps.mapred.local.pipeline;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.Builder;

/**
 * Counter
 *
 * @author yubin.byb
 */
public class PlCounter extends PipelineExampleBase {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.pipe);
  }

  enum MyCounter {
    TOTAL_TASKS, MAP_TASKS, REDUCE_TASKS
  }

  public static class ResourceMapper extends MapperBase {

    @Override
    public void setup(TaskContext context) throws IOException {
      Counter counter = null;
      counter = context.getCounter("MapCounters", "map_setup_counter1");
      counter.setValue(Long.MAX_VALUE);
      counter = context.getCounter("MapCounters", "map_setup_counter2");
      counter.setValue(Long.MIN_VALUE);
    }

    @Override
    public void map(long key, Record record, TaskContext context)
        throws IOException {

      Counter cnt = context.getCounter("MapCounters", "map_input_records");
      cnt.increment(1);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
      Counter map_tasks = context.getCounter(MyCounter.MAP_TASKS);
      Counter total_tasks = context.getCounter(MyCounter.TOTAL_TASKS);
      map_tasks.increment(1);
      total_tasks.increment(1);
    }
  }

  public static class ResourceReducer extends ReducerBase {

    @Override
    public void setup(TaskContext context) throws IOException {
      Counter counter = null;
      counter = context.getCounter("ReduceCounters", "reduce_setup_counter1");
      counter.setValue(Long.MAX_VALUE);
      counter = context.getCounter("ReduceCounters", "reduce_setup_counter2");
      counter.setValue(Long.MIN_VALUE);
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      while (values.hasNext()) {
        Counter cnt = context.getCounter("ReduceCounters",
                                         "reduce_input_records");
        cnt.increment(1);
      }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
      Counter reduce_tasks = context.getCounter(MyCounter.REDUCE_TASKS);
      Counter total_tasks = context.getCounter(MyCounter.TOTAL_TASKS);
      reduce_tasks.increment(1);
      total_tasks.increment(1);

      Record record = context.createOutputRecord();
      record.set(new Object[]{"map.records",
                              context.getCounter("MapCounters", "map_input_records").getValue()});
      context.write(record);
      record.set(new Object[]{
          "reduce.records",
          context.getCounter("ReduceCounters", "reduce_input_records")
              .getValue()});
      context.write(record);
    }
  }

  public static void verify(Counters counters) throws IOException {
    long m = counters.findCounter(MyCounter.MAP_TASKS).getValue();
    long r = counters.findCounter(MyCounter.REDUCE_TASKS).getValue();
    long total = counters.findCounter(MyCounter.TOTAL_TASKS).getValue();

    if (m <= 0 || r <= 0 || m + r != total) {
      throw new IOException("Counter error: m: " + m + ", r: " + r
                            + ", total: " + total);
    }

    long val = counters.findCounter("MapCounters", "map_setup_counter1")
        .getValue();
    if (val != Long.MAX_VALUE) {
      throw new IOException("Counter MapCounters,map_setup_counter1 " + val
                            + " should " + Long.MAX_VALUE);
    }

    val = counters.findCounter("MapCounters", "map_setup_counter2").getValue();
    if (val != Long.MIN_VALUE) {
      throw new IOException("Counter MapCounters,map_setup_counter1 " + val
                            + " should " + Long.MAX_VALUE);
    }

    val = counters.findCounter("ReduceCounters", "reduce_setup_counter1")
        .getValue();
    if (val != Long.MAX_VALUE) {
      throw new IOException("Counter ReduceCounters,reduce_setup_counter1 "
                            + val + " should " + Long.MAX_VALUE);
    }

    val = counters.findCounter("ReduceCounters", "reduce_setup_counter2")
        .getValue();
    if (val != Long.MIN_VALUE) {
      throw new IOException("Counter ReduceCounters,reduce_setup_counter2 "
                            + val + " should " + Long.MAX_VALUE);
    }
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[2];
    args[0] = "pipe_plcounter_in";
    args[1] = "pipe_plcounter_out";

    if (args.length != 2 && args.length != 3) {
      System.err
          .println(
              "Usage: Identity <[project.]in_table> <[project.]out_table> <[path/]resource_file> <resource_file_lines> <resource_table> <resource_table_records>");
      System.exit(2);
    }

    JobConf job = new JobConf();
    Builder builder = Pipeline.builder();

    // mapper
    builder
        .addMapper(ResourceMapper.class)
        .setOutputKeySchema(new Column[]{new Column("key", OdpsType.STRING)})
        .setOutputValueSchema(
            new Column[]{new Column("value", OdpsType.STRING)});

    // reducer
    builder
        .addReducer(ResourceReducer.class)
        .setOutputKeySchema(new Column[]{new Column("key", OdpsType.STRING)})
        .setOutputValueSchema(
            new Column[]{new Column("value", OdpsType.STRING)});

    Pipeline pipeline = builder.createPipeline();

    addInputTable(job, args[0]);
    addOutputTable(job, args[1]);

    RunningJob rJob = JobClient.runJob(job, pipeline);
    verify(rJob.getCounters());
  }
}
