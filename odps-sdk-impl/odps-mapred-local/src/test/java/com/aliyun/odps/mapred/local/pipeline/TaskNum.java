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
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.Builder;

/**
 * TaskNum Test Map & Rudece num
 */
public class TaskNum extends PipelineExampleBase {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.pipe);
  }

  public static class TaskNumMapper extends MapperBase {

    Record val;

    @Override
    public void setup(TaskContext context) throws IOException {
      val = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      context.write(record, val);

      Counter cnt = context.getCounter("MapCounters", "map_input_records");
      cnt.increment(1);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
    }
  }

  /**
   * IdentityReducer
   */
  public static class TaskNumReducer extends ReducerBase {

    @Override
    public void setup(TaskContext context) throws IOException {

    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      while (values.hasNext()) {
        Record val = values.next();
        context.write(key);

        Counter cnt = context.getCounter("ReduceCounters", "reduce_input_records");
        cnt.increment(1);
      }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
      Record rec = context.createOutputRecord();
      rec.setString(0, "reduce_tag");
      context.write(rec);
    }
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[2];
    args[0] = "pipe_tasknum_in";
    args[1] = "pipe_tasknum_out";
    if (args.length != 2 && args.length != 3) {
      System.err.println("Usage: multiinout <in_table> <out_table> <redece_num>");
      System.exit(2);
    }

    JobConf job = new JobConf();
    Builder builder = Pipeline.builder();

    if (args.length == 3) {
      int reduceNum = Integer.parseInt(args[2]);
      job.setNumReduceTasks(reduceNum);
    }

    // mapper
    builder.addMapper(TaskNumMapper.class)
        .setOutputKeySchema(new Column[]{new Column("key", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("value", OdpsType.STRING)});

    // reducer
    builder.addReducer(TaskNumReducer.class);

    Pipeline pipeline = builder.createPipeline();

    addInputTable(job, args[0]);
    addOutputTable(job, args[1]);

    JobClient.runJob(job, pipeline);

  }

}
