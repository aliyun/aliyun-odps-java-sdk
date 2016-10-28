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
 * PlRecord
 */
public class PlRecord extends PipelineExampleBase {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.pipe);
  }

  public static class PlRecordMapper extends MapperBase {

    Record rec;

    @Override
    public void setup(TaskContext context) throws IOException {
      if (context.getJobConf().get("record.test.setup", "map").equalsIgnoreCase("map")) {
        rec = context.createOutputRecord();
        rec.set(new Object[]{"map.setup", 11L, true, 0.0001});
        context.write(rec);
      } else {
        rec = context.createMapOutputValueRecord();
      }
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      if (context.getJobConf().get("record.test.setup", "map").equalsIgnoreCase("map")) {
        context.write(record);
      } else {
        rec.setBigint(0, recordNum);
        context.write(rec, record);
      }

      Counter cnt = context.getCounter("MapCounters", "map_input_records");
      cnt.increment(1);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
      rec = context.createOutputRecord();

      if (context.getJobConf().get("record.test.setup", "map").equalsIgnoreCase("map")) {
        rec.set(new Object[]{"map.cleanup", 12L, false, 0.0002});
        context.write(rec);

        rec.set(0, "map.cleanup");
        rec.set(1, 13L);
        rec.set(2, false);
        rec.set(3, 0.0002D);
        context.write(rec);

        rec.setString(0, "map.cleanup");
        rec.setBigint(1, 14L);
        rec.setBoolean(2, true);
        rec.setDouble(3, 0.0003);
        context.write(rec);

        rec.set("_c_str", "map.cleanup");
        rec.set("_c_int", 15L);
        rec.set("_c_bool", true);
        rec.set("_c_d", 0.0004);
        context.write(rec);
      }
    }
  }

  public static class PlRecordReducer extends ReducerBase {

    @Override
    public void setup(TaskContext context) throws IOException {
      Record rec = context.createOutputRecord();
      rec.set(new Object[]{"reduce.setup", 21L, false, 0.0002});
      context.write(rec);

    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      while (values.hasNext()) {
        Record val = values.next();
        context.write(val);

        Counter cnt = context.getCounter("ReduceCounters", "reduce_input_records");
        cnt.increment(1);
      }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
      Record rec = context.createOutputRecord();

      rec.set(new Object[]{"map.cleanup", 22L, false, 0.0002});
      context.write(rec);

      rec.set(0, "map.cleanup");
      rec.set(1, 23L);
      rec.set(2, false);
      rec.set(3, 0.0002D);
      context.write(rec);

      rec.setString(0, "map.cleanup");
      rec.setBigint(1, 24L);
      rec.setBoolean(2, true);
      rec.setDouble(3, 0.0003);
      context.write(rec);

      rec.set("_c_str", "map.cleanup");
      rec.set("_c_int", 25L);
      rec.set("_c_bool", true);
      rec.set("_c_d", 0.0004);
      context.write(rec);
    }
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[3];
    args[0] = "pipe_plrecord_in";
    args[1] = "pipe_plrecord_out";
    args[2] = "map";

    if (args.length != 2 && args.length != 3) {
      System.err.println("Usage: multiinout <in_table> <out_table> [map|reduce]");
      System.exit(2);
    }

    JobConf job = new JobConf();
    Builder builder = Pipeline.builder();

    if (args.length == 3) {
      job.set("record.test.setup", args[2]);
    } else if (args.length == 2) {
      job.set("record.test.setup", "reduce");
    }

    if (job.get("record.test.setup", "map").equalsIgnoreCase("map")) {
      // mapper
      builder.addMapper(PlRecordMapper.class);
      job.setNumReduceTasks(0);
    } else {
      // mapper
      builder
          .addMapper(PlRecordMapper.class)
          .setOutputKeySchema(new Column[]{new Column("_no", OdpsType.BIGINT)})
          .setOutputValueSchema(
              new Column[]{new Column("_c_str", OdpsType.STRING),
                           new Column("_c_int", OdpsType.BIGINT),
                           new Column("_c_bool", OdpsType.BOOLEAN),
                           new Column("_c_d", OdpsType.DOUBLE)});

      // reducer
      builder.addReducer(PlRecordReducer.class);
    }

    Pipeline pipeline = builder.createPipeline();

    addInputTable(job, args[0]);
    addOutputTable(job, args[1]);

    JobClient.runJob(job, pipeline);

  }

}
