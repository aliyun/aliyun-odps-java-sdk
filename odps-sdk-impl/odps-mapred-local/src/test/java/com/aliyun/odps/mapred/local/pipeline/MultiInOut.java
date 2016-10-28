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

import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.Builder;

/**
 * Multi input & output example
 */
public class MultiInOut extends PipelineExampleBase {

  public static class TokenizerMapper extends MapperBase {

    Record word = null;
    Record one = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.setBigint(0, 1L);

      Counter cnt = context.getCounter("MyCounters", "map.input.num");
      cnt.setValue(context.getJobConf().getInt("map.input.num", 0));
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      for (int i = 0; i < record.getColumnCount(); i++) {
        if (record.get(i) != null) {
          word.setString(0, record.get(i).toString());
        } else {
          word.set(0, record.get(i));
        }
        context.write(word, one);
      }
    }
  }

  public static class SumReducer extends ReducerBase {

    private Record result = null;
    private Record result1 = null;
    private Record result2 = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      long num = context.getJobConf().getInt("reduce.output.num", 0);

      if (num == 3) {
        result = context.createOutputRecord();
        result1 = context.createOutputRecord("out1");
        result2 = context.createOutputRecord("out2");
      } else if (num == 2) {
        result = context.createOutputRecord();
        result1 = context.createOutputRecord("out1");
      } else if (num == 1) {
        result = context.createOutputRecord();
      }

      Counter cnt = context.getCounter("MyCounters", "reduce.output.num");
      cnt.setValue(context.getJobConf().getInt("reduce.output.num", 0));
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      long count = 0;
      long num = context.getJobConf().getInt("reduce.output.num", 0);

      while (values.hasNext()) {
        Record val = values.next();
        count += (Long) val.get(0);
      }

      long mod = count % num;
      if (mod == 0) {
        result.set(0, key.get(0));
        result.set(1, count);
        context.write(result);
      } else if (mod == 1) {
        result1.set(0, key.get(0));
        result1.set(1, count);
        context.write(result1, "out1");
      } else {
        result2.set(0, key.get(0));
        result2.set(1, count);
        context.write(result2, "out2");
      }
    }
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[2];
    args[0] = "pipe_multiinout_in1,pipe_multiinout_in2";
    args[1] = "pipe_multiinout_out1,pipe_multiinout_out2";

    JobConf job = new JobConf();
    Builder builder = Pipeline.builder();

    // mapper
    builder.addMapper(TokenizerMapper.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[]{"word"})
        .setPartitionColumns(new String[]{"word"})
        .setOutputGroupingColumns(new String[]{"word"});

    // reducer
    builder.addReducer(SumReducer.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)});

    Pipeline pipeline = builder.createPipeline();

    String[] inputs = args[0].split(",");
    for (String in : inputs) {
      addInputTable(job, in);
    }

    OutputUtils.addTable(TableInfo.builder().tableName("pipe_multiinout_out1").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("pipe_multiinout_out2").label("out1")
                             .build(), job);

    job.setInt("map.input.num", inputs.length);
    job.setInt("reduce.output.num", 2);

    JobClient.runJob(job, pipeline);

  }

}
