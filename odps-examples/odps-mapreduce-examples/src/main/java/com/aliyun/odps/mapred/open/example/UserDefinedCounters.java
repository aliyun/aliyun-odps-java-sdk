/**
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

package com.aliyun.odps.mapred.open.example;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.data.TableInfo;

/**
 * 
 * User Defined Counters
 * 
 **/
public class UserDefinedCounters {

  enum MyCounter {
    TOTAL_TASKS, MAP_TASKS, REDUCE_TASKS
  }

  public static class TokenizerMapper extends MapperBase {
    private Record word;
    private Record one;

    @Override
    public void setup(TaskContext context) throws IOException {
      super.setup(context);
      Counter map_tasks = context.getCounter(MyCounter.MAP_TASKS);
      Counter total_tasks = context.getCounter(MyCounter.TOTAL_TASKS);
      map_tasks.increment(1);
      total_tasks.increment(1);

      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.set(new Object[] {1L});
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      for (int i = 0; i < record.getColumnCount(); i++) {
        word.set(new Object[] {record.get(i).toString()});
        context.write(word, one);
      }
    }
  }

  public static class SumReducer extends ReducerBase {
    private Record result = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
      Counter reduce_tasks = context.getCounter(MyCounter.REDUCE_TASKS);
      Counter total_tasks = context.getCounter(MyCounter.TOTAL_TASKS);
      reduce_tasks.increment(1);
      total_tasks.increment(1);
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
      long count = 0;
      while (values.hasNext()) {
        Record val = values.next();
        count += (Long) val.get(0);
      }
      result.set(0, key.get(0));
      result.set(1, count);
      context.write(result);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: TestUserDefinedCounters <in_table> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    RunningJob rJob = JobClient.runJob(job);

    // 在作业成功结束后，可以获取到job里面的自定义counter的值
    Counters counters = rJob.getCounters();
    long m = counters.findCounter(MyCounter.MAP_TASKS).getValue();
    long r = counters.findCounter(MyCounter.REDUCE_TASKS).getValue();
    long total = counters.findCounter(MyCounter.TOTAL_TASKS).getValue();

    System.exit(0);
  }
}
