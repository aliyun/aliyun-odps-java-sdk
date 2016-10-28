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

package com.aliyun.odps.mapred.open.example;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * MultiJobs
 * 
 * Running multiple job
 * 
 **/
public class MultiJobs {

  public static class InitMapper extends MapperBase {

    @Override
    public void setup(TaskContext context) throws IOException {
      Record record = context.createOutputRecord();
      long v = context.getJobConf().getLong("multijobs.value", 2);
      record.set(0, v);
      context.write(record);
    }
  }

  public static class DecreaseMapper extends MapperBase {

    @Override
    public void cleanup(TaskContext context) throws IOException {
      // 从JobConf中获取main函数中定义的变量值
      long expect = context.getJobConf().getLong("multijobs.expect.value", -1);
      long v = -1;
      int count = 0;

      Iterator<Record> iter = context.readResourceTable("multijobs_res_table");
      while (iter.hasNext()) {
        Record r = iter.next();
        v = (Long) r.get(0);
        if (expect != v) {
          throw new IOException("expect: " + expect + ", but: " + v);
        }
        count++;
      }

      if (count != 1) {
        throw new IOException("res_table should have 1 record, but: " + count);
      }

      Record record = context.createOutputRecord();
      v--;
      record.set(0, v);
      context.write(record);
      context.getCounter("multijobs", "value").setValue(v);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: TestMultiJobs <table>");
      System.exit(1);
    }
    String tbl = args[0];
    long iterCount = 2;

    System.err.println("Start to run init job.");

    JobConf initJob = new JobConf();

    initJob.setLong("multijobs.value", iterCount);
    initJob.setMapperClass(InitMapper.class);

    InputUtils.addTable(TableInfo.builder().tableName("mr_empty").build(), initJob);
    OutputUtils.addTable(TableInfo.builder().tableName(tbl).build(), initJob);

    initJob.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
    initJob.setMapOutputValueSchema(SchemaUtils.fromString("value:string"));

    initJob.setNumReduceTasks(0);

    JobClient.runJob(initJob);

    while (true) {
      System.err.println("Start to run iter job, count: " + iterCount);

      JobConf decJob = new JobConf();

      decJob.setLong("multijobs.expect.value", iterCount);
      decJob.setMapperClass(DecreaseMapper.class);

      InputUtils.addTable(TableInfo.builder().tableName("mr_empty").build(), decJob);
      OutputUtils.addTable(TableInfo.builder().tableName(tbl).build(), decJob);

      decJob.setNumReduceTasks(0);

      RunningJob rJob = JobClient.runJob(decJob);

      iterCount--;

      if (rJob.getCounters().findCounter("multijobs", "value").getValue() == 0) {
        break;
      }
    }

    if (iterCount != 0) {
      throw new IOException("Job failed.");
    }
  }
}
