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

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.data.TableInfo;

public class MapOnly {

  public static class MapperClass extends MapperBase {
    @Override
    public void setup(TaskContext context) throws IOException {
      boolean is = context.getJobConf().getBoolean("option.mapper.setup", false);

      if (is) {
        Record result = context.createOutputRecord();
        result.set(0, "setup");
        result.set(1, 1L);
        context.write(result);
      }
    }

    @Override
    public void map(long key, Record record, TaskContext context) throws IOException {
      boolean is = context.getJobConf().getBoolean("option.mapper.map", false);

      if (is) {
        Record result = context.createOutputRecord();
        result.set(0, record.get(0));
        result.set(1, 1L);
        context.write(result);
      }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
      boolean is = context.getJobConf().getBoolean("option.mapper.cleanup", false);

      if (is) {
        Record result = context.createOutputRecord();
        result.set(0, "cleanup");
        result.set(1, 1L);
        context.write(result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2 && args.length != 3) {
      System.err.println("Usage: OnlyMapper <in_table> <out_table> [setup|map|cleanup]");
      System.exit(2);
    }

    JobConf job = new JobConf();
    job.setMapperClass(MapperClass.class);
    job.setNumReduceTasks(0);

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    if (args.length == 3) {
      String options = new String(args[2]);

      if (options.contains("setup")) {
        job.setBoolean("option.mapper.setup", true);
      }

      if (options.contains("map")) {
        job.setBoolean("option.mapper.map", true);
      }

      if (options.contains("cleanup")) {
        job.setBoolean("option.mapper.cleanup", true);
      }
    }

    JobClient.runJob(job);
  }
}
