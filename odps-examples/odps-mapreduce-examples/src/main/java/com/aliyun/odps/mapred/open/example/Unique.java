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
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * Unique Remove duplicate words
 * 
 **/
public class Unique {

  public static class OutputSchemaMapper extends MapperBase {
    private Record key;
    private Record value;

    @Override
    public void setup(TaskContext context) throws IOException {
      key = context.createMapOutputKeyRecord();
      value = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      long left = 0;
      long right = 0;

      if (record.getColumnCount() > 0) {
        left = (Long) record.get(0);
        if (record.getColumnCount() > 1) {
          right = (Long) record.get(1);
        }

        key.set(new Object[] {(Long) left, (Long) right});
        value.set(new Object[] {(Long) left, (Long) right});

        context.write(key, value);
      }
    }
  }

  public static class OutputSchemaReducer extends ReducerBase {
    private Record result = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
      result.set(0, key.get(0));
      while (values.hasNext()) {
        Record value = values.next();
        result.set(1, value.get(1));
      }
      context.write(result);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length > 3 || args.length < 2) {
      System.err.println("Usage: unique <in> <out> [key|value|all]");
      System.exit(2);
    }

    String ops = "all";
    if (args.length == 3) {
      ops = args[2];
    }

    // reduce的输入分组是由setOutputGroupingColumns的设置来决定的，这个参数如果不设置
    // 默认就是MapOutputKeySchema
    // Key Unique
    if (ops.equals("key")) {
      JobConf job = new JobConf();

      job.setMapperClass(OutputSchemaMapper.class);
      job.setReducerClass(OutputSchemaReducer.class);

      job.setMapOutputKeySchema(SchemaUtils.fromString("key:bigint,value:bigint"));
      job.setMapOutputValueSchema(SchemaUtils.fromString("key:bigint,value:bigint"));

      job.setPartitionColumns(new String[] {"key"});
      job.setOutputKeySortColumns(new String[] {"key", "value"});
      job.setOutputGroupingColumns(new String[] {"key"});

      job.set("tablename2", args[1]);

      job.setNumReduceTasks(1);
      job.setInt("table.counter", 0);

      InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
      OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

      JobClient.runJob(job);
    }

    // Key&Value Unique
    if (ops.equals("all")) {
      JobConf job = new JobConf();

      job.setMapperClass(OutputSchemaMapper.class);
      job.setReducerClass(OutputSchemaReducer.class);

      job.setMapOutputKeySchema(SchemaUtils.fromString("key:bigint,value:bigint"));
      job.setMapOutputValueSchema(SchemaUtils.fromString("key:bigint,value:bigint"));

      job.setPartitionColumns(new String[] {"key"});
      job.setOutputKeySortColumns(new String[] {"key", "value"});
      job.setOutputGroupingColumns(new String[] {"key", "value"});

      job.set("tablename2", args[1]);

      job.setNumReduceTasks(1);
      job.setInt("table.counter", 0);

      InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
      OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

      JobClient.runJob(job);
    }

    // Value Unique
    if (ops.equals("value")) {
      JobConf job = new JobConf();

      job.setMapperClass(OutputSchemaMapper.class);
      job.setReducerClass(OutputSchemaReducer.class);

      job.setMapOutputKeySchema(SchemaUtils.fromString("key:bigint,value:bigint"));
      job.setMapOutputValueSchema(SchemaUtils.fromString("key:bigint,value:bigint"));

      job.setPartitionColumns(new String[] {"value"});
      job.setOutputKeySortColumns(new String[] {"value"});
      job.setOutputGroupingColumns(new String[] {"value"});

      job.set("tablename2", args[1]);

      job.setNumReduceTasks(1);
      job.setInt("table.counter", 0);

      InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
      OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

      JobClient.runJob(job);
    }

  }

}
