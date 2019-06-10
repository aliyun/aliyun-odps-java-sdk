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
import java.util.LinkedHashMap;

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
 * Multi input & output example.
 * 
 **/
public class MultipleInOut {

  public static class TokenizerMapper extends MapperBase {
    Record word;
    Record one;

    @Override
    public void setup(TaskContext context) throws IOException {
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
    private Record result;
    private Record result1;
    private Record result2;

    @Override
    public void setup(TaskContext context) throws IOException {
      // 对于不同的输出需要创建不同的record，通过label来区分
      result = context.createOutputRecord();
      result1 = context.createOutputRecord("out1");
      result2 = context.createOutputRecord("out2");
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
      long count = 0;
      while (values.hasNext()) {
        Record val = values.next();
        count += (Long) val.get(0);
      }

      long mod = count % 3;
      if (mod == 0) {
        result.set(0, key.get(0));
        result.set(1, count);
        // 不指定label，输出的默认(default)输出
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

    @Override
    public void cleanup(TaskContext context) throws IOException {
      Record result = context.createOutputRecord();

      result.set(0, "default");
      result.set(1, 1L);
      context.write(result);

      Record result1 = context.createOutputRecord("out1");
      result1.set(0, "out1");
      result1.set(1, 1L);
      context.write(result1, "out1");

      Record result2 = context.createOutputRecord("out2");
      result2.set(0, "out1");
      result2.set(1, 1L);
      context.write(result2, "out2");
    }
  }

  // 将分区字符串如"ds=1/pt=2"转为map的形式
  public static LinkedHashMap<String, String> convertPartSpecToMap(String partSpec) {
    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
    if (partSpec != null && !partSpec.trim().isEmpty()) {
      String[] parts = partSpec.split("/");
      for (String part : parts) {
        String[] ss = part.split("=");
        if (ss.length != 2) {
          throw new RuntimeException("ODPS-0730001: error part spec format: " + partSpec);
        }
        map.put(ss[0], ss[1]);
      }
    }
    return map;
  }

  public static void main(String[] args) throws Exception {

    String[] inputs = null;
    String[] outputs = null;
    if (args.length == 2) {
      inputs = args[0].split(",");
      outputs = args[1].split(",");
    } else {
      System.err.println("MultipleInOut in... out...");
      System.exit(1);
    }

    JobConf job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    // 解析用户的输入表字符串
    for (String in : inputs) {
      String[] ss = in.split("\\|");
      if (ss.length == 1) {
        InputUtils.addTable(TableInfo.builder().tableName(ss[0]).build(), job);
      } else if (ss.length == 2) {
        LinkedHashMap<String, String> map = convertPartSpecToMap(ss[1]);
        InputUtils.addTable(TableInfo.builder().tableName(ss[0]).partSpec(map).build(), job);
      } else {
        System.err.println("Style of input: " + in + " is not right");
        System.exit(1);
      }
    }

    // 解析用户的输出表字符串
    for (String out : outputs) {
      String[] ss = out.split("\\|");
      if (ss.length == 1) {
        OutputUtils.addTable(TableInfo.builder().tableName(ss[0]).build(), job);
      } else if (ss.length == 2) {
        LinkedHashMap<String, String> map = convertPartSpecToMap(ss[1]);
        OutputUtils.addTable(TableInfo.builder().tableName(ss[0]).partSpec(map).build(), job);
      } else if (ss.length == 3) {
        if (ss[1].isEmpty()) {
          LinkedHashMap<String, String> map = convertPartSpecToMap(ss[2]);
          OutputUtils.addTable(TableInfo.builder().tableName(ss[0]).partSpec(map).build(), job);
        } else {
          LinkedHashMap<String, String> map = convertPartSpecToMap(ss[1]);
          OutputUtils.addTable(TableInfo.builder().tableName(ss[0]).partSpec(map).label(ss[2])
              .build(), job);
        }
      } else {
        System.err.println("Style of output: " + out + " is not right");
        System.exit(1);
      }
    }

    JobClient.runJob(job);
  }

}
