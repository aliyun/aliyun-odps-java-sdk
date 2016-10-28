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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * 
 * Extracts matching regexs from input files and counts them.
 * 
 **/
public class Grep {

  /**
   * RegexMapper
   **/
  public class RegexMapper extends MapperBase {
    private Pattern pattern;
    private int group;

    private Record word;
    private Record one;

    @Override
    public void setup(TaskContext context) throws IOException {
      JobConf job = (JobConf) context.getJobConf();
      pattern = Pattern.compile(job.get("mapred.mapper.regex"));
      group = job.getInt("mapred.mapper.regex.group", 0);

      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.set(new Object[] {1L});
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      for (int i = 0; i < record.getColumnCount(); ++i) {
        String text = record.get(i).toString();
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
          word.set(new Object[] {matcher.group(group)});
          context.write(word, one);
        }
      }
    }
  }

  /**
   * LongSumReducer
   **/
  public class LongSumReducer extends ReducerBase {
    private Record result = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
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

  /**
   * A {@link Mapper} that swaps keys and values.
   **/
  public class InverseMapper extends MapperBase {
    private Record word;
    private Record count;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputValueRecord();
      count = context.createMapOutputKeyRecord();
    }

    /**
     * The inverse function. Input keys and values are swapped.
     **/
    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      word.set(new Object[] {record.get(0).toString()});
      count.set(new Object[] {(Long) record.get(1)});
      context.write(count, word);
    }
  }

  /**
   * IdentityReducer
   **/
  public class IdentityReducer extends ReducerBase {
    private Record result = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    /** Writes all keys and values directly to output. **/

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
      result.set(0, key.get(0));

      while (values.hasNext()) {
        Record val = values.next();
        result.set(1, val.get(0));
        context.write(result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Grep <inDir> <tmpDir> <outDir> <regex> [<group>]");
      System.exit(2);
    }

    JobConf grepJob = new JobConf();

    grepJob.setMapperClass(RegexMapper.class);
    grepJob.setReducerClass(LongSumReducer.class);

    grepJob.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    grepJob.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), grepJob);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), grepJob);

    grepJob.set("mapred.mapper.regex", args[3]);
    if (args.length == 5) {
      grepJob.set("mapred.mapper.regex.group", args[4]);
    }

    @SuppressWarnings("unused")
    RunningJob rjGrep = JobClient.runJob(grepJob);

    JobConf sortJob = new JobConf();

    sortJob.setMapperClass(InverseMapper.class);
    sortJob.setReducerClass(IdentityReducer.class);

    sortJob.setMapOutputKeySchema(SchemaUtils.fromString("count:bigint"));
    sortJob.setMapOutputValueSchema(SchemaUtils.fromString("word:string"));

    InputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), sortJob);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), sortJob);

    sortJob.setNumReduceTasks(1); // write a single file
    sortJob.setOutputKeySortColumns(new String[] {"count"});

    @SuppressWarnings("unused")
    RunningJob rjSort = JobClient.runJob(sortJob);
  }

}
