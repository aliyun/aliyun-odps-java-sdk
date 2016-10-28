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
import java.util.Date;
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
 * This is the trivial map/reduce program that does absolutely nothing other than use the framework
 * to fragment and sort the input values.
 * 
 **/
public class Sort {

  static int printUsage() {
    System.out.println("sort <input> <output>");
    return -1;
  }

  /**
   * Implements the identity function, mapping record's first two columns to outputs.
   **/
  public static class IdentityMapper extends MapperBase {
    private Record key;
    private Record value;

    @Override
    public void setup(TaskContext context) throws IOException {
      key = context.createMapOutputKeyRecord();
      value = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      key.set(new Object[] {(Long) record.get(0)});
      value.set(new Object[] {(Long) record.get(1)});
      context.write(key, value);
    }

  }

  public class IdentityReducer extends ReducerBase {

    private Record result = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    /**
     * Writes all keys and values directly to output.
     */
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

  /**
   * The main driver for sort program. Invoke this method to submit the map/reduce job.
   * 
   * @throws IOException When there is communication problems with the job tracker.
   **/
  public static void main(String[] args) throws Exception {

    JobConf jobConf = new JobConf();

    jobConf.setMapperClass(IdentityMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);

    jobConf.setNumReduceTasks(1);

    jobConf.setMapOutputKeySchema(SchemaUtils.fromString("key:bigint"));
    jobConf.setMapOutputValueSchema(SchemaUtils.fromString("value:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), jobConf);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), jobConf);

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);

    JobClient.runJob(jobConf);

    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
    System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000
        + " seconds.");
  }
}
