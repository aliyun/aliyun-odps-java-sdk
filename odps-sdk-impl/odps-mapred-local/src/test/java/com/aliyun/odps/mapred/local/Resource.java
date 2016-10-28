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

package com.aliyun.odps.mapred.local;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

/**
 * This is an example ODPS Map/Reduce application. It reads the input table, map
 * each column into words and counts them. The output is a locally sorted list
 * of words and the count of how often they occurred.
 * <p>
 * To run: jar -libjars mapreduce-examples.jar -classpath
 * clt/lib/mapreduce-examples.jar com.aliyun.odps.mapreduce.examples.WordCount
 * <i>in-tbl</i> <i>out-tbl</i>
 */
public class Resource {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  /**
   * Counts the words in each record. For each record, emit each column as
   * (<b>word</b>, <b>1</b>).
   */
  public static class TokenizerMapper extends MapperBase {

    Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
      InputStream in = context.readResourceFileAsStream("file_resource.txt");
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      long count = 0;
      while ((line = br.readLine()) != null) {
        count++;
      }
      br.close();

      result.set(0, "file_resource");
      result.set(1, count);
      context.write(result);

      Iterator<Record> iterator = context.readResourceTable("table_resource1");
      count = 0;
      while (iterator.hasNext()) {
        count++;
        iterator.next();
      }
      result.set(0, "table_resource1");
      result.set(1, count);
      context.write(result);

      iterator = context.readResourceTable("table_resource2");
      count = 0;
      while (iterator.hasNext()) {
        count++;
        iterator.next();
      }
      result.set(0, "table_resource2");
      result.set(1, count);
      context.write(result);
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {

    }
  }

  @Test
  public void test() throws Exception {

    String[] args = new String[2];
    args[0] = "grep_in";
    args[1] = "resource_out";

    JobConf job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);

    job.setResources("file_resource.txt");

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
