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

package com.aliyun.odps.mapred.local.utils;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
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
public class WordCount {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  /**
   * Counts the words in each record. For each record, emit each column as
   * (<b>word</b>, <b>1</b>).
   */
  public static class TokenizerMapper extends MapperBase {

    @Override
    public void setup(TaskContext context) throws IOException {
//      InputStream in=context.readResourceFileAsStream("one.txt");
//      BufferedReader br=new BufferedReader(new InputStreamReader(in));
//      String line;
//      while((line=br.readLine())!=null){
//        System.out.println(line);
//      }
//      br.close();

      Iterator<Record> iterator = context.readResourceTable("table_resource1");
      int count = 0;
      while (iterator.hasNext()) {
        count++;
        iterator.next();
      }
      System.out.println(count);
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {

    }
  }

  @Test
  public void test() throws Exception {

    String[] args = new String[2];
    args[0] = "grep_in";
    args[1] = "grep_out";

    JobConf job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);

    job.setResources("table_resource1");

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
