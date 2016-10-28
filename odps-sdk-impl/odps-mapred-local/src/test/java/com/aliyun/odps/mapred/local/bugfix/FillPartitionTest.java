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

package com.aliyun.odps.mapred.local.bugfix;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

public class FillPartitionTest {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.bug);
  }

  public static class TokenizerMapper extends MapperBase {

    Record result;

    @Override
    public void setup(TaskContext context) throws IOException {

      TableInfo tableInfo = context.getInputTableInfo();
      if (!(tableInfo.getPartPath().equals("p1=1/p2=1/")
            || tableInfo.getPartPath().equals("p1=1/p2=2/")
            || tableInfo.getPartPath().equals("p1=2/p2=1/") || tableInfo.getPartPath().equals(
          "p1=2/p2=2/"))) {
        Assert.fail();
      }

      result = context.createOutputRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      for (int i = 0; i < record.getColumnCount(); i++) {
        String[] words = record.get(i).toString().split("\\s+");
        for (String w : words) {
          result.set(0, w);
          result.set(1, 1);
          context.write(result);
        }
      }
    }
  }

  @Test
  public void test() throws Exception {

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);
    InputUtils.addTable(
        TableInfo.builder().projectName("project_name").tableName("wc_in2").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("fill_partition_out").build(), job);

    RunningJob rj = JobClient.runJob(job);
    rj.waitForCompletion();

    job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);
    InputUtils.addTable(TableInfo.builder().projectName("project_name").tableName("wc_in2")
                            .partSpec("p2=1").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("fill_partition_out").build(), job);

    rj = JobClient.runJob(job);
    rj.waitForCompletion();

    job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);
    InputUtils.addTable(TableInfo.builder().projectName("project_name").tableName("wc_in2")
                            .partSpec("p2=1").partSpec("p1=2").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("fill_partition_out").build(), job);

    rj = JobClient.runJob(job);
    rj.waitForCompletion();
  }

}
