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

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

public class Resource1 {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment("project_name");
  }

  public static class TokenizerMapper extends MapperBase {

    Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
      long count = 0;

      InputStream in = context.readResourceFileAsStream("file_resource.txt");
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
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
  }

  public static void main(String[] args) throws Exception {

    JobConf job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);

    InputUtils.addTable(TableInfo.builder().tableName("wc_in1").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("rs_out").build(), job);

    JobClient.runJob(job);
  }

}
