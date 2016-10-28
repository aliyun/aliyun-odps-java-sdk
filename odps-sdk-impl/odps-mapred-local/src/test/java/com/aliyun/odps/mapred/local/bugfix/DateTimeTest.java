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
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class DateTimeTest {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  public static class TokenizerMapper extends MapperBase {

    Record key;
    Record val;

    @Override
    public void setup(TaskContext context) throws IOException {
      key = context.createMapOutputKeyRecord();
      val = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      key.setString(0, record.getString(0) + "i");
      key.setDatetime(1, record.getDatetime(1));
      val.set(0, "one");
      context.write(key, val);

    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class SumReducer extends ReducerBase {

    Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();

    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {

      while (values.hasNext()) {
        result.setString(0, key.getString(0) + "j");
        Record r = values.next();
        result.set(1, key.get(1));
        context.write(result);
      }
    }
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[3];
    args[0] = "datetime_in";
    args[1] = "datetime_out";

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeySortColumns(new String[]{"b"});
    job.setOutputKeySortOrder(new com.aliyun.odps.mapred.conf.JobConf.SortOrder[]{

        com.aliyun.odps.mapred.conf.JobConf.SortOrder.DESC});

    job.setMapOutputKeySchema(SchemaUtils.fromString("a:string,b:datetime"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("c:string"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
