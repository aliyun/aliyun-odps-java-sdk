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

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class SetOutputGroupingColumns {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.bug);
  }

  public static class MyMapper extends MapperBase {

    Record key;
    Record val;

    @Override
    public void setup(TaskContext context) throws IOException {
      key = context.createMapOutputKeyRecord();
      val = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      key.setBigint(0, record.getBigint(0));
      key.setBigint(1, record.getBigint(1));
      val.setBigint(0, record.getBigint(2));
      context.write(key, val);
    }

  }

  public static class MyCombiner extends ReducerBase {

    Record myKey;
    Record val;

    @Override
    public void setup(TaskContext context) throws IOException {
      myKey = context.createMapOutputKeyRecord();
      val = context.createMapOutputValueRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      while (values.hasNext()) {
        myKey.setBigint(0, key.getBigint(0));
        myKey.setBigint(1, key.getBigint(1));
        Record r = values.next();
        val.setBigint(0, r.getBigint(0));
        System.out.println(key.getBigint(0) + "\t" + key.getBigint(1) + "\t" + val.getBigint(0));
        context.write(myKey, val);
      }
    }

  }

  public static class MyReducer extends ReducerBase {

    Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      while (values.hasNext()) {
        result.setBigint(0, key.getBigint(0));
        result.setBigint(1, key.getBigint(1));
        Record val = values.next();
        result.setBigint(2, val.getBigint(0));
        context.write(result);
      }
    }

  }

  @Test
  public void test() throws OdpsException {
    JobConf job = new JobConf();

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setCombinerClass(MyCombiner.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("c1:bigint,c2:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("c3:bigint"));

    job.setOutputGroupingColumns(new String[]{"c1"});
    job.setOutputKeySortColumns(new String[]{"c2"});
    job.setOutputKeySortOrder(new com.aliyun.odps.mapred.conf.JobConf.SortOrder[]{
        com.aliyun.odps.mapred.conf.JobConf.SortOrder.DESC});

    InputUtils.addTable(TableInfo.builder().tableName("iii").build(), job);
    OutputUtils
        .addTable(TableInfo.builder().tableName("setOutputGroupingColumns_out").build(), job);

    RunningJob rj = JobClient.runJob(job);
    rj.waitForCompletion();
  }
}
