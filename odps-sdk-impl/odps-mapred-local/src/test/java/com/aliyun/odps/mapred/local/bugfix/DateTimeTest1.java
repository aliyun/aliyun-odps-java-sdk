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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class DateTimeTest1 {
  
  public static DateFormat getDateFormat(String formateStr) {
    SimpleDateFormat dateFormat = new SimpleDateFormat(formateStr);
    return dateFormat;
  }

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  public static class TokenizerMapper extends MapperBase {

    Record key;
    Record val;

    List<Date> result;

    @Override
    public void setup(TaskContext context) throws IOException {
      key = context.createMapOutputKeyRecord();
      val = context.createMapOutputValueRecord();
      result = new LinkedList<Date>();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      key.setString(0, record.getString(0));
      val.setDatetime(0, record.getDatetime(1));
      context.write(key, val);

      result.add(new Date(record.getDatetime(1).getTime()));

    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
      int i = 0;
      Assert.assertEquals("2014-11-17 11:22:42 891",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));
      Assert.assertEquals("2013-01-17 11:22:42 895",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));
      Assert.assertEquals("2011-11-17 11:22:42 895",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));
      Assert.assertEquals("2012-02-17 11:22:42 895",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));
      Assert.assertEquals("2014-10-17 11:22:42 896",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));
    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class SumReducer extends ReducerBase {

    List<Date> result;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = new LinkedList<Date>();
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
      int i = 0;
      Assert.assertEquals("2014-11-17 11:22:42 891",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));
      Assert.assertEquals("2013-01-17 11:22:42 895",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));
      Assert.assertEquals("2011-11-17 11:22:42 895",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));
      Assert.assertEquals("2012-02-17 11:22:42 895",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));
      Assert.assertEquals("2014-10-17 11:22:42 896",
          DateTimeTest1.getDateFormat(Constants.DATE_FORMAT_2).format(result.get(i++)));

    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
      while (values.hasNext()) {
        result.add(new Date(values.next().getDatetime(0).getTime()));
      }

    }
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[1];
    args[0] = "datetime_in";


    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("val:datetime"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);

    JobClient.runJob(job);
  }

}
