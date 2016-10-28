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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

/**
 * Join, mr_join_src1/mr_join_src2(key bigint, value string), mr_join_out(key
 * bigint, value1 string, value2 string)
 *
 * @author mingdi
 */
public class JoinRight {

  public static final Log LOG = LogFactory.getLog(JoinRight.class);

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  public static class JoinMapper extends MapperBase {

    private Record mapkey;
    private Record mapvalue;

    @Override
    public void setup(TaskContext context) throws IOException {
      mapkey = context.createMapOutputKeyRecord();
      mapvalue = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long key, Record record, TaskContext context)
        throws IOException {
      long tag = 1;

      String val = record.get(1).toString();
      if (val.startsWith("valb_")) {
        tag = 2;
      }

      mapkey.set(0, record.get(0));
      mapkey.set(1, tag);

      mapvalue.set(0, tag);
      for (int i = 1; i < record.getColumnCount(); i++) {
        mapvalue.set(i, record.get(i));
      }

      context.write(mapkey, mapvalue);
    }

  }

  public static class JoinReducer extends ReducerBase {

    private Record result = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      long k = (Long) key.get(0);
      List<Object[]> list1 = new ArrayList<Object[]>();
      boolean flag = false;
      Record value = null;
      while (values.hasNext()) {
        value = values.next();
        long tag = (Long) value.get(0);
        if (tag == 1) {
          list1.add(value.toArray().clone());
          flag = true;
        } else {
          if (!flag) {
            int index = 0;
            result.set(index++, k);
            for (int i = 1; i < value.getColumnCount(); i++) {
              result.set(index++, null);
            }
            for (int i = 1; i < value.getColumnCount(); i++) {
              result.set(index++, value.get(i));
            }
            context.write(result);
          } else {
            for (Object[] data1 : list1) {
              int index = 0;
              result.set(index++, k);
              for (int i = 1; i < data1.length; i++) {
                result.set(index++, data1[i]);
              }
              for (int i = 1; i < value.getColumnCount(); i++) {
                result.set(index++, value.get(i));
              }
              context.write(result);
            }
          }

        }
      }

    }

  }

  @Test
  public void test() throws Exception {
    String[] args = new String[3];
    args[0] = "join_in1";
    args[1] = "join_in2";
    args[2] = "join_out_right";

    if (args.length != 3) {
      System.err.println("Usage: Join <input table1> <input table2> <out>");
      System.exit(2);
    }
    JobConf job = new JobConf();

    job.setMapperClass(JoinMapper.class);
    job.setReducerClass(JoinReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:bigint,tag:bigint"));
    job.setMapOutputValueSchema(SchemaUtils
                                    .fromString("tagx:bigint,value:string"));

    job.setPartitionColumns(new String[]{"key"});
    job.setOutputKeySortColumns(new String[]{"key", "tag"});
    job.setOutputGroupingColumns(new String[]{"key"});

    job.set("tablename2", args[1]);

    job.setNumReduceTasks(1);
    job.setInt("table.counter", 0);

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);

    JobClient.runJob(job);
  }

}
