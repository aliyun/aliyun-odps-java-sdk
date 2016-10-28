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

package com.aliyun.odps.mapred.local.function;

import java.io.IOException;

import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.CommonUtils;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * TestLongMinvalue(LONG_MIN+1 ~ LONG_MAX valid, LONG_MIN is null)
 */
public class LongMinvalue {

  public static class Mapper1 extends MapperBase {

    @Override
    public void setup(TaskContext context) throws IOException {
      Record result = context.createOutputRecord();
      String tag = context.getJobConf().get("long.value");
      Long val = null;

      if (tag.equals("MAX") || tag.equals("max")) {
        val = Long.MAX_VALUE;
      } else if (tag.equals("MIN") || tag.equals("min")) {
        val = Long.MIN_VALUE + 1;
      } else if (tag.equals("NULL") || tag.equals("null")) {
        val = null;
        ;
      } else if (tag.equals("long.min") || tag.equals("LONG.MIN")) {
        val = Long.MIN_VALUE;
      }

      result.set(0, val);

      context.write(result);
    }
  }

  public static class Mapper2 extends MapperBase {

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      Long col = (Long) record.get(0);
      String tag = context.getJobConf().get("long.value");
      Long val = null;

      if (tag.equals("MAX") || tag.equals("max")) {
        val = Long.MAX_VALUE;
      } else if (tag.equals("MIN") || tag.equals("min")) {
        val = Long.MIN_VALUE + 1;
      } else if (tag.equals("NULL") || tag.equals("null")) {
        val = null;
      } else if (tag.equals("long.min") || tag.equals("LONG.MIN")) {
        val = Long.MIN_VALUE;
      }

      if ((val == null && col != null) || (val != null && col == null)) {
        throw new IOException("test long value " + val + " failed.");
      } else if (val != null && col != null && !val.equals(col)) {
        throw new IOException("test long value " + val + " failed.");
      }
    }
  }

  public static void main(String[] args) throws OdpsException {
    if (args.length != 2) {
      System.out.println("TestLongMinvalue <out_table> <long_value>");
      System.exit(1);
    }

    JobConf job1 = new JobConf();

    job1.setMapperClass(Mapper1.class);

    job1.setNumReduceTasks(0);

    job1.set("long.value", args[1]);

    InputUtils.addTable(TableInfo.builder().tableName(CommonUtils.GENERAL_INPUT_TABLE).build(), job1);
    OutputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job1);

    job1.setMapOutputKeySchema(SchemaUtils
                                   .fromString(CommonUtils.GENERAL_KEY_ROWSCHEMA));
    job1.setMapOutputValueSchema(SchemaUtils
                                     .fromString(CommonUtils.GENERAL_VALUE_ROWSCHEMA));

    JobClient.runJob(job1);

    // Job2
    JobConf job2 = new JobConf();

    job2.setMapperClass(Mapper2.class);
    job2.setNumReduceTasks(0);

    job2.set("long.value", args[1]);

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job2);

    job2.setMapOutputKeySchema(SchemaUtils
                                   .fromString(CommonUtils.GENERAL_KEY_ROWSCHEMA));
    job2.setMapOutputValueSchema(SchemaUtils
                                     .fromString(CommonUtils.GENERAL_VALUE_ROWSCHEMA));

    JobClient.runJob(job2);
  }
  
  @Test
  public void test() throws Exception {
    WareHouse wareHouse = WareHouse.getInstance();
    String project = TestUtils.odps_test_mrtask;
    String outputTable = "mr_longminvalue_out";

    TestUtils.setEnvironment(project);
    
    new LongMinvalue().main(new String[]{outputTable,"null"});
  }
}
