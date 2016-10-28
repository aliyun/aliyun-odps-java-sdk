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

package com.aliyun.odps.mapred.unittest;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class ReadTableResourceTest extends MRUnitTest {
  private final static String INPUT_SCHEMA = "a:string,b:string";
  private final static String OUTPUT_SCHEMA = "k:string,v:bigint";
  private static JobConf job = new JobConf();

  public static class TableReadMapper extends MapperBase {

    @Override
    public void setup(TaskContext context) throws IOException {
      Record record = context.createOutputRecord();
      String resourceName = context.getJobConf().get("test_table");
      Iterator<Record> iter = context.readResourceTable(resourceName);
      while (iter != null && iter.hasNext()) {
        Record r = iter.next();
        for (int i = 0; i < r.getColumnCount(); ++i) {
          record.set(0, r.get(i));
          record.set(1, new Long(1));
          context.write(record);
        }
      }
    }
  }

  public ReadTableResourceTest() {
    job.setMapperClass(TableReadMapper.class);

    job.set("test_table", "wc_in_table");

    job.setNumReduceTasks(0);

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("value:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName("mr_empty").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("table_out").build(), job);
  }

  @Test
  public void TestMapReduce() throws IOException, ClassNotFoundException,
      InterruptedException {
    // prepare test data
    MapUTContext mapContext = new MapUTContext();
    mapContext.setInputSchema(INPUT_SCHEMA);
    mapContext.setOutputSchema(OUTPUT_SCHEMA, job);
    File tableDir = new File("src/test/resources/data/wc_in");
    mapContext.setTableResource("wc_in_table", tableDir);

    // run mapper
    TaskOutput mapOutput = runMapper(job, mapContext);

    // verify results
    Assert.assertEquals(4, mapOutput.getTotalRecordCount());
    Assert.assertTrue(equalRecords(new File("src/test/resources/data/table_out"),
        mapOutput.getOutputRecords(), false));
  }
}
