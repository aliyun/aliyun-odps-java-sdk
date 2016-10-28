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

import junit.framework.Assert;

import org.junit.Test;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.pipeline.MultiInOut;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class MultipleInOutTest extends MRUnitTest {

  private final static String INPUT_SCHEMA = "a:string,b:string";
  private final static String OUTPUT_SCHEMA = "k:string,v:bigint";

  private JobConf job;

  public MultipleInOutTest() throws IOException {
    job = new JobConf();

    job.setMapperClass(MultiInOut.TokenizerMapper.class);
    job.setReducerClass(MultiInOut.SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("value:bigint"));
    
    job.setInt("reduce.output.num", 3);

    InputUtils.addTable(TableInfo.builder().tableName("multi_in_t1").build(), job);
//    InputUtils.addTable(TableInfo.builder().tableName("multi_in_t2").partSpec("pt=2").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("multi_out_t1").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("multi_out_t2").partSpec("a=1/b=1").label("out1").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("multi_out_t2").partSpec("a=2/b=2").label("out2").build(), job);
  }

  @Test
  public void TestMapReduce() throws IOException, ClassNotFoundException,
      InterruptedException {

    // prepare test data
    MapUTContext mapContext = new MapUTContext();
    mapContext.setInputSchema(INPUT_SCHEMA);
    mapContext.setOutputSchema(OUTPUT_SCHEMA, job);
    mapContext.setOutputSchema("out1", OUTPUT_SCHEMA, job);
    mapContext.setOutputSchema("out2", OUTPUT_SCHEMA, job);

    File inputDir = new File("src/test/resources/data/wc_in");
    mapContext.addInputRecordsFromDir(inputDir);
    Assert.assertEquals(2, mapContext.getInputRecords().size());

    // run mapper
    TaskOutput mapOutput = runMapper(job, mapContext);
    Assert.assertEquals(4, mapOutput.getOutputKeyValues().size());

    // prepare reduce data
    ReduceUTContext reduceContext = new ReduceUTContext();
    reduceContext.setOutputSchema(OUTPUT_SCHEMA, job);
    reduceContext.setOutputSchema("out1", OUTPUT_SCHEMA, job);
    reduceContext.setOutputSchema("out2", OUTPUT_SCHEMA, job);
    reduceContext.addInputKeyValues(mapOutput);

    // run reducer
    TaskOutput reduceOutput = runReducer(job, reduceContext);

    // verify results
    Assert.assertEquals(3, reduceOutput.getTotalRecordCount());
    Assert.assertTrue(equalRecords(new File("src/test/resources/data/multi_out"),
        reduceOutput.getOutputRecords(), false));
    Assert.assertTrue(equalRecords(new File("src/test/resources/data/multi_out1"),
        reduceOutput.getOutputRecords("out1"), false));
    Assert.assertTrue(equalRecords(new File("src/test/resources/data/multi_out2"),
        reduceOutput.getOutputRecords("out2"), false));
  }
}
