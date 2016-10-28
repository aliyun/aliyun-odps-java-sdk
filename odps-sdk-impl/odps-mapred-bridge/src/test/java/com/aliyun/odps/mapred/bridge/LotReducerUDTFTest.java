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

package com.aliyun.odps.mapred.bridge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.example.WordCount;
import com.aliyun.odps.mapred.example.WordCountWithMultiInsert;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;

public class LotReducerUDTFTest {

  private static ExecutionContext ctx = new MockExecutionContext();

  class MockReducerUDTF extends LotReducerUDTF {

    List<Object[]> forwarded = new ArrayList<Object[]>();
    Object[][] testData;
    int testDataIndex = 0;

    public MockReducerUDTF(BridgeJobConf conf) {
      this.conf = conf;
    }

    public List<Object[]> getForwarded() {
      return forwarded;
    }

    @Override
    public void forward(Object... o) {
      forwarded.add(o.clone());
    }

    public Object[] getNextRowWapper() {
      if (testDataIndex < testData.length) {
        return testData[testDataIndex++];
      }
      return null;
    }

    public void setTestData(Object[][] testData) {
      this.testData = testData;
    }
  }

  ;

  @Test
  public void testProcess() throws Exception {
    BridgeJobConf conf = new BridgeJobConf();
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    conf.setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"),
                         TableInfo.DEFAULT_LABEL);
    InputUtils.addTable(TableInfo.builder().tableName("in_tbl").build(), conf);
    OutputUtils.addTable(TableInfo.builder().tableName("out_tbl").build(), conf);
    MockReducerUDTF udtf = new MockReducerUDTF(conf);

    udtf.setup(ctx);
    Object[][] testData = new Object[][]{new Object[]{new Text("word"), new LongWritable(1)},
                                         new Object[]{new Text("count"), new LongWritable(2)},
                                         new Object[]{new Text("count"), new LongWritable(1)},
                                         new Object[]{new Text("count"), new LongWritable(1)},
                                         new Object[]{new Text("foo"), new LongWritable(1)},
                                         new Object[]{new Text("bar"), new LongWritable(1)}};
    udtf.setTestData(testData);
    udtf.run();
    udtf.close();
    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(4, forwarded.size());
    assertEquals(new Text("word"), forwarded.get(0)[0]);
    assertEquals(new LongWritable(1), forwarded.get(0)[1]);
    assertEquals(new Text("count"), forwarded.get(1)[0]);
    assertEquals(new LongWritable(4), forwarded.get(1)[1]);
    assertEquals(new Text("foo"), forwarded.get(2)[0]);
    assertEquals(new LongWritable(1), forwarded.get(2)[1]);
    assertEquals(new Text("bar"), forwarded.get(3)[0]);
    assertEquals(new LongWritable(1), forwarded.get(3)[1]);
  }

  @Test
  public void testSkew() throws Exception {
    BridgeJobConf conf = new BridgeJobConf();
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    conf.setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"),
                         TableInfo.DEFAULT_LABEL);
    InputUtils.addTable(TableInfo.builder().tableName("in_tbl").build(), conf);
    OutputUtils.addTable(TableInfo.builder().tableName("out_tbl").build(), conf);
    MockReducerUDTF udtf = new MockReducerUDTF(conf);

    udtf.setup(ctx);
    Object[][] testData = new Object[10000][2];
    Object[] item = new Object[]{new Text("word"), new LongWritable(1)};
    for (int i = 0; i < 10000; i++) {
      testData[i] = item;
    }
    udtf.setTestData(testData);
    udtf.run();
    udtf.close();
    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(1, forwarded.size());
    assertEquals(new Text("word"), forwarded.get(0)[0]);
    assertEquals(new LongWritable(10000), forwarded.get(0)[1]);
  }

  @Test
  public void testMultipleKey() throws Exception {
    BridgeJobConf conf = new BridgeJobConf();
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    conf.setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"),
                         TableInfo.DEFAULT_LABEL);
    InputUtils.addTable(TableInfo.builder().tableName("in_tbl").build(), conf);
    OutputUtils.addTable(TableInfo.builder().tableName("out_tbl").build(), conf);
    MockReducerUDTF udtf = new MockReducerUDTF(conf);

    udtf.setup(ctx);
    Object[][] testData = new Object[1000][2];
    for (int i = 0; i < 1000; i++) {
      Object[] item = new Object[]{new Text("word" + i), new LongWritable(i)};
      testData[i] = item;
    }
    udtf.setTestData(testData);
    udtf.run();
    udtf.close();
    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(1000, forwarded.size());
    assertEquals(new Text("word999"), forwarded.get(999)[0]);
    assertEquals(new LongWritable(999), forwarded.get(999)[1]);
  }

  @Test
  public void testMultiInsert() throws Exception {
    BridgeJobConf conf = new BridgeJobConf();
    conf.setReducerClass(WordCountWithMultiInsert.SumReducer.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    conf.setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"), "out1");
    conf.setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"), "out2");
    InputUtils.addTable(TableInfo.builder().tableName("in_tbl").build(), conf);
    OutputUtils.addTable(TableInfo.builder().tableName("out_tbl").label("out1").build(), conf);
    OutputUtils.addTable(TableInfo.builder().tableName("out_tbl").label("out2").build(), conf);
    MockReducerUDTF udtf = new MockReducerUDTF(conf);

    udtf.setup(ctx);

    Object[][] testData = new Object[][]{new Object[]{new Text("word"), new LongWritable(1)},
                                         new Object[]{new Text("word"), new LongWritable(2)},
                                         new Object[]{new Text("count"), new LongWritable(2)}};
    udtf.setTestData(testData);
    udtf.run();
    udtf.close();
    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(4, forwarded.size());
    assertEquals(new Text("word"), forwarded.get(0)[0]);
    assertEquals(new LongWritable(3), forwarded.get(0)[1]);
    assertEquals(new Text("out2"), forwarded.get(0)[2]);
    assertEquals(new Text("count"), forwarded.get(3)[0]);
    assertEquals(new LongWritable(2), forwarded.get(3)[1]);
    assertEquals(new Text("out1"), forwarded.get(3)[2]);
  }

  @Test
  public void testEmptyInput() throws Exception {
    BridgeJobConf conf = new BridgeJobConf();
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    conf.setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"),
                         TableInfo.DEFAULT_LABEL);
    InputUtils.addTable(TableInfo.builder().tableName("in_tbl").build(), conf);
    OutputUtils.addTable(TableInfo.builder().tableName("out_tbl").build(), conf);
    MockReducerUDTF udtf = new MockReducerUDTF(conf);

    udtf.setup(ctx);
    udtf.close();
    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(0, forwarded.size());
  }

  @Test
  public void testNoIteration() throws Exception {
    BridgeJobConf conf = new BridgeJobConf();
    conf.setReducerClass(ReducerBase.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("nil:bigint"));
    conf.setOutputSchema(SchemaUtils.fromString("key:string"), TableInfo.DEFAULT_LABEL);
    InputUtils.addTable(TableInfo.builder().tableName("in_tbl").build(), conf);
    OutputUtils.addTable(TableInfo.builder().tableName("out_tbl").build(), conf);
    MockReducerUDTF udtf = new MockReducerUDTF(conf);

    udtf.setup(ctx);
    Object[] item = new Object[]{new Text("word"), new LongWritable(1)};
    for (int i = 0; i < 2; i++) {
      udtf.process(item);
    }
    udtf.close();
    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(0, forwarded.size());
  }

  public static class ReduceExceptionReducer extends ReducerBase {

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      throw new RuntimeException("By design.");
    }
  }

  @Test
  public void testThrowException() throws Exception {
    BridgeJobConf conf = new BridgeJobConf();
    conf.setReducerClass(ReduceExceptionReducer.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    conf.setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"),
                         TableInfo.DEFAULT_LABEL);
    InputUtils.addTable(TableInfo.builder().tableName("in_tbl").build(), conf);
    OutputUtils.addTable(TableInfo.builder().tableName("out_tbl").build(), conf);
    MockReducerUDTF udtf = new MockReducerUDTF(conf);

    udtf.setup(ctx);
    try {
      Object[][] testData = new Object[][]{
          new Object[]{new Text("word"), new LongWritable(1)},
          new Object[]{new Text("word"), new LongWritable(2)}};
      udtf.setTestData(testData);
      udtf.run();
      udtf.close();
      fail("Not throwing exception.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("By design."));
    }
  }

  @Test
  public void profile() throws IOException, UDFException {
    BridgeJobConf conf = new BridgeJobConf();
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    conf.setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"),
                         TableInfo.DEFAULT_LABEL);
    InputUtils.addTable(TableInfo.builder().tableName("in_tbl").build(), conf);
    OutputUtils.addTable(TableInfo.builder().tableName("out_tbl").build(), conf);
    MockReducerUDTF udtf = new MockReducerUDTF(conf);

    udtf.setup(ctx);
    Object[][] testData = new Object[1000000][2];
    for (int i = 0; i < 1000000; i++) {
      Object[] item = new Object[]{new Text(RandomStringUtils.randomAlphabetic(2)),
                                   new LongWritable(1)};
      testData[i] = item;
    }
    udtf.setTestData(testData);
    udtf.run();
    udtf.close();
    int sum = 0;
    for (Object[] item : udtf.getForwarded()) {
      sum += ((LongWritable) item[1]).get();
    }
    assertEquals(1000000, sum);
  }

  public static void main(String[] args) throws IOException, UDFException {
    LotReducerUDTFTest test = new LotReducerUDTFTest();
    System.in.read();
    test.profile();
    System.out.println("Done");
    System.in.read();
  }
}
