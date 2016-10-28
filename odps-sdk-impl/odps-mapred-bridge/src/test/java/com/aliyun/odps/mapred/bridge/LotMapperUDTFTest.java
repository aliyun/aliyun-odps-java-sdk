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
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.TableInfo.TableInfoBuilder;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.example.WordCount;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class LotMapperUDTFTest {

  private MockExecutionContext ctx;
  private BridgeJobConf conf;

  Object[][] testData = new Object[][]{new Object[]{new Text("to be ")},
                                       new Object[]{new Text("or not to be")}};
  Object[][] testDataSame = new Object[][]{new Object[]{new Text("1 1 1")},
                                           new Object[]{new Text("1 1 1 1")}};

  class MockMapperUDTF extends LotMapperUDTF {

    List<Object[]> forwarded = new ArrayList<Object[]>();
    Object[][] testData;
    int testDataIndex = 0;

    public MockMapperUDTF(BridgeJobConf conf, Object[][] testData) {
      this.conf = conf;
      this.testData = testData;
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

  @Before
  public void setUp() {
    ctx = new MockExecutionContext();
    conf = new BridgeJobConf();

    TableInfo tblInfo = new TableInfo.TableInfoBuilder().projectName("prj").tableName("tbl")
        .cols(new String[]{"col1"}).build();

    TableInfo output = new TableInfo.TableInfoBuilder().projectName("prj").tableName("out")
        .label("foo").build();

    ctx.setTableInfo(tblInfo.getProjectName() + "." + tblInfo.getTableName());
    InputUtils.addTable(tblInfo, conf);
    OutputUtils.addTable(output, conf);
    conf.setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"), "foo");
    conf.setInputSchema(tblInfo, SchemaUtils.fromString("word:string"));
  }

  @Test
  public void testProcess() throws Exception {
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testData);

    udtf.setup(ctx);
    udtf.run();
    udtf.close();

    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(6, forwarded.size());
    assertEquals(new Text("to"), forwarded.get(0)[0]);
    assertEquals(new LongWritable(1), forwarded.get(0)[1]);
    assertEquals(new Text("be"), forwarded.get(1)[0]);
    assertEquals(new LongWritable(1), forwarded.get(1)[1]);
    assertEquals(new Text("or"), forwarded.get(2)[0]);
    assertEquals(new LongWritable(1), forwarded.get(2)[1]);
    assertEquals(new Text("not"), forwarded.get(3)[0]);
    assertEquals(new LongWritable(1), forwarded.get(3)[1]);
    assertEquals(new Text("to"), forwarded.get(4)[0]);
    assertEquals(new LongWritable(1), forwarded.get(4)[1]);
    assertEquals(new Text("be"), forwarded.get(5)[0]);
    assertEquals(new LongWritable(1), forwarded.get(5)[1]);
  }

  @Test
  public void testCombiner() throws Exception {
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setCombinerClass(WordCount.SumCombiner.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testData);

    udtf.setup(ctx);
    udtf.run();
    udtf.close();

    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(4, forwarded.size());
    assertEquals(new Text("be"), forwarded.get(0)[0]);
    assertEquals(new LongWritable(2), forwarded.get(0)[1]);
    assertEquals(new Text("not"), forwarded.get(1)[0]);
    assertEquals(new LongWritable(1), forwarded.get(1)[1]);
    assertEquals(new Text("or"), forwarded.get(2)[0]);
    assertEquals(new LongWritable(1), forwarded.get(2)[1]);
    assertEquals(new Text("to"), forwarded.get(3)[0]);
    assertEquals(new LongWritable(2), forwarded.get(3)[1]);
  }

  @Test
  public void testCombinerBuffer() throws Exception {
    conf.setCombinerCacheItems(2);
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setCombinerClass(WordCount.SumCombiner.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testData);

    udtf.setup(ctx);
    udtf.run();
    udtf.close();

    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(6, forwarded.size());
    assertEquals(new Text("be"), forwarded.get(0)[0]);
    assertEquals(new Text("to"), forwarded.get(1)[0]);
    assertEquals(new Text("not"), forwarded.get(2)[0]);
    assertEquals(new Text("or"), forwarded.get(3)[0]);
    assertEquals(new Text("be"), forwarded.get(4)[0]);
    assertEquals(new Text("to"), forwarded.get(5)[0]);
  }

  @Test
  public void testCombinerBufferSameKey() throws Exception {
    conf.setCombinerCacheItems(2);
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setCombinerClass(WordCount.SumCombiner.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testDataSame);

    udtf.setup(ctx);
    udtf.run();
    udtf.close();

    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(4, forwarded.size());
    assertEquals(new LongWritable(2), forwarded.get(0)[1]);
    assertEquals(new LongWritable(2), forwarded.get(1)[1]);
    assertEquals(new LongWritable(2), forwarded.get(2)[1]);
    assertEquals(new LongWritable(1), forwarded.get(3)[1]);
  }

  @Test
  public void testEmptyInputWithCombiner() throws Exception {
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setCombinerClass(WordCount.SumCombiner.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testData);

    udtf.setup(ctx);
    udtf.close();

    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(0, forwarded.size());
  }

  @Test
  public void testEmptyInput() throws Exception {
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testData);

    udtf.setup(ctx);
    udtf.close();

    List<Object[]> forwarded = udtf.getForwarded();
    assertEquals(0, forwarded.size());
  }

  @Test
  public void profile() throws Exception {
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setCombinerClass(WordCount.SumCombiner.class);
    // conf.setCombinerClass(WordCount.SumCombiner.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testData);

    udtf.setup(ctx);

    Object[][] testData = new Object[100000][1];
    for (int i = 0; i < 100000; i++) {
      testData[i] = new Object[]{RandomStringUtils.randomAlphabetic(5)};
    }
    udtf.setTestData(testData);
    udtf.run();
    udtf.close();
    int sum = 0;
    for (Object[] item : udtf.getForwarded()) {
      sum += ((LongWritable) item[1]).get();
    }
    assertEquals(100000, sum);
  }

  @Test
  public void testGetTableInfo() {
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testData);
    TableInfoBuilder builder = TableInfo.builder().projectName("foo").tableName("bar");
    assertEquals(builder.build(), udtf.getTableInfo(InputUtils.getTables(conf), "foo.bar"));
    assertEquals(builder.build(), udtf.getTableInfo(InputUtils.getTables(conf),
                                                    "foo.bar/ds=19970701/hr=19;foo.bar/ds=19970701/hr=20"));
    builder.partSpec("ds=19970701/hr=19");
    assertEquals(builder.build(),
                 udtf.getTableInfo(InputUtils.getTables(conf), "foo.bar/ds=19970701/hr=19"));
  }

  public static class ExceptionMapper extends MapperBase {

    @Override
    public void map(long key, Record record, TaskContext context) throws IOException {

      throw new RuntimeException("By design.");
    }
  }

  @Test
  public void testThrowException() throws Exception {
    conf.setMapperClass(ExceptionMapper.class);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testData);

    udtf.setup(ctx);

    try {
      Object[][] testData = new Object[][]{new Object[]{new Text("to be ")},
                                           new Object[]{new Text("or not to be")}};
      udtf.setTestData(testData);
      udtf.run();
      udtf.close();
      fail("Not throwing exception.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("By design."));
    }
  }

  public static class InvalidLabelMapper extends MapperBase {

    @Override
    public void map(long key, Record record, TaskContext context) throws IOException {

      context.write(record, "nonexist");
    }
  }

  @Test
  public void testWriteToInvalidLabel() throws Exception {
    conf.setMapperClass(InvalidLabelMapper.class);
    conf.setNumReduceTasks(0);
    conf.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    conf.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().projectName("proj").tableName("tb2").label("tb2")
                            .build(), conf);
    MockMapperUDTF udtf = new MockMapperUDTF(conf, testData);
    udtf.setup(ctx);

    try {
      Object[][] testData = new Object[][]{new Object[]{new Text("to be ")},
                                           new Object[]{new Text("or not to be")}};
      udtf.setTestData(testData);
      udtf.run();

      udtf.close();
      fail("Not throwing exception.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(ErrorCode.NO_SUCH_LABEL.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    LotMapperUDTFTest test = new LotMapperUDTFTest();
    System.in.read();
    test.profile();
    System.out.println("Done");
    System.in.read();
  }
}
