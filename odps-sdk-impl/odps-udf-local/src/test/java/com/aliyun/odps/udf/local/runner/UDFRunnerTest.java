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

package com.aliyun.odps.udf.local.runner;

import static org.junit.Assert.fail;

import com.aliyun.odps.udf.local.examples.UdfComplex;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.datasource.InputSource;
import com.aliyun.odps.udf.local.datasource.TableInputSource;
import com.aliyun.odps.udf.local.examples.UdfExample;
import com.aliyun.odps.udf.local.examples.UdfResource;

public class UDFRunnerTest {

  static Odps odps;

  @BeforeClass
  public static void setupBeforeClass() {
    Account account = new AliyunAccount("accessId", "accessKey");
    odps = new Odps(account);
    odps.setEndpoint("endpoint");
    odps.setDefaultProject("project_name");
  }

  @Test
  public void test() throws LocalRunException, UDFException {
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    BaseRunner runner = new UDFRunner(odps, new UdfExample());
    runner.feed(new Object[]{"one", "one"}).feed(new Object[]{"three", "three"})
        .feed(new Object[]{"four", "four"});
    List<Object[]> out = runner.yield();

    Assert.assertEquals(3, out.size());
    Assert.assertEquals("ss2s:one,one", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("ss2s:three,three", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("ss2s:four,four", StringUtils.join(out.get(2), ","));
  }

  @Test
  public void testVarargs() throws LocalRunException, UDFException{
    BaseRunner runner = new UDFRunner(odps, new UdfExample());
    runner.feed(new Object[]{BigDecimal.ONE, BigDecimal.ONE}).feed(new Object[]{BigDecimal.ONE, null})
        .feed(new Object[]{null, BigDecimal.ONE});
    List<Object[]> out = runner.yield();

    Assert.assertEquals(3, out.size());
    Assert.assertEquals("2", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("1", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("1", StringUtils.join(out.get(2), ","));
  }

  @Test
  public void testInputFromTable() throws LocalRunException, UDFException, IOException {
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    BaseRunner runner = new UDFRunner(odps, new UdfExample());

    String project = "project_name";
    String table = "wc_in1";
    String[] partitions = null;
    String[] columns = null;

    // not partition table
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(4, out.size());
    Assert.assertEquals("ssss2s:A11,A12,A13,A14", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("ssss2s:A21,A22,A23,A24", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("ssss2s:A31,A32,A33,A34", StringUtils.join(out.get(2), ","));
    Assert.assertEquals("ssss2s:A41,A42,A43,A44", StringUtils.join(out.get(3), ","));

    // not partition table
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    runner = new UDFRunner(odps, new UdfExample());
    inputSource = new TableInputSource(TableInfo.builder().projectName(project).tableName(table)
                                           .build());
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals(4, out.size());
    Assert.assertEquals("ssss2s:A11,A12,A13,A14", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("ssss2s:A21,A22,A23,A24", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("ssss2s:A31,A32,A33,A34", StringUtils.join(out.get(2), ","));
    Assert.assertEquals("ssss2s:A41,A42,A43,A44", StringUtils.join(out.get(3), ","));

  }

  @Test
  public void testColumnFilter() throws LocalRunException, UDFException, IOException {
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    BaseRunner runner = new UDFRunner(odps, new UdfExample());

    // partition table
    String project = "project_name";
    String table = "wc_in2";
    String[] partitions = null;
    String[] columns = new String[]{"colc", "cola"};
    partitions = new String[]{"p2=1", "p1=2"};
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(3, out.size());
    Assert.assertEquals("ss2s:three3,three1", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("ss2s:three3,three1", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("ss2s:three3,three1", StringUtils.join(out.get(2), ","));
  }

  @Test
  public void testPartitionTable() throws LocalRunException, UDFException, IOException {
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    BaseRunner runner = new UDFRunner(odps, new UdfExample());

    // partition table
    String project = "project_name";
    String table = "wc_in2";
    String[] partitions = null;
    String[] columns = null;
    partitions = new String[]{"p2=1", "p1=2"};
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(3, out.size());
    Assert.assertEquals("sss2s:three1,three2,three3", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("sss2s:three1,three2,three3", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("sss2s:three1,three2,three3", StringUtils.join(out.get(2), ","));

    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    runner = new UDFRunner(odps, new UdfExample());
    inputSource = new TableInputSource(TableInfo.builder().projectName(project).tableName(table)
                                           .partSpec("p2=1/p1=2").build());
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals(3, out.size());
    Assert.assertEquals("sss2s:three1,three2,three3", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("sss2s:three1,three2,three3", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("sss2s:three1,three2,three3", StringUtils.join(out.get(2), ","));
  }

  @Test
  public void testResource() throws LocalRunException, UDFException {
    //runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfResource");
    BaseRunner runner = new UDFRunner(odps, new UdfResource());
    runner.feed(new Object[]{"one", "one"}).feed(new Object[]{"three", "three"})
        .feed(new Object[]{"four", "four"});
    List<Object[]> out = runner.yield();

    Assert.assertEquals(3, out.size());
    Assert
        .assertEquals(
            "ss2s:one,one|fileResourceLineCount=3|tableResource1RecordCount=4|tableResource2RecordCount=4",
            StringUtils.join(out.get(0), ","));
    Assert
        .assertEquals(
            "ss2s:three,three|fileResourceLineCount=3|tableResource1RecordCount=4|tableResource2RecordCount=4",
            StringUtils.join(out.get(1), ","));
    Assert
        .assertEquals(
            "ss2s:four,four|fileResourceLineCount=3|tableResource1RecordCount=4|tableResource2RecordCount=4",
            StringUtils.join(out.get(2), ","));
  }

  @Test
  public void testTime() throws LocalRunException, UDFException, IOException {
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    BaseRunner runner = new UDFRunner(odps, new UdfExample());

    // partition table
    String project = "project_name";
    String table = "datetime_in";
    String[] partitions = null;
    String[] columns = null;
    partitions = null;
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(5, out.size());

    Assert.assertEquals("sss2s:str0|2014-11-17 11:22:42 891", out.get(0)[0]);
    Assert.assertEquals("sss2s:str1|2013-01-17 11:22:42 895", out.get(1)[0]);
    Assert.assertEquals("sss2s:str2|2011-11-17 11:22:42 895", out.get(2)[0]);
    Assert.assertEquals("sss2s:str3|2012-02-17 11:22:42 895", out.get(3)[0]);
    Assert.assertEquals("sss2s:str4|2014-10-17 11:22:42 896", out.get(4)[0]);

  }

  @Test
  public void testStatic() {
    try {
      //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
      BaseRunner runner = new UDFRunner(odps, new UdfExample());
      runner.feed(new Object[]{"one"});
    } catch (LocalRunException e) {
      Assert.assertEquals("'evaluate' method can't be static", e.getMessage());
      return;
    } catch (UDFException e) {
      fail("should not catch this exception");
    }
    fail("should throw exception,but not");

  }

  @Test
  public void testDataType() throws LocalRunException, UDFException, IOException {

    // //Test Integer: data from table///////
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    BaseRunner runner = new UDFRunner(odps, new UdfExample());

    String project = "project_name";
    String table = "ii";
    String[] partitions = null;
    String[] columns = null;

    // not partition table
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(5, out.size());
    Assert.assertEquals(3L, out.get(0)[0]);
    Assert.assertEquals(7L, out.get(1)[0]);
    Assert.assertEquals(11L, out.get(2)[0]);
    Assert.assertEquals(15L, out.get(3)[0]);
    Assert.assertEquals(19L, out.get(4)[0]);

    // //Test Integer: data from program/////////
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    runner = new UDFRunner(odps, new UdfExample());

    runner.feed(new Object[]{1L, 2L}).feed(new Object[]{3L, 4L})
        .feed(new Object[]{5L, 6L});
    out = runner.yield();

    Assert.assertEquals(3, out.size());
    Assert.assertEquals(3L, out.get(0)[0]);
    Assert.assertEquals(7L, out.get(1)[0]);
    Assert.assertEquals(11L, out.get(2)[0]);

    // //Test Double: data from table///////
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    runner = new UDFRunner(odps, new UdfExample());

    project = "project_name";
    table = "dd";
    partitions = null;
    columns = null;

    // not partition table
    inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals(5, out.size());
    Assert.assertEquals(3.3D, (Double) out.get(0)[0], 0.0000001D);
    Assert.assertEquals(7.7D, (Double) out.get(1)[0], 0.0000001D);
    Assert.assertEquals(12.1D, (Double) out.get(2)[0], 0.0000001D);
    Assert.assertEquals(16.5D, (Double) out.get(3)[0], 0.0000001D);
    Assert.assertEquals(20D, (Double) out.get(4)[0], 0.0000001D);

    // //Test Double: data from program/////////
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    runner = new UDFRunner(odps, new UdfExample());

    runner.feed(new Object[]{1.1D, 2.2D}).feed(new Object[]{3.3D, 4.4D})
        .feed(new Object[]{5.5D, 6.6D}).feed(new Object[]{7.7D, 8.8D})
        .feed(new Object[]{9.9D, 10.10D});
    out = runner.yield();

    Assert.assertEquals(5, out.size());
    Assert.assertEquals(3.3D, (Double) out.get(0)[0], 0.0000001D);
    Assert.assertEquals(7.7D, (Double) out.get(1)[0], 0.0000001D);
    Assert.assertEquals(12.1D, (Double) out.get(2)[0], 0.0000001D);
    Assert.assertEquals(16.5D, (Double) out.get(3)[0], 0.0000001D);
    Assert.assertEquals(20D, (Double) out.get(4)[0], 0.0000001D);

    // //Test Boolean: data from table///////
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    runner = new UDFRunner(odps, new UdfExample());

    project = "project_name";
    table = "bb";
    partitions = null;
    columns = null;
    // not partition table
    inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals(5, out.size());
    Assert.assertEquals(false, out.get(0)[0]);
    Assert.assertEquals(false, out.get(1)[0]);
    Assert.assertEquals(false, out.get(2)[0]);
    Assert.assertEquals(false, out.get(3)[0]);
    Assert.assertEquals(true, out.get(4)[0]);

    // //Test Boolean: data from program/////////
    //BaseRunner runner = new UDFRunner(odps, "com.aliyun.odps.udf.local.examples.UdfExample");
    runner = new UDFRunner(odps, new UdfExample());

    runner.feed(new Object[]{true, false}).feed(new Object[]{null, true})
        .feed(new Object[]{false, null}).feed(new Object[]{null, null})
        .feed(new Object[]{true, true});
    out = runner.yield();

    Assert.assertEquals(5, out.size());
    Assert.assertEquals(false, out.get(0)[0]);
    Assert.assertEquals(false, out.get(1)[0]);
    Assert.assertEquals(false, out.get(2)[0]);
    Assert.assertEquals(false, out.get(3)[0]);
    Assert.assertEquals(true, out.get(4)[0]);

  }

  @Test
  public void testFeedAll() throws LocalRunException, UDFException {
    BaseRunner runner = new UDFRunner(odps, new UdfExample());

    List<Object[]> inputs = new ArrayList<Object[]>();
    inputs.add(new Object[]{"one", "one"});
    inputs.add(new Object[]{"three", "three"});
    inputs.add(new Object[]{"four", "four"});

    runner.feedAll(inputs);
    List<Object[]> out = runner.yield();

    Assert.assertEquals(3, out.size());
    Assert.assertEquals("ss2s:one,one", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("ss2s:three,three", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("ss2s:four,four", StringUtils.join(out.get(2), ","));
  }

  @Test
  public void testRunTest() throws LocalRunException, UDFException {
    BaseRunner runner = new UDFRunner(odps, new UdfExample());

    List<Object[]> inputs = new ArrayList<Object[]>();
    inputs.add(new Object[]{"one", "one"});
    inputs.add(new Object[]{"three", "three"});
    inputs.add(new Object[]{"four", "four"});

    runner.feedAll(inputs);

    inputs = new ArrayList<Object[]>();
    inputs.add(new Object[]{"ss2s:one,one"});
    inputs.add(new Object[]{"ss2s:three,three"});
    inputs.add(new Object[]{"ss2s:four,four"});

    runner.runTest(inputs);

    runner = new UDFRunner(odps, new UdfExample());

    Object[][] inputs1 = new Object[3][];
    inputs1[0] = new Object[]{"one", "one"};
    inputs1[1] = new Object[]{"three", "three"};
    inputs1[2] = new Object[]{"four", "four"};

    runner.feedAll(inputs1);

    inputs1 = new Object[3][];
    inputs1[0] = new Object[]{"ss2s:one,one"};
    inputs1[1] = new Object[]{"ss2s:three,three"};
    inputs1[2] = new Object[]{"ss2s:four,four"};

    runner.runTest(inputs1);

  }
  
  @Test
  public void testMultiInput() throws LocalRunException, UDFException {
    BaseRunner runner = new UDFRunner(odps, new UdfExample());

    // partition table
    String project = "project_name";
    String table = "wc_in2";
    String[] partitions = null;
    String[] columns = new String[]{"colc", "cola"};
    partitions = new String[]{"p2=1", "p1=2"};
    
    //input1
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    
    //input2
    Object[][] inputs1 = new Object[2][];
    inputs1[0] = new Object[]{"one", "one"};
    inputs1[1] = new Object[]{"two", "two"};
    runner.feedAll(inputs1);
    
    List<Object[]> out = runner.yield();
    Assert.assertEquals(5, out.size());
    Assert.assertEquals("ss2s:one,one", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("ss2s:two,two", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("ss2s:three3,three1", StringUtils.join(out.get(2), ","));
    Assert.assertEquals("ss2s:three3,three1", StringUtils.join(out.get(3), ","));
    Assert.assertEquals("ss2s:three3,three1", StringUtils.join(out.get(4), ","));
    
  }

  @Test
  public void testNewType() throws LocalRunException, UDFException{
    BaseRunner runner = new UDFRunner(odps, new UdfComplex());
    String project = "project_name";
    String table = "max_compute_basic_types";
    String[] columns = new String[]{"a_tinyint"};
    InputSource inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals("Byte:1", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_smallint"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Short:2", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_int"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Integer:3", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_float"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Float:5.1", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_double"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Double:6.2", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_decimal"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("BigDecimal:7.3", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_vachar"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Varchar:a varchar", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_datetime"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Long:1510333261000", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_timestamp"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Long:1515052675229", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_binary"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Binary:abc", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"a_boolean"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Boolean:true", StringUtils.join(out.get(0), ","));

    table = "max_compute_complex_types";
    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"c_array"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("List:[1, 2, 3]", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"c_map"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Map:{name=ZhanShan, age=111}", StringUtils.join(out.get(0), ","));

    runner = new UDFRunner(odps, new UdfComplex());
    columns = new String[]{"c_struct"};
    inputSource = new TableInputSource(project, table, null, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals("Struct:[1, 2]", StringUtils.join(out.get(0), ","));
  }

}
