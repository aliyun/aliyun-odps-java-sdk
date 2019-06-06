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

import com.aliyun.odps.local.common.utils.SchemaUtils;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.aliyun.odps.udf.local.InvalidFunctionException;
import com.aliyun.odps.udf.local.examples.Udtf_any;
import com.aliyun.odps.udf.local.examples.Udtf_complex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.datasource.InputSource;
import com.aliyun.odps.udf.local.datasource.TableInputSource;
import com.aliyun.odps.udf.local.examples.Udtf_bi2db;
import com.aliyun.odps.udf.local.examples.Udtf_ss2si;
import com.aliyun.odps.udf.local.examples.Udtf_ss2sis_Datetime_Resource;
import com.aliyun.odps.udf.local.examples.Udtf_ss2sis_Resource;
import com.aliyun.odps.udf.local.examples.Udtf_sss2sss;
import com.aliyun.odps.udf.local.examples.Udtf_ssss2ssss;

public class UDTFRunnerTest {

  UDTFRunner runner;
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
    //runner = new UDTFRunner(null, "com.aliyun.odps.udf.local.examples.Udtf_ss2si");
    runner = new UDTFRunner(null, new Udtf_ss2si());
    runner.feed(new Object[]{"one", "one"}).feed(new Object[]{"three", "three"})
        .feed(new Object[]{"four", "four"});
    List<Object[]> out = runner.yield();
    Assert.assertEquals(3, out.size());
    Assert.assertEquals("one,3", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("three,5", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("four,4", StringUtils.join(out.get(2), ","));
  }

  @Test
  public void testInputFromTable() throws LocalRunException, UDFException, IOException {
    //runner = new UDTFRunner(odps, "com.aliyun.odps.udf.local.examples.Udtf_ssss2ssss");
    runner = new UDTFRunner(null, new Udtf_ssss2ssss());
    String project = "project_name";
    String table = "wc_in1";
    String[] partitions = null;
    String[] columns = null;

    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(4, out.size());
    Assert.assertEquals("A11,A12,A13,A14", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("A21,A22,A23,A24", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("A31,A32,A33,A34", StringUtils.join(out.get(2), ","));
    Assert.assertEquals("A41,A42,A43,A44", StringUtils.join(out.get(3), ","));

  }

  @Test
  public void testColumnFilter() throws LocalRunException, UDFException, IOException {
    //runner = new UDTFRunner(odps, "com.aliyun.odps.udf.local.examples.Udtf_sss2sss");
    runner = new UDTFRunner(null, new Udtf_sss2sss());

    String project = "project_name";
    String table = "wc_in1";
    String[] partitions = null;
    String[] columns = new String[]{"col3", "col4", "col1"};

    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(4, out.size());
    Assert.assertEquals("A13,A14,A11", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("A23,A24,A21", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("A33,A34,A31", StringUtils.join(out.get(2), ","));
    Assert.assertEquals("A43,A44,A41", StringUtils.join(out.get(3), ","));
  }

  @Test
  public void testPartitionTable() throws LocalRunException, UDFException, IOException {
    //runner = new UDTFRunner(odps, "com.aliyun.odps.udf.local.examples.Udtf_sss2sss");
    runner = new UDTFRunner(null, new Udtf_sss2sss());
    // partition table
    String project = "project_name";
    String table = "wc_in2";
    String[] partitions = null;
    String[] columns = new String[]{"colb", "colc", "cola"};
    partitions = new String[]{"p2=1", "p1=2"};
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(3, out.size());
    Assert.assertEquals("three2,three3,three1", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("three2,three3,three1", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("three2,three3,three1", StringUtils.join(out.get(2), ","));
  }

  @Test
  public void testResource() throws LocalRunException, UDFException {
    //runner = new UDTFRunner(odps, "com.aliyun.odps.udf.local.examples.Udtf_ss2sis_Resource");
    runner = new UDTFRunner(odps, new Udtf_ss2sis_Resource());
    runner.feed(new Object[]{"one", "one"}).feed(new Object[]{"three", "three"})
        .feed(new Object[]{"four", "four"});
    List<Object[]> out = runner.yield();
    Assert.assertEquals(3, out.size());
    Assert.assertEquals(
        "one,3,fileResourceLineCount=3|tableResource1RecordCount=4|tableResource2RecordCount=4",
        StringUtils.join(out.get(0), ","));
    Assert.assertEquals(
        "three,5,fileResourceLineCount=3|tableResource1RecordCount=4|tableResource2RecordCount=4",
        StringUtils.join(out.get(1), ","));
    Assert.assertEquals(
        "four,4,fileResourceLineCount=3|tableResource1RecordCount=4|tableResource2RecordCount=4",
        StringUtils.join(out.get(2), ","));
  }

  @Test
  public void testDatetimeResource() throws LocalRunException, UDFException {
    //runner = new UDTFRunner(odps,"com.aliyun.odps.udf.local.examples.Udtf_ss2sis_Datetime_Resource");
    runner = new UDTFRunner(null, new Udtf_ss2sis_Datetime_Resource());
    runner.feed(new Object[]{"one", "one"}).feed(new Object[]{"three", "three"})
        .feed(new Object[]{"four", "four"});
    List<Object[]> out = runner.yield();
    Assert.assertEquals(3, out.size());
    Assert
        .assertEquals(
            "one,3,2014-11-17 11:22:42 891|2013-01-17 11:22:42 895|2011-11-17 11:22:42 895|2012-02-17 11:22:42 895|2014-10-17 11:22:42 896",
            StringUtils.join(out.get(0), ","));
    Assert
        .assertEquals(
            "three,5,2014-11-17 11:22:42 891|2013-01-17 11:22:42 895|2011-11-17 11:22:42 895|2012-02-17 11:22:42 895|2014-10-17 11:22:42 896",
            StringUtils.join(out.get(1), ","));
    Assert
        .assertEquals(
            "four,4,2014-11-17 11:22:42 891|2013-01-17 11:22:42 895|2011-11-17 11:22:42 895|2012-02-17 11:22:42 895|2014-10-17 11:22:42 896",
            StringUtils.join(out.get(2), ","));

  }

  @Test
  public void testDataType() throws LocalRunException, UDFException, IOException {
    //runner = new UDTFRunner(null, "com.aliyun.odps.udf.local.examples.Udtf_bi2db");
    runner = new UDTFRunner(null, new Udtf_bi2db());

    runner.feed(new Object[]{new Boolean(true), 1L})
        .feed(new Object[]{new Boolean(false), 0L})
        .feed(new Object[]{new Boolean(false), -1L}).feed(new Object[]{null, null})
        .feed(new Object[]{null, Long.MAX_VALUE})
        .feed(new Object[]{new Boolean(true), Long.MIN_VALUE});
    List<Object[]> out = runner.yield();
    Assert.assertEquals(6, out.size());
    Assert.assertEquals("1.1,true", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("0.0,false", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("0.0,false", StringUtils.join(out.get(2), ","));
    Assert.assertEquals("0.0,false", StringUtils.join(out.get(3), ","));
    Assert.assertEquals("0.0,true", StringUtils.join(out.get(4), ","));
    Assert.assertEquals("1.1,false", StringUtils.join(out.get(5), ","));

    //runner = new UDTFRunner(null, "com.aliyun.odps.udf.local.examples.Udtf_bi2db");
    runner = new UDTFRunner(null, new Udtf_bi2db());
    // partition table
    String project = "project_name";
    String table = "bi";
    String[] partitions = null;
    String[] columns = null;
    partitions = null;
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    out = runner.yield();
    Assert.assertEquals(6, out.size());
    Assert.assertEquals("1.1,true", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("0.0,false", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("0.0,false", StringUtils.join(out.get(2), ","));
    Assert.assertEquals("0.0,false", StringUtils.join(out.get(3), ","));
    Assert.assertEquals("0.0,true", StringUtils.join(out.get(4), ","));
    Assert.assertEquals("1.1,false", StringUtils.join(out.get(5), ","));
  }

  @Test
  public void testFeedAll() throws LocalRunException, UDFException {
    runner = new UDTFRunner(null, new Udtf_ss2si());

    List<Object[]> inputs = new ArrayList<Object[]>();
    inputs.add(new Object[]{"one", "one"});
    inputs.add(new Object[]{"three", "three"});
    inputs.add(new Object[]{"four", "four"});

    runner.feedAll(inputs);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(3, out.size());
    Assert.assertEquals("one,3", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("three,5", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("four,4", StringUtils.join(out.get(2), ","));
  }


  @Test
  public void testRunTest() throws LocalRunException, UDFException {
    runner = new UDTFRunner(null, new Udtf_ss2si());

    List<Object[]> inputs = new ArrayList<Object[]>();
    inputs.add(new Object[]{"one", "one"});
    inputs.add(new Object[]{"three", "three"});
    inputs.add(new Object[]{"four", "four"});

    runner.feedAll(inputs);

    inputs = new ArrayList<Object[]>();
    inputs.add(new Object[]{"one", 3L});
    inputs.add(new Object[]{"three", 5L});
    inputs.add(new Object[]{"four", 4L});

    runner.runTest(inputs);

    runner = new UDTFRunner(null, new Udtf_ss2si());

    Object[][] inputs1 = new Object[3][];
    inputs1[0] = new Object[]{"one", "one"};
    inputs1[1] = new Object[]{"three", "three"};
    inputs1[2] = new Object[]{"four", "four"};

    runner.feedAll(inputs1);

    inputs1 = new Object[3][];
    inputs1[0] = new Object[]{"one", 3L};
    inputs1[1] = new Object[]{"three", 5L};
    inputs1[2] = new Object[]{"four", 4L};

    runner.runTest(inputs1);

  }
  

  @Test
  public void testMultiInput() throws LocalRunException, UDFException, IOException {
    //runner = new UDTFRunner(odps, "com.aliyun.odps.udf.local.examples.Udtf_ssss2ssss");
    runner = new UDTFRunner(null, new Udtf_ssss2ssss());
    String project = "project_name";
    String table = "wc_in1";
    String[] partitions = null;
    String[] columns = null;

    //input1
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    
    //input2
    Object[][] inputs1 = new Object[1][];
    inputs1[0] = new Object[]{"A01", "A02", "A03", "A04"};


    runner.feedAll(inputs1);
    
    
    List<Object[]> out = runner.yield();
    Assert.assertEquals(5, out.size());
    Assert.assertEquals("A01,A02,A03,A04", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("A11,A12,A13,A14", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("A21,A22,A23,A24", StringUtils.join(out.get(2), ","));
    Assert.assertEquals("A31,A32,A33,A34", StringUtils.join(out.get(3), ","));
    Assert.assertEquals("A41,A42,A43,A44", StringUtils.join(out.get(4), ","));

  }

  @Test
  public void testComplexType() throws LocalRunException, UDFException{
    runner = new UDTFRunner(null, new Udtf_complex());

    String project = "project_name";
    String table = "wc_in1";
    String[] partitions = null;
    String[] columns = new String[]{"col1"};

    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(4, out.size());
    Assert.assertEquals("A11,[A11x, A11y]", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("A21,[A21x, A21y]", StringUtils.join(out.get(1), ","));
    Assert.assertEquals("A31,[A31x, A31y]", StringUtils.join(out.get(2), ","));
    Assert.assertEquals("A41,[A41x, A41y]", StringUtils.join(out.get(3), ","));
  }

  @Test
  public void testAny() throws LocalRunException, UDFException{
    runner = new UDTFRunner(null, new Udtf_any());
    runner.feed(new Object[]{"one", "one"}).feed(new Object[]{"two", "three"});
    List<Object[]> out = runner.yield();
    Assert.assertEquals(2, out.size());
    Assert.assertEquals("true,one,one", StringUtils.join(out.get(0), ","));
    Assert.assertEquals("false,two,three", StringUtils.join(out.get(1), ","));
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseInvalidResolveTypeInfo() throws InvalidFunctionException{
    UDTFRunner.parseResolveInfo("long->long");
  }

  @Test
  public void parseResolveTypeInfo() throws InvalidFunctionException{
    String[] outs = UDTFRunner.parseResolveInfo("struct<a:bigint>,string->string");
    Assert.assertEquals(2, outs.length);
    Assert.assertEquals("struct<a:bigint>,string", outs[0]);
    Assert.assertEquals("string", outs[1]);

    outs = UDTFRunner.parseResolveInfo("varchar(10) -> smallint");
    Assert.assertEquals(2, outs.length);
    Assert.assertEquals("varchar(10) ", outs[0]);
    Assert.assertEquals(" smallint", outs[1]);
    List<TypeInfo> inputTypes = SchemaUtils.parseResolveTypeInfo(outs[0]);
    Assert.assertEquals(1, inputTypes.size());
    Assert.assertTrue(inputTypes.get(0) instanceof VarcharTypeInfo);

    outs = UDTFRunner.parseResolveInfo("ANY, * -> boolean, *");
    Assert.assertEquals(2, outs.length);
    Assert.assertEquals("ANY, * ", outs[0]);
    Assert.assertEquals(" boolean, *", outs[1]);

  }

}
