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

import com.aliyun.odps.udf.local.examples.UdafComplex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.util.Map;
import java.util.UUID;
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
import com.aliyun.odps.udf.local.examples.AggregateCharCount;
import com.aliyun.odps.udf.local.examples.AggregateCharCount_Resource;

public class AggregatorRunnerTest {

  AggregatorRunner runner;
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
    //runner = new AggregatorRunner(null, "com.aliyun.odps.udf.local.examples.AggregateCharCount");
    runner = new AggregatorRunner(null, new AggregateCharCount());
    runner.feed(new Object[]{"one", "one"}).feed(new Object[]{"three", "three"})
        .feed(new Object[]{"four", "four"});
    List<Object[]> out = runner.yield();
    Assert.assertEquals(1, out.size());
    Assert.assertEquals(24L, out.get(0)[0]);

  }

  @Test
  public void testInputFromTable() throws LocalRunException, UDFException, IOException {
    //runner = new AggregatorRunner(null, "com.aliyun.odps.udf.local.examples.AggregateCharCount");
    runner = new AggregatorRunner(null, new AggregateCharCount());
    String project = "project_name";
    String table = "wc_in1";
    String[] partitions = null;
    String[] columns = null;

    // not partition table
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(1, out.size());
    Assert.assertEquals(48L, out.get(0)[0]);

  }

  @Test
  public void testColumnFilter() throws LocalRunException, UDFException, IOException {
    //runner = new AggregatorRunner(null, "com.aliyun.odps.udf.local.examples.AggregateCharCount");
    runner = new AggregatorRunner(null, new AggregateCharCount());
    // partition table
    String project = "project_name";
    String table = "wc_in2";
    String[] partitions = null;
    String[] columns = new String[]{"colc", "cola"};
    partitions = new String[]{"p2=1", "p1=2"};
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(1, out.size());
    Assert.assertEquals(36L, out.get(0)[0]);
  }

  @Test
  public void testPartitionTable() throws LocalRunException, UDFException, IOException {
    //runner = new AggregatorRunner(null, "com.aliyun.odps.udf.local.examples.AggregateCharCount");
    runner = new AggregatorRunner(null, new AggregateCharCount());
    // partition table
    String project = "project_name";
    String table = "wc_in2";
    String[] partitions = null;
    String[] columns = null;
    partitions = new String[]{"p2=1", "p1=2"};
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    runner.addInputSource(inputSource);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(1, out.size());
    Assert.assertEquals(54L, out.get(0)[0]);
  }

  @Test
  public void testResource() throws LocalRunException, UDFException {
    //runner = new AggregatorRunner(odps, "com.aliyun.odps.udf.local.examples.AggregateCharCount_Resource");
    runner = new AggregatorRunner(odps, new AggregateCharCount_Resource());
    runner.feed(new Object[]{"one", "one"}).feed(new Object[]{"three", "three"})
        .feed(new Object[]{"four", "four"});
    List<Object[]> out = runner.yield();
    Assert.assertEquals(1, out.size());
    // 24+3+4+4
    Assert.assertEquals(35L, out.get(0)[0]);

  }

  @Test
  public void testFeedAll() throws LocalRunException, UDFException {
    runner = new AggregatorRunner(null, new AggregateCharCount());

    List<Object[]> inputs = new ArrayList<Object[]>();
    inputs.add(new Object[]{"one", "one"});
    inputs.add(new Object[]{"three", "three"});
    inputs.add(new Object[]{"four", "four"});

    runner.feedAll(inputs);
    List<Object[]> out = runner.yield();
    Assert.assertEquals(1, out.size());
    Assert.assertEquals(24L, out.get(0)[0]);

  }

  @Test
  public void testRunTest() throws LocalRunException, UDFException {
    runner = new AggregatorRunner(null, new AggregateCharCount());

    List<Object[]> inputs = new ArrayList<Object[]>();
    inputs.add(new Object[]{"one", "one"});
    inputs.add(new Object[]{"three", "three"});
    inputs.add(new Object[]{"four", "four"});

    runner.feedAll(inputs);

    inputs = new ArrayList<Object[]>();
    inputs.add(new Object[]{24L});

    runner.runTest(inputs);

    runner = new AggregatorRunner(null, new AggregateCharCount());

    Object[][] inputs1 = new Object[3][];
    inputs1[0] = new Object[]{"one", "one"};
    inputs1[1] = new Object[]{"three", "three"};
    inputs1[2] = new Object[]{"four", "four"};

    runner.feedAll(inputs1);

    inputs1 = new Object[1][];
    inputs1[0] = new Object[]{24L};

    runner.runTest(inputs1);

  }
  
  @Test
  public void testMultiInput() throws LocalRunException, UDFException, IOException {
    //runner = new AggregatorRunner(null, "com.aliyun.odps.udf.local.examples.AggregateCharCount");
    runner = new AggregatorRunner(null, new AggregateCharCount());
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
    Object[][] inputs1 = new Object[3][];
    inputs1[0] = new Object[]{"one", "one"};
    inputs1[1] = new Object[]{"three", "three"};
    inputs1[2] = new Object[]{"four", "four"};
    runner.feedAll(inputs1);
    
    List<Object[]> out = runner.yield();
    Assert.assertEquals(1, out.size());
    Assert.assertEquals(36L+24L, out.get(0)[0]);
  }

  @Test
  public void testComplex() throws LocalRunException, UDFException {
    String str1 = UUID.randomUUID().toString();
    String str2 = UUID.randomUUID().toString();
    runner = new AggregatorRunner(null, new UdafComplex());
    runner.feed(new Object[]{buildArrayList(str1, null, str2)}).feed(new Object[]{null})
        .feed(new Object[]{buildArrayList( str2, null)});
    List<Object[]> out = runner.yield();
    Assert.assertEquals(1, out.size());
    Assert.assertEquals(1, out.get(0).length);
    Object result = out.get(0)[0];
    Map map = (Map) result;
    Assert.assertEquals(3, map.size());
    Assert.assertEquals(1, map.get(str1));
    Assert.assertEquals(2, map.get(str2));
    Assert.assertEquals(2, map.get(null));
  }

  private List<String> buildArrayList(String... elements) {
    if (elements == null) {
      return null;
    }
    List<String> list = new ArrayList<>();
    for (String ele : elements) {
      list.add(ele);
    }
    return list;
  }

}
