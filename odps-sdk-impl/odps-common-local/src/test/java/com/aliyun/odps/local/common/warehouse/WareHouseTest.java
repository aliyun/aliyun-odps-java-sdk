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

package com.aliyun.odps.local.common.warehouse;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.WareHouse;

public class WareHouseTest {

  WareHouse wareHouse = WareHouse.getInstance();

  @Before
  public void setup() {
    Account account = new AliyunAccount("accessId", "accessKey");
    Odps odps = new Odps(account);
    odps.setEndpoint("endpoint");
    odps.setDefaultProject("project_name");
    wareHouse.setOdps(odps);
  }

  @Test
  public void testGetDataFiles() throws IOException, OdpsException {
    String project = "project_name";
    String table = "wc_in1";
    List<File> list = wareHouse.getDataFiles(project, table, null, ',');
    Assert.assertEquals(1, list.size());
    Assert.assertEquals("data", list.get(0).getName());

    table = "wc_in2";
    list = wareHouse.getDataFiles(project, table, null, ',');
    Assert.assertEquals(4, list.size());
    Assert.assertEquals("data", list.get(2).getName());

    PartitionSpec partitionSpec = new PartitionSpec();
    partitionSpec.set("p1", "1");
    list = wareHouse.getDataFiles(project, table, partitionSpec, ',');
    Assert.assertEquals(2, list.size());

    partitionSpec = new PartitionSpec();
    partitionSpec.set("p1", "1");
    partitionSpec.set("p2", "2");
    list = wareHouse.getDataFiles(project, table, partitionSpec, ',');
    Assert.assertEquals(1, list.size());

    // RuntimeException can't catch

    // partitionSpec = new PartitionSpec();
    // partitionSpec.set("p1", "1");
    // partitionSpec.set("p2", "3");
    // try {
    // list = wareHouse.getDataFiles(project, table, partitionSpec, ',');
    // } catch (Exception e) {
    // return;
    // }
    // Assert.assertEquals(0, 1);

  }

  @Test
  public void testReadData() throws IOException, OdpsException {
    String project = "project_name";
    String table = "wc_in1";

    List<Object[]> list = wareHouse.readData(project, table, null, null, ',');
    Assert.assertEquals(4, list.size());
    Assert.assertEquals(4, list.get(2).length);
    Assert.assertEquals("A3", list.get(2)[2]);

    table = "wc_in2";
    list = wareHouse.readData(project, table, null, null, ',');
    Assert.assertEquals(10, list.size());

    PartitionSpec partitionSpec = new PartitionSpec();
    partitionSpec.set("p1", "1");
    partitionSpec.set("p2", "2");
    list = wareHouse.readData(project, table, partitionSpec, null, ',');
    Assert.assertEquals(2, list.size());

    partitionSpec = new PartitionSpec();
    partitionSpec.set("p2", "1");
    list = wareHouse.readData(project, table, partitionSpec, null, ',');
    Assert.assertEquals(4, list.size());

  }

  @Test
  public void testGetPartitionToPathMap() throws IOException {
    String project = "project_name";
    String table = "wc_in2";
    Map<PartitionSpec, File> map = wareHouse.getPartitionToPathMap(project, table);
    Assert.assertEquals(4, map.size());

    // partitions has no data file
    project = "project_name";
    table = "mr_p_empty";
    map = wareHouse.getPartitionToPathMap(project, table);
    Assert.assertEquals(0, map.size());

  }

  @Test
  public void testGetProjectNames() {
    List<String> list = wareHouse.getProjectNames();
    Assert.assertEquals(true, list.contains("project_name"));
    Assert.assertEquals(true, list.contains("odps_test_bridgejob"));
  }

  @Test
  public void testGetTableNames() throws IOException {
    String project = "project_name";
    List<String> list = wareHouse.getTableNames(project);
    Assert.assertEquals(true, list.contains("rs_out"));
    Assert.assertEquals(true, list.contains("wc_in1"));
    Assert.assertEquals(true, list.contains("wc_in2"));
    Assert.assertEquals(true, list.contains("wc_out"));
  }

  @Test
  public void testValid() throws IOException {
    String projName = null;
    String tblName = null;
    PartitionSpec partitionSpec = null;
    String[] readCols = null;

    projName = "project_name";
    try {
      wareHouse.valid(projName, tblName, partitionSpec, readCols);
    } catch (OdpsException e) {
      Assert.assertEquals("Table Name is null", e.getMessage());
    }

    projName = "project_name";
    tblName = "wc_in2";
    partitionSpec = new PartitionSpec();
    partitionSpec.set("p1", "1");

    try {
      wareHouse.valid(projName, tblName, partitionSpec, readCols);
    } catch (OdpsException e) {
      Assert.assertEquals("", e.getMessage());
    }

    projName = "project_name";
    tblName = "wc_in2";
    partitionSpec = new PartitionSpec();
    partitionSpec.set("p1", "1");
    partitionSpec.set("p2", "3");
    try {
      wareHouse.valid(projName, tblName, partitionSpec, readCols);
    } catch (OdpsException e) {
      Assert.assertEquals("table project_name.wc_in2(p1=1/p2=3) not exitsts", e.getMessage());
    }

    projName = "project_name";
    tblName = "wc_in2";
    partitionSpec = new PartitionSpec();
    partitionSpec.set("p1", "1");
    partitionSpec.set("p2", "2");
    readCols = new String[]{"colc", "colb"};
    try {
      wareHouse.valid(projName, tblName, partitionSpec, readCols);
    } catch (OdpsException e) {
      Assert.assertEquals("", e.getMessage());
    }

    projName = "project_name";
    tblName = "wc_in2";
    partitionSpec = new PartitionSpec();
    partitionSpec.set("p1", "1");
    partitionSpec.set("p2", "2");
    readCols = new String[]{"cola", "cold", "colc"};
    try {
      wareHouse.valid(projName, tblName, partitionSpec, readCols);
    } catch (OdpsException e) {
      Assert.assertEquals("table project_name.wc_in2 do not have column :cold", e.getMessage());
    }
  }

  @Test
  public void testReadResourceFileAsStream() throws IOException, OdpsException {
    String project = "project_name";
    String resource = "file_resource.txt";
    BufferedInputStream in = wareHouse.readResourceFileAsStream(project, resource, ',');
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String line = br.readLine();
    Assert.assertEquals("line1", line);
    line = br.readLine();
    Assert.assertEquals("line2", line);
    line = br.readLine();
    Assert.assertEquals("line3", line);
    line = br.readLine();
    Assert.assertNull(line);
    br.close();
  }

  @Test
  public void testReadResourceTable() throws IOException, OdpsException {
    String project = "project_name";
    String resource = "table_resource2";
    Iterator<Object[]> iterator = wareHouse.readResourceTable(project, resource, ',');
    int count = 0;
    Object[] testData = null;
    while (iterator.hasNext()) {
      Object[] record = iterator.next();
      count++;
      if (count == 3) {
        testData = record;
      }
    }
    Assert.assertEquals(4, count);
    Assert.assertEquals("four2", (String) testData[1]);
  }

  @Test
  public void readResourceFile() throws IOException, OdpsException {
    String project = "project_name";
    String resource = "file_resource.txt";
    byte[] result = wareHouse.readResourceFile(project, resource, ',');
    // on windows, if core.autocrlf=true, file_resource.txt will be 19 bytes long
    Assert.assertTrue(result.length == 17 || result.length == 19);
  }

  @Test
  public void testMultiThread() throws InterruptedException {
    Account account = new AliyunAccount("accessId1", "accessKey1");
    Odps odps = new Odps(account);
    odps.setEndpoint("endpoint1");
    odps.setDefaultProject("project_name1");
    WareHouse.getInstance().setOdps(odps);
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }

        Account account = new AliyunAccount("accessId2", "accessKey2");
        Odps odps = new Odps(account);
        odps.setEndpoint("endpoint2");
        odps.setDefaultProject("project_name2");
        WareHouse.getInstance().setOdps(odps);

        Assert.assertEquals("endpoint2", WareHouse.getInstance().getOdps().getEndpoint());
        Assert.assertEquals("project_name2", WareHouse.getInstance().getOdps().getDefaultProject());

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }

        Assert.assertEquals("endpoint2", WareHouse.getInstance().getOdps().getEndpoint());
        Assert.assertEquals("project_name2", WareHouse.getInstance().getOdps().getDefaultProject());

      }
    };
    thread.start();
    Assert.assertEquals("endpoint1", WareHouse.getInstance().getOdps().getEndpoint());
    Assert.assertEquals("project_name1", WareHouse.getInstance().getOdps().getDefaultProject());

    Thread.sleep(500);

    Assert.assertEquals("endpoint1", WareHouse.getInstance().getOdps().getEndpoint());
    Assert.assertEquals("project_name1", WareHouse.getInstance().getOdps().getDefaultProject());

    account = new AliyunAccount("accessId1", "accessKey1");
    odps = new Odps(account);
    odps.setEndpoint("endpoint1");
    odps.setDefaultProject("project_name1");
    WareHouse.getInstance().setOdps(odps);

    thread.join();

    Assert.assertEquals("endpoint1", WareHouse.getInstance().getOdps().getEndpoint());
    Assert.assertEquals("project_name1", WareHouse.getInstance().getOdps().getDefaultProject());

  }

  @Test
  public void testResolvePartition() {
    Configuration configuration = new Configuration();
    configuration.set(Constants.JOB_NAME, "mr_20150212083126_505_21462");
    wareHouse.setConfiguration(configuration);

    File tempFile = new File(
        "temp/mr_20150212083126_505_21462/input/project_name/wc_in2/p1=1/p2=2/data");
    File whInputFile = wareHouse.getJobDir().convertInputFile(tempFile);
    PartitionSpec partitionSpec = wareHouse.resolvePartition("project_name", "wc_in2", whInputFile);
    Assert.assertEquals("p1='1',p2='2'", partitionSpec.toString());
  }

}
