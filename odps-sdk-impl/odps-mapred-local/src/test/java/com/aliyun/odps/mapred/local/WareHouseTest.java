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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.ApsaraAccount;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.mapred.cli.OdpsConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;

public class WareHouseTest {

  static OdpsConf odpsConf;

  static {
    odpsConf = new OdpsConf();
    Account account = new ApsaraAccount("xxx", "xxx");
    Odps odps = new Odps(account);
    odps.setDefaultProject(odpsConf.getProjName());
    if (odpsConf.getEndpoint() != null) {
      odps.setEndpoint(odpsConf.getEndpoint());
    }
    SessionState.get().setOdps(odps);
    SessionState.get().setLocalRun(true);

  }

  static WareHouse wareHouse;
  String defaultProject = "odps_test_mrtask";
  String defaultTable = "wordcount_in";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    JobConf conf = new JobConf();
    wareHouse = WareHouse.getInstance();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testGetTableMeta() {

  }

  @Test
  public void testGetPartitionsStringStringColumnArray() {

  }

  @Test
  public void testHasTableScheme() throws IOException {
    boolean flag = wareHouse.existsTable(defaultProject, defaultTable);
    assertEquals(flag, true);

    flag = wareHouse.existsTable(defaultProject, "xxx");
    assertEquals(flag, false);
  }

  @Test
  public void testGetTableScheme() {

  }

  @Test
  public void testHasPartition() throws IOException {
    PartitionSpec partSpec = new PartitionSpec("p1=1,p2=2");
    boolean flag = wareHouse.existsPartition(defaultProject, defaultTable, partSpec);
    assertEquals(flag, true);

    partSpec = new PartitionSpec("p2=2,p1=2");
    flag = wareHouse.existsPartition(defaultProject, defaultTable, partSpec);
    assertEquals(flag, true);

    partSpec = new PartitionSpec("p1=1");
    flag = wareHouse.existsPartition(defaultProject, defaultTable, partSpec);
    assertEquals(flag, true);

    partSpec = new PartitionSpec("p3=2,p4=2");
    flag = wareHouse.existsPartition(defaultProject, defaultTable, partSpec);
    assertEquals(flag, false);

  }

  @Test
  public void testGetPartitionsStringString() {

  }

}
