/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps.tunnel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.jcip.annotations.NotThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.security.SecurityConfiguration;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.task.SQLCostTask;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel.DownloadSession;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;


/**
 * Created by nizheming on 15/12/15.
 */
@NotThreadSafe
public class InstanceTunnelTest extends TestBase {

  private static InstanceTunnel tunnel;
  private static InstanceTunnel protectionTunnel;

  private static Odps protectionOdps;


  @BeforeClass
  public static void testInstanceTunnel() throws OdpsException, IOException {
    protectionOdps = TestBase.odps.clone();
    protectionOdps.setDefaultProject("odps_test_project_instancetunnel");
    odps.tables().delete("instance_tunnel_test", true);
    protectionOdps.tables().delete("instance_tunnel_test", true);
    tunnel = OdpsTestUtils.newInstanceTunnel(odps);
    protectionTunnel = OdpsTestUtils.newInstanceTunnel(protectionOdps);
    OdpsTestUtils.createTableForTest(odps, "instance_tunnel_test");
    OdpsTestUtils.createTableForTest(protectionOdps, "instance_tunnel_test");
    // Map<String, String> hints = new HashMap<String, String>();
    // hints.put("odps.sql.preparse.odps2", "true");
    // hints.put("odps.sql.planner.parser.odps2", "true");
    // hints.put("odps.sql.planner.mode", "lot");
    // hints.put("odps.compiler.output.format", "lot,pot");
    // hints.put("odps.compiler.playback", "true");
    // hints.put("odps.compiler.warning.disable", "false");
    // hints.put("odps.sql.ddl.odps2", "true");
    // hints.put("odps.sql.runtime.mode", "EXECUTIONENGINE");
    // hints.put("odps.sql.sqltask.new", "true");
    // SQLTask.setDefaultHints(hints);
  }

  @Test
  public void testNormalSelect() throws OdpsException, IOException {
    Instance instance = SQLTask.run(odps, "select count(*) as cnt from instance_tunnel_test;");
    System.err.println(instance.getId());
    instance.waitForSuccess();
    InstanceTunnel.DownloadSession session =
        tunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    assertEquals(session.getRecordCount(), 1L);
    assertEquals(session.getSchema().getColumns().size(), 1L);
    assertEquals(session.getSchema().getColumns().get(0).getName(), "cnt");
    assertEquals(session.getSchema().getColumns().get(0).getType(), OdpsType.BIGINT);
    TunnelRecordReader reader = session.openRecordReader(0, 1L);
    Record record = reader.read();
    System.out.println(record.getBigint(0));
    assertTrue(record.getBigint(0) > 0L);
    assertNull(reader.read());
  }

  @Test
  public void testSelectStarFromTableAndCompress() throws OdpsException, IOException {
    Instance instance = SQLTask.run(odps, "select * from instance_tunnel_test;");
    System.err.println(instance.getId());
    instance.waitForSuccess();
    InstanceTunnel.DownloadSession session =
        tunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    assertTrue(session.getRecordCount() > 0L);
    assertEquals(session.getSchema().getColumns().size(), 1L);
    assertEquals(session.getSchema().getColumns().get(0).getName(), "c1");
    assertEquals(session.getSchema().getColumns().get(0).getType(), OdpsType.BIGINT);
    TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount());
    for (Record record = reader.read(); record != null; record = reader.read()) {
      System.err.println(record.get(0).toString());
    }

    // test compress
    reader = session.openRecordReader(0, session.getRecordCount(), true);
    for (Record record = reader.read(); record != null; record = reader.read()) {
      System.err.println(record.get(0).toString());
    }
  }

  @Test
  public void testLimit() throws OdpsException, IOException {
    Instance instance = SQLTask.run(odps, "select * from instance_tunnel_test limit 2;");
    System.err.println(instance.getId());
    instance.waitForSuccess();
    InstanceTunnel.DownloadSession session =
        tunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    assertEquals(session.getRecordCount(), 2L);
    assertEquals(session.getSchema().getColumns().size(), 1L);
    assertEquals(session.getSchema().getColumns().get(0).getName(), "c1");
    assertEquals(session.getSchema().getColumns().get(0).getType(), OdpsType.BIGINT);
    TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount());
    for (Record record = reader.read(); record != null; record = reader.read()) {
      System.err.println(record.get(0).toString());
    }

  }


  @Test
  public void testSelectOrderBy() throws OdpsException, IOException {
    // order by must has order in tunnel download
    Instance instance =
        SQLTask
            .run(
                odps,
                "select row_number() over (partition by 1 order by rand()) as id from instance_tunnel_test order by id limit 25;");
    System.err.println(instance.getId());
    instance.waitForSuccess();
    InstanceTunnel.DownloadSession session =
        tunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    assertEquals(session.getRecordCount(), 21L);
    assertEquals(session.getSchema().getColumns().size(), 1L);
    assertEquals(session.getSchema().getColumns().get(0).getName(), "id");
    assertEquals(session.getSchema().getColumns().get(0).getType(), OdpsType.BIGINT);
    TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount());
    for (int i = 0; i < session.getRecordCount(); ++i) {
      Record record = reader.read();
      System.err.println(record.getBigint(0));
      assertEquals(record.getBigint(0), new Long(i + 1));
    }

  }

  @Test
  public void testMaxDataSize() throws OdpsException, IOException {
    // big data table
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("id", OdpsType.STRING));
    odps.tables().delete("instance_tunnel_big", true);

    odps.tables().create("instance_tunnel_big", schema);
    TableTunnel tunnel = OdpsTestUtils.newTableTunnel(odps);
    TableTunnel.UploadSession uploadSession =
        tunnel.createUploadSession(odps.getDefaultProject(), "instance_tunnel_big");
    RecordWriter writer = uploadSession.openRecordWriter(0);
    Record r = new ArrayRecord(schema);
    // 2.6MB in string
    String magicString = StringUtils.repeat("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 100000);
    r.setString(0, magicString);
    for (int i = 0; i < 20; ++i) {
      writer.write(r);
    }


    writer.close();
    uploadSession.commit(new Long[] {0L});
    Instance instance = SQLTask.run(odps, "select lower(id) as str from instance_tunnel_big;");
    System.err.println(instance.getId());
    instance.waitForSuccess();

    for (String st : instance.getTaskResults().values()) {
      System.err.println(st);
      assertTrue(st.contains("Fetched data is larger than the rendering limitation"));
    }

    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    InstanceTunnel.DownloadSession session =
        instanceTunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    assertEquals(session.getRecordCount(), 20);
    assertEquals(session.getSchema().getColumns().size(), 1L);
    assertEquals(session.getSchema().getColumns().get(0).getName(), "str");
    assertEquals(session.getSchema().getColumns().get(0).getType(), OdpsType.STRING);
    TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount());
    for (int i = 0; i < session.getRecordCount(); ++i) {
      Record record = reader.read();
      assertEquals(record.getString(0), magicString.toLowerCase());
    }

  }

  @Test
  public void testSelectEmptyTable() throws OdpsException {
    // Empty normal table
    odps.tables().delete("instance_tunnel_empty", true);
    Instance instance = SQLTask.run(odps, "create table instance_tunnel_empty(key string);");
    instance.waitForSuccess();
    Instance id = SQLTask.run(odps, "select * from instance_tunnel_empty;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    id.waitForSuccess();

    InstanceTunnel.DownloadSession session =
        instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    assertEquals(0, session.getRecordCount());
  }

  @Test
  public void testSelectEmptyPt() throws OdpsException, IOException {
    String emptyTable = "instance_tunnel_empty_pt";
    odps.tables().delete(emptyTable, true);
    OdpsTestUtils.createEmptyTableWithEmptyPartitionForTest(odps, emptyTable, "p1");
    Instance i = SQLTask.run(odps, "select * from " + emptyTable + " where p1='p1';");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    i.waitForSuccess();
    InstanceTunnel.DownloadSession session =
        instanceTunnel.createDownloadSession(odps.getDefaultProject(), i.getId());
    assertEquals(0, session.getRecordCount());
  }

  @Test
  public void testSelectPtTableValue() throws OdpsException, IOException {
    // direct download from partition table
    odps.tables().delete("instance_tunnel_pt", true);
    Instance instance =
        SQLTask
            .run(odps, "create table instance_tunnel_pt(key string) partitioned by (pt string);");
    instance.waitForSuccess();
    instance =
        SQLTask
            .run(
                odps,
                "insert overwrite table instance_tunnel_pt partition(pt='a') select 'a' from (select count(1) from instance_tunnel_pt) t;");
    instance.waitForSuccess();
    instance =
        SQLTask
            .run(odps,
                "insert overwrite table instance_tunnel_pt partition(pt='b') select 'b' from instance_tunnel_pt;");
    instance.waitForSuccess();

    instance = SQLTask.run(odps, "select * from instance_tunnel_pt;");
    instance.waitForSuccess();
    InstanceTunnel.DownloadSession session =
        tunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    assertEquals(session.getRecordCount(), 2L);
    TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount());
    Set<String> set = new HashSet<String>();
    for (Record record = reader.read(); record != null; record = reader.read()) {
      set.add(record.get(0).toString());
      assertEquals(true, record.get(1) != null);
    }
    assertEquals(2L, set.size());

    instance = SQLTask.run(odps, "select * from instance_tunnel_pt where pt='a';");
    instance.waitForSuccess();

    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    assertEquals(1, session.getRecordCount());
  }

  @Test
  public void testSelectEmptyPtTable() throws OdpsException {
    // Empty Partition Table
    odps.tables().delete("instance_tunnel_pt_empty", true);
    Instance instance =
        SQLTask.run(odps,
            "create table instance_tunnel_pt_empty(key string) partitioned by (pt string);");
    instance.waitForSuccess();
    instance = SQLTask.run(odps, "select * from instance_tunnel_pt_empty;");
    instance.waitForSuccess();
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    try {
      instanceTunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }


  @Test(expected = TunnelException.class)
  public void testNotExistsInstanceId() throws TunnelException {
    // not exist instance id
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    try {
      instanceTunnel.createDownloadSession(odps.getDefaultProject(), "Not Exists");
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test(expected = TunnelException.class)
  public void testNotExistsTable() throws OdpsException {
    // failed instance
    Instance instance = SQLTask.run(odps, "select * from not_exists;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    try {
      instance.waitForSuccess();
    } catch (OdpsException e) {
      // do nothing
    }
    try {
      instanceTunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test(expected = TunnelException.class)
  public void testDDLSQL() throws OdpsException {
    // ddl sql
    Instance instance = SQLTask.run(odps, "drop table if exists not_exists;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    instance.waitForSuccess();
    try {
      instanceTunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test(expected = TunnelException.class)
  public void testSQLCostTask() throws OdpsException {
    // sqlplantask
    Instance instance = SQLCostTask.run(odps, "select count(*) from instance_tunnel_test;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    try {
      instance.waitForSuccess();
      instanceTunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    } catch (OdpsException e) {
      e.printStackTrace();
      throw e;
    }
  }


  @Test(expected = TunnelException.class)
  public void testRunningInstance() throws OdpsException {
    // running instance
    Instance instance = SQLTask.run(odps, "select count(*) from instance_tunnel_test;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);

    try {
      instanceTunnel.createDownloadSession(odps.getDefaultProject(), instance.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public synchronized void testQueryTableInProtectionProject() throws OdpsException, IOException {
    protectionOdps.tables().delete("instance_tunnel_table", true);
    Instance protectionInstance =
        SQLTask
            .run(protectionOdps, "create table instance_tunnel_table(key string, value string);");
    protectionInstance.waitForSuccess();
    protectionInstance =
        SQLTask
            .run(
                protectionOdps,
                "insert into table instance_tunnel_table select count(*) key, count(*) value  from instance_tunnel_table;");
    protectionInstance.waitForSuccess();
    protectionInstance = SQLTask.run(protectionOdps, "select key from instance_tunnel_table;");
    protectionInstance.waitForSuccess();
    enableProtection(protectionOdps, protectionOdps.getDefaultProject(), null);
    try {
      protectionTunnel.createDownloadSession(protectionOdps.getDefaultProject(),
          protectionInstance.getId());
      throw new RuntimeException("should throw exception!!" + "instanceId:"
          + protectionInstance.getId());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(e instanceof TunnelException);
      TunnelException te = (TunnelException) e;
      assertTrue("NoPermission".equals(te.getErrorCode()));
      assertTrue(te
          .getErrorMsg()
          .contains(
              "project '"
                  + protectionOdps.getDefaultProject()
                  + "' is protected. You need to contact the project owner to set the exception policy for you"));
      if (te.getErrorMsg().contains("projects/" + protectionOdps.getDefaultProject() + "/tables/")) {
        System.out.println("add table exception");
        enableProtection(protectionOdps, protectionOdps.getDefaultProject(), "{\n"
            + "    \"Version\": \"1\",\n" + "    \"Statement\":\n" + "    [{\n"
            + "        \"Effect\":\"Allow\",\n"
            + "        \"Principal\":\"ALIYUN$odpstest1@aliyun.com\",\n"
            + "        \"Action\":[\"odps:Select\"],\n"
            + "        \"Resource\":\"acs:odps:*:projects/" + protectionOdps.getDefaultProject()
            + "/tables/instance_tunnel_table\",\n" + "        \"Condition\":{\n"
            + "            \"StringEquals\": {\n"
            + "                \"odps:TaskType\":[\"DT\", \"SQL\"]\n" + "            }\n"
            + "        }\n" + "    }]\n" + "    }");

      } else if (te.getErrorMsg().contains(
          "projects/" + protectionOdps.getDefaultProject() + "/instances/")) {
        System.out.println("add instance exception");
        enableProtection(protectionOdps, protectionOdps.getDefaultProject(), "{\n"
            + "    \"Version\": \"1\",\n" + "    \"Statement\":\n" + "    [{\n"
            + "        \"Effect\":\"Allow\",\n"
            + "        \"Principal\":\"ALIYUN$odpstest1@aliyun.com\",\n"
            + "        \"Action\":[\"odps:Read\"],\n"
            + "        \"Resource\":\"acs:odps:*:projects/" + protectionOdps.getDefaultProject()
            + "/instances/" + protectionInstance.getId() + "\",\n" + "        \"Condition\":{\n"
            + "            \"StringEquals\": {\n"
            + "                \"odps:TaskType\":[\"DT\", \"SQL\"]\n" + "            }\n"
            + "        }\n" + "    }]\n" + "    }");

      } else {
        throw new RuntimeException("errMsg is not expected:" + te.getErrorMsg());
      }

      DownloadSession protectionSession =
          protectionTunnel.createDownloadSession(protectionOdps.getDefaultProject(),
              protectionInstance.getId());
      System.out.println();
      TunnelRecordReader reader =
          protectionSession.openRecordReader(0, protectionSession.getRecordCount());
      Record r = null;
      while ((r = reader.read()) != null) {
        System.out.println(r.get(0));
      }
      // change user
      try {
        removeUser(protectionOdps.getDefaultProject(), "test_perf800@aliyun.com");
      } catch (Exception e2) {
      }
      addUser(protectionOdps.getDefaultProject(), "test_perf800@aliyun.com");
      Account account = new AliyunAccount("A6sLRxAhGuxmxIXO", "UmSTx4Uks1uUPkpy2MMPA98lIa4Vma");
      Odps o = new Odps(account);
      o.setDefaultProject(protectionOdps.getDefaultProject());
      o.setEndpoint(protectionOdps.getEndpoint());
      InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(o);
      try {
        instanceTunnel.new DownloadSession(protectionOdps.getDefaultProject(),
            protectionInstance.getId(), protectionSession.getId());
        throw new RuntimeException("should throw exception!!");
      } catch (Exception e1) {
        e1.printStackTrace();
        assertTrue(e1 instanceof TunnelException);
        te = (TunnelException) e1;
        System.out.println(te.getErrorMsg());
        assertTrue("NoPermission".equals(te.getErrorCode()));
        assertTrue(te.getErrorMsg().contains(
            "Only instance's owner can create, download and query InstanceTunnel's session."));
      }
    }
  }

  @Test
  public synchronized void testQueryViewInProtectionProject() throws OdpsException, IOException {
    protectionOdps.tables().delete("instance_tunnel_table", true);
    Instance instance =
        SQLTask
            .run(protectionOdps, "create table instance_tunnel_table(key string, value string);");
    instance.waitForSuccess();
    instance =
        SQLTask
            .run(
                protectionOdps,
                "insert into table instance_tunnel_table select count(*) key, count(*) value  from instance_tunnel_table;");
    instance.waitForSuccess();
    instance = SQLTask.run(protectionOdps, "drop view if exists instance_tunnel_view;");
    instance.waitForSuccess();
    instance =
        SQLTask
            .run(protectionOdps,
                "create view if not exists instance_tunnel_view (key) as select key from instance_tunnel_table;");
    instance.waitForSuccess();
    instance = SQLTask.run(protectionOdps, "select key from instance_tunnel_view;");
    instance.waitForSuccess();
    enableProtection(protectionOdps, protectionOdps.getDefaultProject(), null);
    try {
      protectionTunnel.createDownloadSession(protectionOdps.getDefaultProject(), instance.getId());
      throw new RuntimeException("should throw exception!!");
    } catch (Exception e) {
      assertTrue(e instanceof TunnelException);
      TunnelException te = (TunnelException) e;
      assertTrue("NoPermission".equals(te.getErrorCode()));
      assertTrue(te
          .getErrorMsg()
          .contains(
              "project '"
                  + protectionOdps.getDefaultProject()
                  + "' is protected. You need to contact the project owner to set the exception policy for you"));
      if (te.getErrorMsg().contains("projects/" + protectionOdps.getDefaultProject() + "/tables/")) {
        System.out.println("add table exception");
        enableProtection(protectionOdps, protectionOdps.getDefaultProject(), "{\n"
            + "    \"Version\": \"1\",\n" + "    \"Statement\":\n" + "    [{\n"
            + "        \"Effect\":\"Allow\",\n"
            + "        \"Principal\":\"ALIYUN$odpstest1@aliyun.com\",\n"
            + "        \"Action\":[\"odps:Select\"],\n"
            + "        \"Resource\":\"acs:odps:*:projects/" + protectionOdps.getDefaultProject()
            + "/tables/instance_tunnel_view\",\n" + "        \"Condition\":{\n"
            + "            \"StringEquals\": {\n"
            + "                \"odps:TaskType\":[\"DT\", \"SQL\"]\n" + "            }\n"
            + "        }\n" + "    }]\n" + "    }");

      } else if (te.getErrorMsg().contains(
          "projects/" + protectionOdps.getDefaultProject() + "/instances/")) {
        System.out.println("add instance exception");
        enableProtection(protectionOdps, protectionOdps.getDefaultProject(), "{\n"
            + "    \"Version\": \"1\",\n" + "    \"Statement\":\n" + "    [{\n"
            + "        \"Effect\":\"Allow\",\n"
            + "        \"Principal\":\"ALIYUN$odpstest1@aliyun.com\",\n"
            + "        \"Action\":[\"odps:Read\"],\n"
            + "        \"Resource\":\"acs:odps:*:projects/" + protectionOdps.getDefaultProject()
            + "/instances/" + instance.getId() + "\",\n" + "        \"Condition\":{\n"
            + "            \"StringEquals\": {\n"
            + "                \"odps:TaskType\":[\"DT\", \"SQL\"]\n" + "            }\n"
            + "        }\n" + "    }]\n" + "    }");

      } else {
        throw new RuntimeException("errMsg is not expected:" + te.getErrorMsg());
      }
      DownloadSession session =
          protectionTunnel.createDownloadSession(protectionOdps.getDefaultProject(),
              instance.getId());
      System.out.println();
      TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount());
      Record r = null;
      while ((r = reader.read()) != null) {
        System.out.println(r.get(0));
      }
      // change user
      try {
        removeUser(protectionOdps.getDefaultProject(), "test_perf800@aliyun.com");
      } catch (Exception e2) {
      }
      addUser(protectionOdps.getDefaultProject(), "test_perf800@aliyun.com");
      Account account = new AliyunAccount("A6sLRxAhGuxmxIXO", "UmSTx4Uks1uUPkpy2MMPA98lIa4Vma");
      Odps o = new Odps(account);
      o.setDefaultProject(protectionOdps.getDefaultProject());
      o.setEndpoint(protectionOdps.getEndpoint());
      InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(o);
      try {
        instanceTunnel.new DownloadSession(protectionOdps.getDefaultProject(), instance.getId(),
            session.getId());
        throw new RuntimeException("should throw exception!!");
      } catch (Exception e1) {
        assertTrue(e1 instanceof TunnelException);
        te = (TunnelException) e1;
        System.out.println(te.getErrorMsg());
        assertTrue("NoPermission".equals(te.getErrorCode()));
        assertTrue(te.getErrorMsg().contains(
            "Only instance's owner can create, download and query InstanceTunnel's session."));
      }
    }
  }

  private static void addUser(String project, String user) throws OdpsException {
    SecurityManager securityManager = odps.projects().get(project).getSecurityManager();
    securityManager.runQuery("add user " + user, false);
  }

  private static void removeUser(String project, String user) throws OdpsException {
    SecurityManager securityManager = odps.projects().get(project).getSecurityManager();
    securityManager.runQuery("remove user " + user, false);
  }

  private static void enableProtection(Odps odps, String project, String policy)
      throws OdpsException {
    SecurityManager securityManager = odps.projects().get(project).getSecurityManager();
    SecurityConfiguration securityConfig = securityManager.getSecurityConfiguration();
    securityConfig.enableProjectProtection(policy);
    securityManager.setSecurityConfiguration(securityConfig);
    // wait cache expired
    try {
      Thread.sleep(70 * 1000);
    } catch (InterruptedException e) {
    }
  }

  private static void disableProtection(Odps odps, String project) throws OdpsException {
    SecurityManager securityManager = odps.projects().get(project).getSecurityManager();
    SecurityConfiguration securityConfig = securityManager.getSecurityConfiguration();
    securityConfig.disableProjectProtection();
    securityManager.setSecurityConfiguration(securityConfig);
    // wait cache expired
    try {
      Thread.sleep(70 * 1000);
    } catch (InterruptedException e) {
    }
  }

  @Test
  public void testNoRouteToHost() {
    InstanceTunnel tunnel = OdpsTestUtils.newInstanceTunnel(odps);
    String endpoint = "http://not.exists.url";
    tunnel.setEndpoint(endpoint);
    try {
      tunnel.createDownloadSession("what", "ever");
    } catch (TunnelException e) {
      e.printStackTrace();
      Assert.assertTrue(e.getMessage().contains(endpoint));
    }
  }

}
