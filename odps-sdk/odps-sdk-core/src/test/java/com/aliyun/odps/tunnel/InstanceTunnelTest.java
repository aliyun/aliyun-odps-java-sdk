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

package com.aliyun.odps.tunnel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.task.SQLCostTask;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;


/**
 * Created by nizheming on 15/12/15.
 */
public class InstanceTunnelTest extends TestBase {

  private static InstanceTunnel tunnel;

  @BeforeClass
  public static void testInstanceTunnel() throws OdpsException, IOException {
    odps.tables().delete("instance_tunnel_test", true);
    tunnel = OdpsTestUtils.newInstanceTunnel(odps);
    OdpsTestUtils.createTableForTest("instance_tunnel_test");

  }

  @Test
  public void testPositive() throws OdpsException, IOException {
    //normal case
    Instance id = SQLTask.run(odps, "select count(*) as cnt from instance_tunnel_test;");
    System.err.println(id.getId());
    id.waitForSuccess();
    InstanceTunnel.DownloadSession session = tunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
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
  public void testPositive1() throws OdpsException, IOException {
    //direct read table
    Instance id = SQLTask.run(odps, "select * from instance_tunnel_test;");
    System.err.println(id.getId());
    id.waitForSuccess();
    InstanceTunnel.DownloadSession session = tunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    assertTrue(session.getRecordCount() > 0L);
    assertEquals(session.getSchema().getColumns().size(), 1L);
    assertEquals(session.getSchema().getColumns().get(0).getName(), "c1");
    assertEquals(session.getSchema().getColumns().get(0).getType(), OdpsType.BIGINT);
    TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount());
    for (Record record = reader.read(); record != null; record = reader.read()) {
      System.err.println(record.get(0).toString());
    }

    //test compress
    reader = session.openRecordReader(0, session.getRecordCount(), true);
    for (Record record = reader.read(); record != null; record = reader.read()) {
      System.err.println(record.get(0).toString());
    }
  }

  @Ignore("limit operator not work yet")
  @Test
  public void testPositive2() throws OdpsException, IOException {
    //limit operator
    Instance id = SQLTask.run(odps, "select * from instance_tunnel_test limit 2;");
    System.err.println(id.getId());
    id.waitForSuccess();
    InstanceTunnel.DownloadSession session = tunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
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
  public void testPositive3() throws OdpsException, IOException {
    //order by must has order in tunnel download
    Instance id = SQLTask.run(odps, "select row_number() over (partition by 1 order by rand()) as id from instance_tunnel_test order by id limit 25;");
    System.err.println(id.getId());
    id.waitForSuccess();
    InstanceTunnel.DownloadSession session = tunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
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
  public void testPositive4() throws OdpsException, IOException {
    //big data table
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("id", OdpsType.STRING));
    odps.tables().delete("instance_tunnel_big", true);

    odps.tables().create("instance_tunnel_big", schema);
    TableTunnel tunnel = OdpsTestUtils.newTableTunnel(odps);
    TableTunnel.UploadSession uploadSession = tunnel.createUploadSession(odps.getDefaultProject(), "instance_tunnel_big");
    RecordWriter writer = uploadSession.openRecordWriter(0);
    Record r = new ArrayRecord(schema);
    //2.6MB in string
    String magicString = StringUtils.repeat("ABCDEFGHIJKLMNOPQRSTUVWXYZ",100000);
    r.setString(0, magicString);
    for (int i = 0; i<20; ++i) {
      writer.write(r);
    }


    writer.close();
    uploadSession.commit(new Long[]{0L});

    Instance id = SQLTask.run(odps, "select lower(id) as str from instance_tunnel_big;");
    System.err.println(id.getId());
    id.waitForSuccess();

    for (String st : id.getTaskResults().values()) {
      System.err.println(st);
      assertTrue(st.contains("Fetched data is larger than the rendering limitation"));
    }

    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    InstanceTunnel.DownloadSession session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
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
  public void testPositive5() throws OdpsException {
    //Empty normal table
    odps.tables().delete("instance_tunnel_empty", true);
    Instance instance = SQLTask.run(odps, "create table instance_tunnel_empty(key string);");
    instance.waitForSuccess();
    Instance id = SQLTask.run(odps, "select * from instance_tunnel_empty;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    id.waitForSuccess();

    InstanceTunnel.DownloadSession session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    assertEquals(0, session.getRecordCount());
  }

  @Test
  public void testPositive6() throws OdpsException {
    //direct download from partition table
    odps.tables().delete("instance_tunnel_pt", true);
    Instance instance = SQLTask.run(odps, "create table instance_tunnel_pt(key string) partitioned by (pt string);");
    instance.waitForSuccess();
    instance = SQLTask.run(odps, "insert overwrite table instance_tunnel_pt partition(pt='a') select 'a' from (select count(1) from instance_tunnel_pt) t;");
    instance.waitForSuccess();
    instance = SQLTask.run(odps, "insert overwrite table instance_tunnel_pt partition(pt='b') select 'a' from instance_tunnel_pt;");
    instance.waitForSuccess();

    Instance id = SQLTask.run(odps, "select * from instance_tunnel_pt;");
    id.waitForSuccess();
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);

    InstanceTunnel.DownloadSession session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    assertEquals(2, session.getRecordCount());

    id = SQLTask.run(odps, "select * from instance_tunnel_pt where pt='a';");
    id.waitForSuccess();
    instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);

    session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    assertEquals(1, session.getRecordCount());
  }

  @Test(expected = TunnelException.class)
  public void testNegative1() throws TunnelException {
    // not exist instance id
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    try {
      InstanceTunnel.DownloadSession session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), "Not Exists");
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }


  @Test(expected = TunnelException.class)
  public void testNegative2() throws OdpsException {
    //Empty Partition Table
    odps.tables().delete("instance_tunnel_pt_empty", true);
    Instance instance = SQLTask.run(odps, "create table instance_tunnel_pt_empty(key string) partitioned by (pt string);");
    instance.waitForSuccess();
    Instance id = SQLTask.run(odps, "select * from instance_tunnel_pt_empty;");
    id.waitForSuccess();
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    try {
      InstanceTunnel.DownloadSession session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }


  @Test(expected = TunnelException.class)
  public void testNegative3() throws OdpsException {
    //failed instance
    Instance id = SQLTask.run(odps, "select * from not_exists;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    try {
      id.waitForSuccess();
    } catch (OdpsException e) {
      //do nothing
    }
    try {
      InstanceTunnel.DownloadSession session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test(expected = TunnelException.class)
  public void testNegative4() throws OdpsException {
    //ddl sql
    Instance id = SQLTask.run(odps, "drop table if exists not_exists;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    id.waitForSuccess();
    try {
      InstanceTunnel.DownloadSession session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test(expected = TunnelException.class)
  public void testNegative5() throws OdpsException {
    //sqlplantask
    Instance id = SQLCostTask.run(odps, "select count(*) from instance_tunnel_test;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);
    try {
      id.waitForSuccess();
      InstanceTunnel.DownloadSession session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    } catch (OdpsException e) {
      e.printStackTrace();
      throw e;
    }
  }


  @Test(expected = TunnelException.class)
  public void testNegative6() throws OdpsException {
    //running instance
    Instance id = SQLTask.run(odps, "select count(*) from instance_tunnel_test;");
    InstanceTunnel instanceTunnel = OdpsTestUtils.newInstanceTunnel(odps);

    try {
      InstanceTunnel.DownloadSession session = instanceTunnel.createDownloadSession(odps.getDefaultProject(), id.getId());
    } catch (TunnelException e) {
      e.printStackTrace();
      throw e;
    }
  }
}
