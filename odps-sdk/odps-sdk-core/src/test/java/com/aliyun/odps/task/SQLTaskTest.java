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

package com.aliyun.odps.task;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * Created by nizheming on 15/4/24.
 */
public class SQLTaskTest extends TestBase {
  
  @BeforeClass
  public static void testInstanceTunnel() throws OdpsException, IOException {
//    Map<String, String> hints = new HashMap<String, String>();
//    hints.put("odps.sql.preparse.odps2", "true");
//    hints.put("odps.sql.planner.parser.odps2", "true");
//    hints.put("odps.sql.planner.mode", "lot");
//    hints.put("odps.compiler.output.format", "lot,pot");
//    hints.put("odps.compiler.playback", "true");
//    hints.put("odps.compiler.warning.disable", "false");
//    hints.put("odps.sql.ddl.odps2", "true");
//    hints.put("odps.sql.runtime.mode", "EXECUTIONENGINE");
//    hints.put("odps.sql.sqltask.new", "true");
//    SQLTask.setDefaultHints(hints);
  }

  @Test
  public void testCreatePriority() throws OdpsException {
    Instance
        i =
        SQLTask.run(odps, odps.getDefaultProject(), "select * from src;", "testsql", null, null,
                    3);
    assertEquals(i.getPriority(), 3);
  }

  @Test
  public void testSelectSQLTask() throws OdpsException, IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    try {
      odps.tables().create("test_select_sql_result", schema);
    } catch (OdpsException e) {
    }

    String taskName = "test_select_sql_task";
    Instance i = SQLTask.run(odps, odps.getDefaultProject(),
                             "select * from test_select_sql_result;", taskName, null, null, 3);
    i.waitForSuccess();
    List<Record> records = SQLTask.getResult(i, taskName);
    assertEquals(0, records.size());

    i =
        SQLTask.run(odps, odps.getDefaultProject(), "select * from test_select_sql_result;", null,
                    null);
    i.waitForSuccess();
    records = SQLTask.getResult(i);
    assertEquals(0, records.size());
  }

  @Test
  public void testInstanceTunnelResult() throws OdpsException, IOException {
    odps.tables().delete("test_select_sql_test_from_tunnel", true);

    OdpsTestUtils.createTableForTest("test_select_sql_test_from_tunnel");

    String taskName = "test_select_sql_task_tunnel";
    Instance i = SQLTask.run(odps, odps.getDefaultProject(),
                             "select * from test_select_sql_test_from_tunnel;", taskName, null,
                             null);
    i.waitForSuccess();
    System.out.println(i.getId());
    List<Record> records = SQLTask.getResultByInstanceTunnel(i, taskName);
    // get all res
    assertEquals(21, records.size());

    // limit param less than result record number
    assertEquals(4, SQLTask.getResultByInstanceTunnel(i, taskName, 4L).size());

    // limit 3 in sql query
    i = SQLTask.run(odps, odps.getDefaultProject(),
                    "select * from test_select_sql_test_from_tunnel limit 3;", null, null);
    i.waitForSuccess();
    records = SQLTask.getResultByInstanceTunnel(i);
    assertEquals(3, records.size());

    // AnonymousSQLTask
    assertEquals(2, SQLTask.getResultByInstanceTunnel(i, 2L).size());
    assertEquals(3, SQLTask.getResultByInstanceTunnel(i, 5L).size());

    String tableName = "test_select_huge_sql_test_from_tunnel";
    odps.tables().delete(tableName, true);
    OdpsTestUtils.createBigTableForTest(tableName);// table has 10010 records

    i = SQLTask.run(odps, odps.getDefaultProject(), "select * from " + tableName + ";", null, null);
    i.waitForSuccess();
    
    records = SQLTask.getResult(i);
    assertEquals(10000, records.size());
    
    assertEquals(OdpsType.STRING, records.get(0).getColumns()[0].getType());
    
    records = SQLTask.getResultByInstanceTunnel(i);
    assertEquals(10000, records.size());
    
    assertEquals(OdpsType.BIGINT, records.get(0).getColumns()[0].getType());

    ResultSet rs = SQLTask.getResultSet(i);
    records = new ArrayList<Record>();
    for (Record r : rs) {
      records.add(r);
    }

    assertEquals(OdpsType.BIGINT, records.get(0).getColumns()[0].getType());
    assertEquals(OdpsType.BIGINT, rs.getTableSchema().getColumn(0).getType());

    assertEquals(10010, records.size());
    assertEquals(1l, records.get(1).get(0));
    assertEquals(5000l, records.get(5000).get(0));
    assertEquals(10008l, records.get(10008).get(0));
    assertEquals(10009l, records.get(10009).get(0));

    rs = SQLTask.getResultSet(i, 10003L);
    records = new ArrayList<Record>();
    for (Record r : rs) {
      records.add(r);
    }
    assertEquals(10003, records.size());
    
    rs = SQLTask.getResultSet(i, 10003L);
    records = new ArrayList<Record>();
    while(rs.hasNext()){
      records.add(rs.next());
    }
    assertEquals(10003, records.size());


    rs = SQLTask.getResultSet(i, 10011L);
    records = new ArrayList<Record>();
    for (Record r : rs) {
      records.add(r);
    }
    assertEquals(10010, records.size());

    rs = SQLTask.getResultSet(i, 10L);
    records = new ArrayList<Record>();
    for (Record r : rs) {
      records.add(r);
    }
    assertEquals(10, records.size());
  }

  @Test(expected = TunnelException.class)
  public void testInstanceTunnelResultNeg() throws OdpsException, IOException {
    String taskName = "test_select_sql_task_tunnel_neg";
    Instance i = SQLTask.run(odps, odps.getDefaultProject(),
                             "create table if not exists  test_select_sql_test_from_tunnel (name string) ;",
                             taskName, null, null);
    i.waitForSuccess();
    SQLTask.getResultByInstanceTunnel(i, taskName);
  }

  @Test
  public void testGetSqlWarning() throws OdpsException {
    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.preparse.odps2", "lot");
    hints.put("odps.sql.planner.parser.odps2", "true");
    hints.put("odps.sql.planner.mode", "lot");
    hints.put("odps.compiler.warning.disable", "false");

    Instance i = SQLTask.run(odps, odps.getDefaultProject(), "select 1 +'1' from src;", hints, null);
    List<String> res = SQLTask.getSqlWarning(i);
//    Assert.assertNotNull(res);
    if (res != null) {
      for (String r : res) {
        System.out.println(r);
      }
    }

  }
}
