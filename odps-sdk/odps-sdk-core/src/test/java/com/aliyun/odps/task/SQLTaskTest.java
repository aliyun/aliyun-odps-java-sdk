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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.FallbackPolicy;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.util.Utils;

/**
 * Created by nizheming on 15/4/24.
 */
public class SQLTaskTest extends TestBase {

  private static SQLExecutor sqlExecutor;

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
    Map<String, String> properties = new HashMap<>();
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
    builder.odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .properties(properties)
        .fallbackPolicy(FallbackPolicy.nonFallbackPolicy());
    sqlExecutor = builder.build();
  }

  @Test
  public void testCreatePriority() throws OdpsException {
    Instance
        i =
        SQLTask.run(odps, odps.getDefaultProject(), "select * from src;", "testsql", null, null, 3);
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
    Instance
        i =
        SQLTask.run(odps, odps.getDefaultProject(), "select * from test_select_sql_result;",
                    taskName, null, null, 3);
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

    OdpsTestUtils.createTableForTest(odps, "test_select_sql_test_from_tunnel");

    String taskName = "test_select_sql_task_tunnel";
    Instance
        i =
        SQLTask.run(odps, odps.getDefaultProject(),
                    "select * from test_select_sql_test_from_tunnel;", taskName, null, null);
    i.waitForSuccess();
    System.out.println(i.getId());
    List<Record> records = SQLTask.getResultByInstanceTunnel(i, taskName);
    // get all res
    assertEquals(21, records.size());

    // limit param less than result record number
    assertEquals(4, SQLTask.getResultByInstanceTunnel(i, taskName, 4L).size());

    // limit 3 in sql query
    i =
        SQLTask.run(odps, odps.getDefaultProject(),
                    "select * from test_select_sql_test_from_tunnel limit 3;", null, null);
    i.waitForSuccess();
    records = SQLTask.getResultByInstanceTunnel(i);
    assertEquals(3, records.size());

    // AnonymousSQLTask
    assertEquals(2, SQLTask.getResultByInstanceTunnel(i, 2L).size());
    assertEquals(3, SQLTask.getResultByInstanceTunnel(i, 5L).size());

    String
        tableName =
        "test_select_huge_sql_test_from_tunnel_in_SQLTaskTest_testInstanceTunnelResult";
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
    while (rs.hasNext()) {
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
    Instance
        i =
        SQLTask.run(odps, odps.getDefaultProject(),
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

    Instance
        i =
        SQLTask.run(odps, odps.getDefaultProject(), "select 1 +'1' from src;", hints, null);
    List<String> res = SQLTask.getSqlWarning(i);
//    Assert.assertNotNull(res);
    if (res != null) {
      for (String r : res) {
        System.out.println(r);
      }
    }

  }

  @Test
  public void testGetResultSchema() throws OdpsException {
    String query = "select 1;";
    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.type.system.odps2", "true");
    Instance i = SQLTask.run(odps, odps.getDefaultProject(), query, hints, null);

    TableSchema schema = SQLTask.getResultSchema(i);
    Assert.assertEquals(1, schema.getColumns().size());
    Column column = schema.getColumn(0);
    Assert.assertEquals(OdpsType.INT, column.getTypeInfo().getOdpsType());
  }

  @Test
  public void testGetResultSchemaWithSettings() throws OdpsException {
    String query = "select 1;";
    Map<String, String> hints = new HashMap<>();
    // In hive compatible mode, the integer constant type will be INT, instead of BIGINT
    hints.put("odps.sql.hive.compatible", "true");
    Instance i = SQLTask.run(odps, odps.getDefaultProject(), query, hints, null);

    TableSchema schema = SQLTask.getResultSchema(i);
    Assert.assertEquals(1, schema.getColumns().size());
    Column column = schema.getColumn(0);
    Assert.assertEquals(OdpsType.INT, column.getTypeInfo().getOdpsType());
  }

  @Test
  public void testGetResultSchemaFromDDL() throws OdpsException {
    try {
      String ddl = "create table if not exists testGetResultSchemaFromDDL(col1 string);";
      Instance i = SQLTask.run(odps, ddl);
      i.waitForSuccess();
      try {
        SQLTask.getResultSchema(i);
        Assert.fail();
      } catch (OdpsException e) {
        Assert.assertTrue(e.getMessage().contains("DDL not supported in LINEAGE analyze"));
      }
    } finally {
      odps.tables().delete("testGetResultSchemaFromDDL", true);
    }
  }

  @Test
  public void testGetResultSchemaFromDML() throws OdpsException {
    try {
      Instance
          i =
          SQLTask.run(odps, "create table if not exists testGetResultSchemaFromDML(col1 string);");
      i.waitForSuccess();
      i = SQLTask.run(odps, "insert into testGetResultSchemaFromDML values('foo');");
      i.waitForSuccess();
      try {
        SQLTask.getResultSchema(i);
        Assert.fail();
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("Not a query"));
      }
    } finally {
      odps.tables().delete("testGetResultSchemaFromDDL", true);
    }
  }

  @Test
  public void testGetRawResult() throws OdpsException {
    String tableName = "SQLTask_GetRawResult_Test";
    String dropTableSql = "drop table if exists " + tableName + ";";
    String createTableSql = "create table " + tableName + " (key string, value bigint);";
    String sql = "show create table " + tableName + ";";

    SQLTask.run(odps, dropTableSql).waitForSuccess();
    SQLTask.run(odps, createTableSql).waitForSuccess();

    Instance instance = SQLTask.run(odps, sql);
    instance.waitForSuccess();

    String res = SQLTask.getRawResult(instance);
    System.out.println(res);
    Assert.assertNotNull(res);

    String taskName = "SQLTASK_GETRAWRESULT";
    instance = SQLTask.run(odps, odps.getDefaultProject(), sql, taskName, null, null);
    instance.waitForSuccess();

    res = SQLTask.getRawResult(instance, taskName);
    System.out.println(res);
    Assert.assertNotNull(res);

  }
  /** test acid2 table schema evolution, copy from SchemaEvolutionTest */

  @Test
  public void download_normal() throws IOException, OdpsException {
    String tempTable = "test_schema_evolution_download_normal";
    dropTableIfExists(tempTable);

    executeSQL("create table " + tempTable
               + " (id bigint not null, primary key(id)) tblproperties (\"transactional\"=\"true\");");
    // upload some data
    executeSQL("insert into table " + tempTable + " values (1);");
    // evolve the schema
    executeSQL("alter table " + tempTable + " add columns (value string);");
    // upload some data
    executeSQL("insert into table " + tempTable + " values (2, \"hello world!\");");

    List<Record> records = readTable(tempTable);
    check_normal_result(records.get(0));
    check_normal_result(records.get(1));
    dropTableIfExists(tempTable);
  }

  private void check_normal_result(Record buffer) {
    if (buffer.getBigint(0).intValue() == 1) {
      Assert.assertEquals(true, buffer.isNull(1));
    } else {
      Assert.assertEquals(2, buffer.getBigint(0).intValue());
      Assert.assertEquals("hello world!", buffer.getString(1));
    }
  }

  @Test
  public void download_nested() throws IOException, OdpsException {
    String tempTable = "test_schema_evolution_download_nested";
    dropTableIfExists(tempTable);
    executeSQL("create table " + tempTable
               + " (key int not null, point struct<x: double, y:double> not null, primary key(key)) tblproperties (\"transactional\"=\"true\");");
    // upsert some data
    executeSQL("insert into table " + tempTable + " values (1, named_struct('x', 1.0, 'y', 2.0));");
    // evolve the schema
    executeSQL("alter table " + tempTable + " add columns (point.z double);");
    // upsert some data
    executeSQL("insert into table " + tempTable
               + " values (2, named_struct('x', 3.0, 'y', 4.0, 'z', 5.0));");
    // download
    List<Record> records = readTable(tempTable);
    check_nested_result((ArrayRecord) records.get(0));
    check_nested_result((ArrayRecord) records.get(1));
    dropTableIfExists(tempTable);
  }

  private void check_nested_result(ArrayRecord buffer) {
    if (buffer.getStruct(1).getFieldValue("x").equals(3.0)) {
      Assert.assertEquals(3.0, buffer.getStruct(1).getFieldValue("x"));
      Assert.assertEquals(4.0, buffer.getStruct(1).getFieldValue("y"));
      Assert.assertEquals(5.0, buffer.getStruct(1).getFieldValue("z"));
    } else {
      Assert.assertEquals(1.0, buffer.getStruct(1).getFieldValue("x"));
      Assert.assertEquals(2.0, buffer.getStruct(1).getFieldValue("y"));
      Assert.assertNull(buffer.getStruct(1).getFieldValue("z"));
    }
  }

  @Test
  public void download_reordered() throws IOException, OdpsException {
    String tempTable = "test_schema_evolution_download_reordered";
    dropTableIfExists(tempTable);
    executeSQL("create table " + tempTable
               + " (id int, value string not null, value2 double, primary key(value)) tblproperties (\"transactional\"=\"true\");");
    // upsert some data
    executeSQL("insert into table " + tempTable + " values (1, 'hello', 2.0);");
    executeSQL("insert into table " + tempTable + " values (1, 'hello', 2.0);");
    // evolve the schema
    executeSQL("alter table " + tempTable + " change column id id2 int after value;");
    // upsert some data
    executeSQL("insert into table " + tempTable + " values ('world', 2, 4.0);");
    // download
    List<Record> records = readTable(tempTable);
    check_reordered_result(records.get(0));
    check_reordered_result(records.get(1));
    dropTableIfExists(tempTable);
  }

  private void check_reordered_result(Record buffer) {
    if (buffer.getBigint(1).intValue() == 1) {
      Assert.assertEquals("hello", buffer.getString(0));
      Assert.assertEquals(1, buffer.getBigint(1).intValue());
      Assert.assertEquals(2.0, buffer.getDouble(2), 0.0);
    } else {
      Assert.assertEquals("world", buffer.getString(0));
      Assert.assertEquals(2, buffer.getBigint(1).intValue());
      Assert.assertEquals(4.0, buffer.getDouble(2), 0.0);
    }
  }

  @Test
  public void download_deleted() throws IOException, OdpsException {
    String tempTable = "test_schema_evolution_download_deleted";
    dropTableIfExists(tempTable);
    executeSQL("create table " + tempTable
               + " (id string not null, value string, value2 double, primary key(id)) tblproperties (\"transactional\"=\"true\");");
    // upsert some data
    executeSQL("insert into table " + tempTable + " values ('hello', 'hello', 2.0);");
    // evolve the schema
    executeSQL("alter table " + tempTable + " drop columns(value, value2);");
    // upsert some data
    executeSQL("insert into table " + tempTable + " values ('world');");
    // download
    List<Record> records = readTable(tempTable);
    check_deleted_result(records.get(0));
    check_deleted_result(records.get(1));
    dropTableIfExists(tempTable);
  }

  private void check_deleted_result(Record buffer) {
    if (buffer.getString(0).equals("hello")) {
    } else {
      Assert.assertEquals("world", buffer.getString(0));
    }
  }

  private List<Record> readTable(String tableName) throws OdpsException, IOException {
    String sql = "select * from " + odps.getDefaultProject() + "." + tableName + ";";
    sqlExecutor.run(sql, null);
    return sqlExecutor.getResult();
  }

  private static void dropTableIfExists(String tableName) throws OdpsException {
    executeSQL("drop table if exists " + tableName + ";");
  }

  private static void executeSQL(String sql) throws OdpsException {
    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.allow.schema.evolution", "true");
    hints.put("odps.sql.decimal.odps2", "true");
    hints.put("odps.sql.upsertable.table.enable", "true");
    System.out.println("Execute SQL => " + sql);
    SQLTask.run(odps, odps.getDefaultProject(), sql, "SqlTask", hints, null).waitForSuccess();
  }

}
