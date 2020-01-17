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

package com.aliyun.odps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.type.TypeInfoFactory;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class TablesTest extends TestBase {

  private String tableName = this.getClass().getSimpleName() + "_table_name_for_testTables";
  private TableSchema schema;

  @Before
  public void setUp() throws OdpsException {
    odps = OdpsTestUtils.newDefaultOdps();

    schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addColumn(new Column("c2", OdpsType.BOOLEAN));
    schema.addColumn(new Column("c3", OdpsType.DATETIME));
    schema.addColumn(new Column("c4", OdpsType.STRING));

    schema.addPartitionColumn(new Column("p1", OdpsType.BIGINT));
    schema.addPartitionColumn(new Column("p2", OdpsType.STRING));

    createTable(tableName);
  }

  private void createTable(String name) throws OdpsException {
    deleteTable(name);

    odps.tables().create(odps.getDefaultProject(), name, schema);
  }

  private void deleteTable(String name) throws OdpsException {
    if (odps.tables().exists(name)) {
      odps.tables().delete(name);
    }
  }

  @After
  public void tearDown() throws OdpsException {
    deleteTable(tableName);
  }

  @Test
  public void testCreateTable() throws OdpsException {
    UUID id = UUID.randomUUID();
    String tableName = id.toString().replace("-", "");
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addColumn(new Column("_c2", OdpsType.STRING, "_comment here"));
    schema.addPartitionColumn(new Column("p1", OdpsType.STRING));
    schema.addPartitionColumn(new Column("_p2", OdpsType.STRING, "_comment here"));
    odps.tables()
        .create(odps.getDefaultProject(), "testCreateTable" + tableName, schema, "_table comment",
                false);
    Table table = odps.tables().get("testCreateTable" + tableName);
    assertEquals(table.getComment(), "_table comment");
    TableSchema returnSchema = table.getSchema();
    assertEquals(returnSchema.getColumns().size(), 2L);
    assertEquals(returnSchema.getColumn("c1").getName(), "c1");
    assertEquals(returnSchema.getColumn("c1").getType(), OdpsType.BIGINT);
    assertEquals(returnSchema.getColumn("c1").getComment(), "");
    assertEquals(returnSchema.getColumn("_c2").getName(), "_c2");
    assertEquals(returnSchema.getColumn("_c2").getType(), OdpsType.STRING);
    assertEquals(returnSchema.getColumn("_c2").getComment(), "_comment here");

    assertEquals(returnSchema.getPartitionColumn("p1").getName(), "p1");
    assertEquals(returnSchema.getPartitionColumn("p1").getType(), OdpsType.STRING);
    assertEquals(returnSchema.getPartitionColumn("p1").getComment(), "");
    assertEquals(returnSchema.getPartitionColumn("_p2").getName(), "_p2");
    assertEquals(returnSchema.getPartitionColumn("_p2").getType(), OdpsType.STRING);
    assertEquals(returnSchema.getPartitionColumn("_p2").getComment(), "_comment here");
  }

  @Test
  public void testCreateTableWithLifeCycle() throws OdpsException {
    UUID id = UUID.randomUUID();
    String tableName = "testCreateTableWithLifeCycle" + id.toString().replace("-", "");
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addColumn(new Column("_c2", OdpsType.STRING, "_comment here"));
    schema.addPartitionColumn(new Column("p1", OdpsType.STRING));
    schema.addPartitionColumn(new Column("_p2", OdpsType.STRING, "_comment here"));
    odps.tables()
        .createTableWithLifeCycle(odps.getDefaultProject(), tableName, schema, null, false, 10L);
    assertEquals(odps.tables().get(tableName).getLife(), 10L);
    deleteTable(tableName);
  }

  @Test
  public void testCreateExternalTable() throws OdpsException {
    UUID id = UUID.randomUUID();
    String tableName = "testCreateExternalTable" + id.toString().replace("-", "");
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addColumn(new Column("_c2", OdpsType.STRING, "_comment here"));
    schema.addPartitionColumn(new Column("p1", OdpsType.STRING));
    schema.addPartitionColumn(new Column("_p2", OdpsType.STRING, "_comment here"));
    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.preparse.odps2", "lot");
    hints.put("odps.sql.planner.mode", "lot");
    hints.put("odps.sql.planner.parser.odps2", "true");
    hints.put("odps.sql.ddl.odps2", "true");
    hints.put("odps.compiler.output.format", "lot,pot");

    String storageHandler = "com.aliyun.odps.CsvStorageHandler";

    odps.tables()
        .createExternal(odps.getDefaultProject(), tableName, schema,
            "MOCKoss://full/uri/path/to/oss/directory/",
            storageHandler, null, null,
            "External table with partitions and builtin CsvStorageHandler", false, 10L, hints, null);
    Table createdTable = odps.tables().get(tableName);
    assertEquals(createdTable.isExternalTable(), true);
    assertEquals(createdTable.getStorageHandler(), storageHandler);
    assertEquals(createdTable.getLife(), 10L);
    deleteTable(tableName);

    schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addColumn(new Column("_c2", OdpsType.STRING));

    storageHandler = "com.aliyun.odps.udf.example.text.TextStorageHandler";
    List<String > jars = new ArrayList<String>();
    jars.add("odps-udf-example.jar");
    jars.add("another.jar");
    Map<String,String> properties = new HashMap<String, String>();
    properties.put("odps.text.option.delimiter", "|");
    properties.put("my.own.option", "value");
    odps.tables()
        .createExternal(odps.getDefaultProject(), tableName, schema,
            "MOCKoss://full/uri/path/to/oss/directory/",
            storageHandler, jars, properties,
            "External table using user defined TextStorageHandler", true, null, hints, null);
    createdTable = odps.tables().get(tableName);
    assertEquals(createdTable.isExternalTable(), true);
    assertEquals(createdTable.getStorageHandler(), storageHandler);
    assertEquals(createdTable.getResources().split(",").length, 2);
    deleteTable(tableName);
  }

  @Test
  public void testCreateTableWithHints() throws OdpsException {
    UUID id = UUID.randomUUID();
    String tableName = "testCreateTableWithHints" + id.toString().replace("-", "");
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", TypeInfoFactory.getCharTypeInfo(10)));
    schema.addColumn(new Column("_c2", TypeInfoFactory.STRING, "_comment here"));
    schema.addPartitionColumn(new Column("p1", TypeInfoFactory.getVarcharTypeInfo(20)));
    schema.addColumn(new Column("_p2", TypeInfoFactory
        .getMapTypeInfo(TypeInfoFactory.BIGINT, TypeInfoFactory.BOOLEAN)));
    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.hive.compatible", "true");
    hints.put("odps.sql.preparse.odps2", "hybrid");
    hints.put("odps.sql.planner.mode", "lot");
    hints.put("odps.sql.planner.parser.odps2", "true");
    hints.put("odps.sql.ddl.odps2", "true");

    odps.tables().create(odps.getDefaultProject(), tableName, schema, null, false, 10L, hints, null);

    assertEquals(odps.tables().get(tableName).getLife(), 10L);
    deleteTable(tableName);
  }

  @Test(expected = OdpsException.class)
  public void testCreateTableCrossProjectNeg() throws OdpsException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addColumn(new Column("c2", OdpsType.BOOLEAN));
    schema.addColumn(new Column("c3", OdpsType.DATETIME));
    schema.addColumn(new Column("c4", OdpsType.STRING));

    schema.addPartitionColumn(new Column("p1", OdpsType.BIGINT));
    schema.addPartitionColumn(new Column("p2", OdpsType.STRING));

    odps.tables().create("NOT_EXIST_PROJECT", tableName, schema);
  }

  @Test
  public void testGetString() {
    Table table = odps.tables().get(tableName);
    Assert.assertNotNull(table);
  }

  @Test
  public void testExistsString() throws OdpsException {
    boolean flag = odps.tables().exists(tableName);
    assertEquals(flag, true);

    flag = odps.tables().exists("table_name_for_test_not_exists");
    assertEquals(flag, false);
  }

  @Test
  public void testIteratorTableFilter() throws IOException, OdpsException {
    TableFilter tableFilter = new TableFilter();
    tableFilter.setName("user");
    Iterator<Table> iterator = odps.tables().iterator(tableFilter);
    while (iterator.hasNext()) {
      Table table = iterator.next();
      Assert.assertNotNull(table.getName());
    }

    tableFilter = new TableFilter();
    String owner = OdpsTestUtils.getCurrentUser();
    tableFilter.setOwner(owner);
    iterator = odps.tables().iterator(tableFilter);
    while (iterator.hasNext()) {
      Table table = iterator.next();
      Assert.assertNotNull(table.getOwner());
    }
  }

  @Test
  public void testIterableTableFilter() throws IOException, OdpsException {
    TableFilter tableFilter = new TableFilter();
    tableFilter.setName("user");

    for (Table table : odps.tables().iterable(tableFilter)) {
      Assert.assertNotNull(table.getName());
    }

    tableFilter = new TableFilter();
    String owner = OdpsTestUtils.getCurrentUser();
    tableFilter.setOwner(owner);

    for (Table table : odps.tables().iterable(tableFilter)) {
      Assert.assertNotNull(table.getOwner());
    }
  }

  @Test
  public void testIteratorString() {
    Iterator<Table> iterator = odps.tables().iterator();
    if (iterator.hasNext()) {
      Assert.assertNotNull(iterator.next().getName());
    }
  }

  @Test
  public void testIterableString() {
    for (Table table : odps.tables().iterable()) {
      Assert.assertNotNull(table.getName());
    }
  }

  @Test
  public void testDeleteString() throws OdpsException {
    //Already Test in method tearDown() ,please don't delete again
  }


  @Test(expected = NoSuchObjectException.class)
  public void testLoadTablesNeg_TableNotExists() throws OdpsException {
    List<String> tables = new ArrayList<String>();
    String name = OdpsTestUtils.getRandomTableName("TABLE_RELOAD_TEST_NOT_EXIST");
    deleteTable(name);

    tables.add(name);
    odps.tables().loadTables(tables);

    fail();
  }

  @Test(expected = OdpsException.class)
  public void testLoadTablesNeg_OverHeadTables() throws OdpsException {
    List<Table> tables = getTables(odps, 101);
    List<String> tableNames = new ArrayList<String>(); // for tear down
    String name = null;
    if (tables.size() < 100) {
      int number = 101 - tables.size();
      while (number-- > 0) {
        name = OdpsTestUtils.getRandomTableName("testReloadTables_OverHead");
        tableNames.add(name);
        createTable(name);
        tables.add(odps.tables().get(name));
      }
    }

    try {
      odps.tables().reloadTables(tables);
    } catch (OdpsException e) {
      assertTrue(e.getMessage().contains("limit"));

      for (String n : tableNames) {
        deleteTable(n);
      }
      throw e;
    }

    fail();
  }

  @Test
  public void testIteratorExtended() {
    Iterator<Table> it = odps.tables().iterator(odps.getDefaultProject(), null, true);

    int counter = 0;
    while (it.hasNext() && counter < 100) {
      Table t = it.next();
      // The following fields should be loaded when listing table with extended flag
      t.isExternalTable();
      t.isVirtualView();
      t.getComment();

      // The above method calls should not trigger reloading
      assertFalse(t.isLoaded());
      counter += 1;
    }
  }


  private void checkReloadResult(List<Table> reloadedTables) {
    for (Table t : reloadedTables) {
      assertTrue(t.isLoaded());
      assertNotNull(t.getSchema());
      assertEquals(odps.tables().get(t.getName()).getJsonSchema(), t.getJsonSchema());
      assertEquals(odps.getDefaultProject(), t.getProject());
    }
  }

  private List<Table> getTables(Odps odps, int limit) {
    List<Table> tables = new ArrayList<Table>();
    Iterator<Table> tableIterator = odps.tables().iterator();
    while (tableIterator.hasNext() && tables.size() < limit) {
      tables.add(tableIterator.next());
    }
    return tables;
  }
}
