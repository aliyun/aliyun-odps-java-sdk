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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.StringUtils;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class TablesTest extends TestBase {

  private static final TableSchema SCHEMA;
  static {
    SCHEMA = new TableSchema();
    SCHEMA.addColumn(new Column("c1", TypeInfoFactory.BIGINT));
    SCHEMA.addColumn(new Column("c2", TypeInfoFactory.BOOLEAN, "comment"));
    SCHEMA.addColumn(new Column("c3", TypeInfoFactory.DATETIME));
    SCHEMA.addColumn(new Column("c4", TypeInfoFactory.STRING));
    SCHEMA.addPartitionColumn(new Column("p1", TypeInfoFactory.BIGINT));
    SCHEMA.addPartitionColumn(new Column("p2", TypeInfoFactory.STRING, "comment"));
  }

  private static final String BASE_TABLE_NAME_PREFIX = TablesTest.class.getSimpleName();

  private static final List<String> tablesToDrop = new LinkedList<>();

  @AfterClass
  public static void tearDown() {
    // Drop tables created this time
    for (String tableName : tablesToDrop) {
      try {
        odps.tables().delete(tableName, true);
      } catch (OdpsException e) {
        System.err.println("Failed to drop table: " + tableName);
      }
    }

    // Drop tables previously created
    for (Table table : odps.tables()) {
      if (table.getName().startsWith(BASE_TABLE_NAME_PREFIX.toLowerCase())) {
        try {
          odps.tables().delete(table.getName());
        } catch (OdpsException e) {
          System.err.println("Failed to drop table: " + table.getName());
        }
      }
    }
  }

  @Test
  public void testListSystemCatalogTables() throws OdpsException {
    Assume.assumeTrue(odps.projects().exists("system_catalog"));
    Iterator<Table>
        it =
        odps.tables().iterator("system_catalog", "information_schema", null, false);
    ListIterator<Table> listIterator = (ListIterator<Table>) it;
    List<Table> tables = listIterator.list(null, 1);
    assertNotNull(listIterator.getMarker());
  }

  @Test
  public void testGetSystemCatalogTable() throws OdpsException {
    Assume.assumeTrue(odps.projects().exists("system_catalog"));
    Table table = odps.tables().get("system_catalog", "information_schema", "catalog_privileges");
    table.reload();
  }
  @Test
  public void testCreateTable() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s", BASE_TABLE_NAME_PREFIX, "testCreateTable", OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);

    odps.tables()
        .create(odps.getDefaultProject(), tableName, SCHEMA, "comment", false);

    // Validate table level attributes
    Table table = odps.tables().get(tableName);
    assertEquals(table.getComment(), "comment");

    // Validate column level attributes
    TableSchema actualSchema = table.getSchema();
    assertEquals(SCHEMA.getColumns().size(), actualSchema.getColumns().size());
    for (int i = 0; i < SCHEMA.getColumns().size(); i++) {
      Column expectedColumn = SCHEMA.getColumn(i);
      Column actualColumn = actualSchema.getColumn(i);
      assertEquals(expectedColumn.getName(), actualColumn.getName());
      assertEquals(expectedColumn.getTypeInfo().getOdpsType(),
                   actualColumn.getTypeInfo().getOdpsType());
      assertEquals(StringUtils.isNullOrEmpty(expectedColumn.getComment()),
                   StringUtils.isNullOrEmpty(actualColumn.getComment()));
    }
    assertEquals(SCHEMA.getPartitionColumns().size(), actualSchema.getPartitionColumns().size());
    for (int i = 0; i < SCHEMA.getPartitionColumns().size(); i++) {
      Column expectedColumn = SCHEMA.getPartitionColumn(i);
      Column actualColumn = actualSchema.getPartitionColumn(i);
      assertEquals(expectedColumn.getName(), actualColumn.getName());
      assertEquals(expectedColumn.getTypeInfo().getOdpsType(),
                   actualColumn.getTypeInfo().getOdpsType());
      assertEquals(StringUtils.isNullOrEmpty(expectedColumn.getComment()),
                   StringUtils.isNullOrEmpty(actualColumn.getComment()));
    }
  }

  @Test
  public void testCreateTableWithLifeCycle() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testCreateTableWithLifeCycle",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);

    odps.tables().createTableWithLifeCycle(
        odps.getDefaultProject(),
        tableName,
        SCHEMA,
        null,
        false,
        10L);

    assertEquals(odps.tables().get(tableName).getLife(), 10L);
  }

  @Test
  public void testCreateExternalTableWithBuiltinStorageHandler() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testCreateExternalTableWithBuiltinStorageHandler",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.preparse.odps2", "lot");
    hints.put("odps.sql.planner.mode", "lot");
    hints.put("odps.sql.planner.parser.odps2", "true");
    hints.put("odps.sql.ddl.odps2", "true");
    hints.put("odps.compiler.output.format", "lot,pot");

    odps.tables().createExternal(
        odps.getDefaultProject(),
        tableName,
        SCHEMA,
        "MOCKoss://full/uri/path/to/oss/directory/",
        "com.aliyun.odps.CsvStorageHandler",
        null,
        null,
        "External table with partitions and builtin CsvStorageHandler",
        false,
        10L,
        hints,
        null);

    Table table = odps.tables().get(tableName);
    assertTrue(table.isExternalTable());
    assertEquals(table.getStorageHandler(), "com.aliyun.odps.CsvStorageHandler");
    assertEquals(table.getLife(), 10L);
  }

  @Test
  public void testCreateExternalTableWithUserDefinedStorageHandler() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testCreateExternalTableWithUserDefinedStorageHandler",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);

    List<String > jars = new ArrayList<String>();
    jars.add("odps-udf-example.jar");
    jars.add("another.jar");

    Map<String,String> properties = new HashMap<String, String>();
    properties.put("odps.text.option.delimiter", "|");
    properties.put("my.own.option", "value");

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.preparse.odps2", "lot");
    hints.put("odps.sql.planner.mode", "lot");
    hints.put("odps.sql.planner.parser.odps2", "true");
    hints.put("odps.sql.ddl.odps2", "true");
    hints.put("odps.compiler.output.format", "lot,pot");

    odps.tables().createExternal(
        odps.getDefaultProject(),
        tableName,
        SCHEMA,
        "MOCKoss://full/uri/path/to/oss/directory/",
        "com.aliyun.odps.udf.example.text.TextStorageHandler",
        jars,
        properties,
        "External table using user defined TextStorageHandler",
        true,
        10L,
        hints,
        null);

    Table table = odps.tables().get(tableName);
    assertTrue(table.isExternalTable());
    assertEquals(table.getStorageHandler(),
                 "com.aliyun.odps.udf.example.text.TextStorageHandler");
    assertEquals(table.getResources().split(",").length, 2);
  }

  @Test
  public void testCreateTableWithHints() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testCreateTableWithHints",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.hive.compatible", "true");
    hints.put("odps.sql.preparse.odps2", "hybrid");
    hints.put("odps.sql.planner.mode", "lot");
    hints.put("odps.sql.planner.parser.odps2", "true");
    hints.put("odps.sql.ddl.odps2", "true");

    odps.tables().create(
        odps.getDefaultProject(),
        tableName,
        SCHEMA,
        null,
        false,
        10L,
        hints,
        null);

    assertEquals(odps.tables().get(tableName).getLife(), 10L);
  }

  @Test(expected = OdpsException.class)
  public void testCreateTableCrossProjectNeg() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testCreateTableCrossProjectNeg",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);

    odps.tables().create("nonexistent", tableName, SCHEMA);
  }

  @Test
  public void testExistsPos() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testExistsPos",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);

    odps.tables().create(tableName, SCHEMA);
    assertTrue(odps.tables().exists(tableName));
  }

  @Test
  public void testExistsNeg() throws OdpsException {
    assertFalse(odps.tables().exists("nonexistent"));
  }

  @Test
  public void testIteratorFilterByName() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testIteratorFilterByName",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);
    odps.tables().create(tableName, SCHEMA);

    TableFilter tableFilter = new TableFilter();
    tableFilter.setName(BASE_TABLE_NAME_PREFIX);
    Iterator<Table> iterator = odps.tables().iterator(tableFilter);
    while (iterator.hasNext()) {
      Table table = iterator.next();
      assertNotNull(table.getName());
      assertTrue(table.getName().startsWith(BASE_TABLE_NAME_PREFIX.toLowerCase()));
    }
  }

  @Test
  public void testIteratorFilterByOwner() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testIteratorFilterByOwner",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);
    odps.tables().create(tableName, SCHEMA);

    String owner = OdpsTestUtils.getCurrentUser(odps);
    assertNotNull(owner);

    TableFilter tableFilter = new TableFilter();
    tableFilter.setOwner(owner);
    Iterator<Table> iterator = odps.tables().iterator(tableFilter);
    while (iterator.hasNext()) {
      Table table = iterator.next();
      assertEquals(owner, table.getOwner());
    }
  }

  @Test
  public void testIterableFilterByName() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testIterableFilterByName",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);
    odps.tables().create(tableName, SCHEMA);

    TableFilter tableFilter = new TableFilter();
    tableFilter.setName(BASE_TABLE_NAME_PREFIX);
    for (Table table : odps.tables().iterable(tableFilter)) {
      System.out.println(table.getName());
      assertNotNull(table.getName());
      assertTrue(table.getName().startsWith(BASE_TABLE_NAME_PREFIX.toLowerCase()));
    }
  }

  @Test
  public void testIterableFilterByOwner() throws OdpsException {
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testIterableFilterByOwner",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);
    odps.tables().create(tableName, SCHEMA);

    String owner = OdpsTestUtils.getCurrentUser(odps);
    assertNotNull(owner);

    TableFilter tableFilter = new TableFilter();
    tableFilter.setOwner(owner);
    for (Table table : odps.tables().iterable(tableFilter)) {
      assertEquals(owner, table.getOwner());
    }
  }

  @Test
  public void testBatchLoading() throws OdpsException {
    List<Table> expectedTables = getTables(odps, odps.getDefaultProject(), 10);
    assertTrue(expectedTables.size() > 0);

    List<String> names = expectedTables.stream().map(Table::getName).collect(Collectors.toList());

    // Test method Tables#reloadTables
    List<Table> actualTables = odps.tables().reloadTables(expectedTables);
    batchAsserting(expectedTables, actualTables);

    // Test method Tables#loadTables
    actualTables = odps.tables().loadTables(names);
    batchAsserting(expectedTables, actualTables);
  }

  @Test
  public void testBatchLoadingCrossProjects() throws OdpsException, IOException {
    String crossProject = OdpsTestUtils.loadConfig().getProperty("security.project");
    assertNotEquals(odps.getDefaultProject(), crossProject);

    List<Table> expectedTables = getTables(odps, crossProject, 10);
    assertTrue(expectedTables.size() > 0);

    List<String> names = expectedTables.stream().map(Table::getName).collect(Collectors.toList());

    // Test method Tables#loadTables
    List<Table> actualTables = odps.tables().loadTables(crossProject, names);
    batchAsserting(expectedTables, actualTables);

    // Test method Tables#reloadTables
    expectedTables.addAll(getTables(odps, odps.getDefaultProject(), 10));
    assertTrue(expectedTables.size() > actualTables.size());
    actualTables = odps.tables().reloadTables(expectedTables);
    batchAsserting(expectedTables, actualTables);
  }

  @Test
  public void testBatchLoadingDuplicated() throws OdpsException {
    List<Table> expectedTables = getTables(odps, odps.getDefaultProject(), 10);
    assertTrue(expectedTables.size() > 0);

    expectedTables.add(expectedTables.get(0));

    List<Table> actualTables = odps.tables().reloadTables(expectedTables);
    assertEquals(expectedTables.size() - 1, actualTables.size());

    batchAsserting(expectedTables.subList(0, expectedTables.size() - 1), actualTables);
  }

  // Make sure CheckPermissionUsingAcl=true !!!
  @Test
  public void testBatchLoadingVariousPermission() throws OdpsException, InterruptedException {
    Odps grantOdps = OdpsTestUtils.newGrantOdps(odps.getDefaultProject());

    // Use OdpsTestUtils.getProperty("grant.user") to test privilege
    List<Table> expectedTables = new ArrayList<Table>();
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testBatchLoadingVariousPermission",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);
    odps.tables().create(tableName, SCHEMA);

    expectedTables.add(odps.tables().get(tableName));

    // Make sure OdpsTestUtils.getProperty("grant.user") has role admin and list privilege
    try {
      odps.projects().get().getSecurityManager()
          .runQuery("GRANT ADMIN TO " + OdpsTestUtils.getProperty("grant.user"), false);
    } catch (OdpsException e) {
      assertTrue(e.getMessage().contains("Principal already have the Role:admin"));
    }
    odps.projects().get().getSecurityManager()
        .runQuery("GRANT LIST ON PROJECT " + odps.getDefaultProject() + " TO " + OdpsTestUtils.getProperty("grant.user"), false);

    // Revoke role admin
    odps.projects().get().getSecurityManager()
        .runQuery("REVOKE ADMIN FROM " + OdpsTestUtils.getProperty("grant.user"), false);
    // Revoke list privilege from odpstest4
    odps.projects().get().getSecurityManager()
        .runQuery("REVOKE LIST ON PROJECT " + odps.getDefaultProject() + " FROM USER " + OdpsTestUtils.getProperty("grant.user"), false);


    List<Table> actualTables = grantOdps.tables().reloadTables(expectedTables);
    assertEquals(1, actualTables.size());
    assertEquals(tableName.toLowerCase(), actualTables.get(0).getName());
    assertEquals(odps.getDefaultProject(), actualTables.get(0).getProject());
    // Owner and schema should not be visible due to lack of privilege
    assertNull(actualTables.get(0).getOwner());
    assertNull(actualTables.get(0).getSchema());

    // Re-grant list privilege
    odps.projects().get().getSecurityManager()
        .runQuery("GRANT LIST ON PROJECT " + odps.getDefaultProject() + " TO USER " + OdpsTestUtils.getProperty("grant.user"), false);

    actualTables = grantOdps.tables().reloadTables(expectedTables);
    assertEquals(1, actualTables.size());
    assertEquals(tableName.toLowerCase(), actualTables.get(0).getName());
    assertEquals(odps.getDefaultProject(), actualTables.get(0).getProject());
    // Owner should be visible since list privilege is granted
    assertEquals(odps.tables().get(tableName).getOwner(), actualTables.get(0).getOwner());
    // Schema should still not visible since it required desc privilege
    assertNull(actualTables.get(0).getSchema());

    // Re-grant role admin. Now it has desc privilege
    odps.projects().get().getSecurityManager()
        .runQuery("GRANT ADMIN TO " + OdpsTestUtils.getProperty("grant.user"), false);

    actualTables = grantOdps.tables().reloadTables(expectedTables);
    assertEquals(1, actualTables.size());
    assertEquals(tableName.toLowerCase(), actualTables.get(0).getName());
    assertEquals(odps.getDefaultProject(), actualTables.get(0).getProject());
    assertEquals(odps.tables().get(tableName).getOwner(), actualTables.get(0).getOwner());
    assertNotNull(actualTables.get(0).getSchema());
    // app account return size=-1
    // assertEquals(odps.tables().get(tableName).getJsonSchema(), actualTables.get(0).getJsonSchema());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testBatchLoadingNonExistent() throws OdpsException {
    List<String> tables = new ArrayList<String>();
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testBatchLoadingNeg",
        OdpsTestUtils.getRandomName());

    tables.add(tableName);
    odps.tables().loadTables(tables);
    fail();
  }

  @Test(expected = OdpsException.class)
  public void testBatchLoadingExceedsNumTablesLimit() throws OdpsException {
    List<Table> expectedTables = getTables(odps, odps.getDefaultProject(), 101);
    // Make sure there are more than 101 tables
    for (int i = 0; i < 101 - expectedTables.size(); i++) {
      String tableName = String.format(
          "%s_%s_%s",
          BASE_TABLE_NAME_PREFIX,
          "testBatchLoadingExceedsNumTablesLimit",
          OdpsTestUtils.getRandomName());
      tablesToDrop.add(tableName);
      odps.tables().create(tableName, SCHEMA);
    }

    try {
      odps.tables().reloadTables(expectedTables);
    } catch (OdpsException e) {
      assertTrue(e.getMessage().contains("limit"));
      throw e;
    }

    fail();
  }

  @Test
  public void testIteratorGetMarker() throws OdpsException {
    int maxItems = 10;
    String marker = null;

    ListIterator<Table> it = (ListIterator<Table>) odps.tables().iterator(odps.getDefaultProject(), null);
    List<Table> page1 = it.list(marker, maxItems);
    assertEquals(10, page1.size());
    marker = it.getMarker();
    assertNotNull(marker);
    System.out.println(marker);

    List<Table> page2 = it.list(marker, maxItems);
    assertEquals(10, page2.size());
    assertNotEquals(marker, it.getMarker());
    marker = it.getMarker();
    assertNotNull(marker);
    System.out.println(marker);
    assertNotEquals(page1.get(0).getName(), page2.get(0).getName());
  }


  @Test
  public void testIteratorExtended() throws OdpsException {
    // Make sure there are at least one table
    String tableName = String.format(
        "%s_%s_%s",
        BASE_TABLE_NAME_PREFIX,
        "testBatchLoadingExceedsNumTablesLimit",
        OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);
    odps.tables().create(tableName, SCHEMA);

    Iterator<Table> it = odps.tables().iterator(odps.getDefaultProject(), null, true);

    int counter = 0;
    while (it.hasNext() && counter < 10) {
      Table t = it.next();
      // The following fields should be loaded when listing table with extended flag
      t.isExternalTable();
      t.isVirtualView();
      t.getComment();

      // The above method calls should not trigger reloading
      assertFalse(t.isLoaded());
      counter += 1;
    }
    assertTrue(counter > 0);
  }

  @Test
  public void testIteratorWithFilter() throws OdpsException {
    // Make sure there are at least one table
    String tableName = String.format(
            "%s_%s_%s",
            BASE_TABLE_NAME_PREFIX,
            "testBatchLoadingExceedsNumTablesLimit",
            OdpsTestUtils.getRandomName());
    tablesToDrop.add(tableName);
    odps.tables().create(tableName, SCHEMA);

    TableFilter filter = new TableFilter();
    filter.setType(Table.TableType.MANAGED_TABLE);
    Iterator<Table> it = odps.tables().iterator(odps.getDefaultProject(), filter, true);

    int counter = 0;
    while (it.hasNext() && counter < 10) {
      Table t = it.next();

      assertEquals(Table.TableType.MANAGED_TABLE, t.getType());

      // The above method calls should not trigger reloading
      assertFalse(t.isLoaded());
      counter += 1;
    }
    assertTrue(counter > 0);
  }

  private void batchAsserting(List<Table> expectedTables, List<Table> actualTables) {
    assertEquals(expectedTables.size(), actualTables.size());

    for (int i = 0; i < expectedTables.size(); i++) {
      Table expected = expectedTables.get(i);
      Table actual = actualTables.get(i);

      assertTrue(actual.isLoaded());
      assertNotNull(actual.getSchema());
      assertEquals(expected.getJsonSchema(), actual.getJsonSchema());
      assertEquals(expected.getProject(), actual.getProject());
    }
  }

  private List<Table> getTables(Odps odps, String project, int limit) {
    List<Table> tables = new ArrayList<Table>();
    Iterator<Table> tableIterator = odps.tables().iterator(project);
    while (tableIterator.hasNext() && tables.size() < limit) {
      tables.add(tableIterator.next());
    }
    return tables;
  }

  @Test
  public void testGetExternalTable() throws OdpsException {
    List<Table> expectedTables = getTables(odps, odps.getDefaultProject(), 100);
//    assertTrue(expectedTables.size() > 0);

    List<String> names = expectedTables.stream().map(Table::getName).collect(Collectors.toList());
    List<String> owners = expectedTables.stream().map(Table::getOwner).collect(Collectors.toList());
    for (Table t:expectedTables) {
      System.out.println(t.getName());
//      System.out.println(t.getFileNum());
      System.out.println(t.getComment());
    }

  }
}
