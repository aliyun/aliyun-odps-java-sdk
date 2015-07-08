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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.task.SQLTask;

public class TableTest extends TestBase {

  private static TableSchema schema;

  private static String TABLE_NAME = "table_name_for_test";
  private static String TABLE_NAME_2 = "table_name_for_test2";
  private static String TABLE_NAME_3 = "table_name_for_test3";
  private static String HUB_TABLE_NAME = "hubtable_name_for_test";
  private static String HUB_TABLE_NAME_2 = "table_name_for_test_2";
  private static String HUB_TABLE_NAME_3 = "hubtable_name_for_test_3";
  private static String HUB_TABLE_NAME_4 = "hubtable_name_for_test_4";
  private static String TRUNCATE_TABLE_NAME = "truncate_test";
  private static String SOURCE_TABLE_NAME = "test_table_test";
  private static String NO_CHECK_TALBE = "no_check_table";

  @BeforeClass
  public static void setUp() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
    OdpsTestUtils.createTableForTest(SOURCE_TABLE_NAME);

    schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addColumn(new Column("c2", OdpsType.BOOLEAN));
    schema.addColumn(new Column("c3", OdpsType.DATETIME));
    schema.addColumn(new Column("c4", OdpsType.STRING));
    schema.addColumn(new Column("c5", OdpsType.DECIMAL));
    ArrayList<OdpsType> genericTypeList6 = new ArrayList<OdpsType>();
    Column c6 = new Column("c6", OdpsType.ARRAY);
    genericTypeList6.add(OdpsType.BIGINT);
    c6.setGenericTypeList(genericTypeList6);
    schema.addColumn(c6);

    ArrayList<OdpsType> genericTypeList7 = new ArrayList<OdpsType>();
    Column c7 = new Column("c7", OdpsType.MAP);
    genericTypeList7.add(OdpsType.STRING);
    genericTypeList7.add(OdpsType.STRING);
    c7.setGenericTypeList(genericTypeList7);
    schema.addColumn(c7);

    schema.addPartitionColumn(new Column("p1", OdpsType.BIGINT));
    schema.addPartitionColumn(new Column("p2", OdpsType.STRING));

    odps.tables().create(odps.getDefaultProject(), TABLE_NAME, schema, true);
    odps.tables().create(odps.getDefaultProject(), TABLE_NAME_2, schema, true);
    odps.tables().create(odps.getDefaultProject(), TABLE_NAME_3, schema, true);
    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME, schema, true);
    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME_2, schema, true);
    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME_3, schema, true);
    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME_4, schema, true);

    odps.projects().get().getSecurityManager()
        .runQuery("SET LABEL 2 to TABLE " + TABLE_NAME + "(c1)", false);

    odps.projects().get().getSecurityManager()
        .runQuery("SET LABEL D2 to TABLE " + TABLE_NAME + "(c3)", false);

    odps.tables().delete(TRUNCATE_TABLE_NAME, true);

    Instance i = SQLTask
        .run(odps, "create table " + TRUNCATE_TABLE_NAME + " as select * from " + SOURCE_TABLE_NAME
                   + ";");
    i.waitForSuccess();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    odps.tables().delete(TABLE_NAME, true);
    odps.tables().delete(TABLE_NAME_2, true);
    odps.tables().delete(TABLE_NAME_3, true);
    odps.tables().delete(HUB_TABLE_NAME, true);
    odps.tables().delete(HUB_TABLE_NAME_2, true);
    odps.tables().delete(TRUNCATE_TABLE_NAME, true);
    odps.tables().delete(HUB_TABLE_NAME_3, true);
    odps.tables().delete(HUB_TABLE_NAME_4, true);
  }

  @Test
  public void testGetSchema() throws OdpsException {
    Table a = odps.tables().get(TABLE_NAME);
    a.getSchema().getColumn("c1").getType().equals(OdpsType.BIGINT);
    a.getSchema().getColumn("c2").getType().equals(OdpsType.BOOLEAN);
    a.getSchema().getColumn("c3").getType().equals(OdpsType.DATETIME);
    a.getSchema().getColumn("c4").getType().equals(OdpsType.STRING);
    a.getSchema().getColumn("c5").getType().equals(OdpsType.DECIMAL);
    a.getSchema().getColumn("c6").getType().equals(OdpsType.ARRAY);
    a.getSchema().getColumn("c7").getType().equals(OdpsType.MAP);
    a.getSchema().getColumn("c6").getGenericTypeList().get(0).equals(OdpsType.BIGINT);
    a.getSchema().getColumn("c7").getGenericTypeList().get(0).equals(OdpsType.STRING);
    a.getSchema().getColumn("c7").getGenericTypeList().get(1).equals(OdpsType.STRING);
  }

  @Test
  public void testExistsPartition() throws OdpsException {
    Table a = odps.tables().get(TABLE_NAME);
    assertFalse(a.hasPartition(new PartitionSpec("p1=2,p2=3")));
    a.createPartition(new PartitionSpec("p1=2,p2=3"));
    assertTrue(a.hasPartition(new PartitionSpec("p1=2,p2=3")));
  }

  @Test(expected = IOException.class)
  public void testReadTableWithNotSupportSchema() throws OdpsException, IOException {
    Instance instance = SQLTask
        .run(
            odps,
            "insert overwrite table "
            + TABLE_NAME
            + " partition(p1=1,p2=3) select 1, true, null, 'string's, cast(1 as decimal),  array(1,2) ,  str_to_map('1=b,2=d',',','=') from "
            + SOURCE_TABLE_NAME + ";");
    instance.waitForSuccess();
    Table a = odps.tables().get(TABLE_NAME);
    RecordReader rr = a.read(1000000);
    Record g;
    while ((g = rr.read()) != null) {
      for (int i = 0; i < g.getColumnCount(); ++i) {
        System.out.println(g.getColumns()[i].getName() + ":" + g.get(i));
      }
    }
  }

  @Test
  public void testReadTable() throws OdpsException, IOException {

    Table a = odps.tables().get(TRUNCATE_TABLE_NAME);
    RecordReader rr = a.read(1000000);
    Record g;
    while ((g = rr.read()) != null) {
      for (int i = 0; i < g.getColumnCount(); ++i) {
        System.out.println(g.getColumns()[i].getName() + ":" + g.get(i));
      }
    }

    try {
      a.read(-1);
    } catch (OdpsException e) {
      return;
    } finally {
    }
    Assert.fail("Read should not accept limit < 0");
  }

  @Test
  public void testColumnLabel() throws OdpsException {
    Table table = odps.tables().get(TABLE_NAME);
    Assert.assertEquals(table.getSchema().getColumn("c3").getCategoryLabel(), "D2");
    Assert.assertEquals(table.getSchema().getColumn("c3").getLabel(), new Long(0));
    Assert.assertEquals(table.getSchema().getColumn("c1").getLabel(), new Long(2));
    Assert.assertEquals(table.getSchema().getColumn("c2").getLabel(), null);
  }

  @Test
  public void testNoCheckCreateDrop() throws OdpsException {
    odps.tables().create(odps.getDefaultProject(), NO_CHECK_TALBE, schema, true);
    odps.tables().create(odps.getDefaultProject(), NO_CHECK_TALBE, schema, true);
    odps.tables().delete(odps.getDefaultProject(), NO_CHECK_TALBE, true);
    odps.tables().delete(odps.getDefaultProject(), NO_CHECK_TALBE, true);
  }

  @Test
  public void testCreatePartition() throws OdpsException {

    Table table = odps.tables().get(TABLE_NAME);
    PartitionSpec spec = new PartitionSpec();
    spec.set("p1", "1");
    spec.set("p2", "2");

    // create partition
    table.createPartition(spec);

    // get all partitions
    Iterator<Partition> parts = table.getPartitionIterator(spec);
    assertTrue(parts.hasNext());

    Partition part = parts.next();
    Assert.assertNotNull(part.getCreatedTime());

    table.deletePartition(spec);

  }

  @Test
  public void testCreateExistsPartition() throws OdpsException {

    Table table = odps.tables().get(TABLE_NAME);
    PartitionSpec spec = new PartitionSpec();
    spec.set("p1", "partition");
    spec.set("p2", "exists");

    // create partition
    table.createPartition(spec, true);
    table.createPartition(spec, true);

    // get all partitions
    Iterator<Partition> parts = table.getPartitionIterator(spec);
    assertTrue(parts.hasNext());

    Partition part = parts.next();
    Assert.assertNotNull(part.getCreatedTime());

    table.deletePartition(spec, true);
    table.deletePartition(spec, true);

  }

  @Test
  public void testCreateShardsOnHubTable() throws OdpsException {
    odps.tables().get(HUB_TABLE_NAME).createShards(2, true, 2);
  }

  @Test
  public void testCreateShardOnNormalTable() throws OdpsException {
    odps.tables().get(TABLE_NAME_2).createShards(2);
  }

  @Test(expected = OdpsException.class)
  public void testCreateShardsTwiceOnHubTable() throws OdpsException {
    odps.tables().get(HUB_TABLE_NAME_2).createShards(1, true, 7);
    odps.tables().get(HUB_TABLE_NAME_2).createShards(2, true, 7);
  }

  @Test(expected = OdpsException.class)
  public void testCreatePartitionCrossProject() throws OdpsException {

    Table table = odps.tables().get("NOT_EXIST_PROJECT", TABLE_NAME);
    PartitionSpec spec = new PartitionSpec();
    spec.set("p1", "2");
    spec.set("p2", "3");

    table.createPartition(spec);
  }

  @Test
  public void testTruncate() throws OdpsException {
    Table t = odps.tables().get(TRUNCATE_TABLE_NAME);
    assertTrue(t.getSize() > 0);
    t.truncate();
    t.reload();
    assertTrue(t.getSize() == 0);
  }

  @Test
  public void testGetJsonSchema() throws OdpsException {
    Table a = odps.tables().get(TABLE_NAME);
    a.getJsonSchema();
  }

  @Test
  public void testGetExtendInfo() throws OdpsException {
    Table a = odps.tables().get(TABLE_NAME);

    assertTrue(!a.isArchived());
    a.getPhysicalSize();
    a.getFileNum();
  }

  @Test
  public void testGetShardInfo() throws OdpsException {
    Table t1 = odps.tables().get(HUB_TABLE_NAME_3);
    t1.createShards(3, true, 7);
    Shard s1 = t1.getShard();
    assertTrue(s1 != null);
    assertTrue(s1.getShardNum() == 3);
    assertTrue(s1.getHubLifecycle() == 7);
    assertTrue(s1.getDistributeColumnNames() != null && s1.getDistributeColumnNames().size() == 0);
    assertTrue(s1.getSortColumnNames() != null && s1.getSortColumnNames().size() == 0);

    Table t2 = odps.tables().get(HUB_TABLE_NAME_4);
    t2.createShards(2);
    Shard s2 = t2.getShard();
    assertTrue(s2 != null);
    assertTrue(s2.getShardNum() == 2);
    assertTrue(s2.getHubLifecycle() == -1);
    assertTrue(s2.getDistributeColumnNames() != null && s2.getDistributeColumnNames().size() == 0);
    assertTrue(s2.getSortColumnNames() != null && s2.getSortColumnNames().size() == 0);

    Table t3 = odps.tables().get(TABLE_NAME);
    Shard s3 = t3.getShard();
    assertTrue(s3 == null);

    Table t4 = odps.tables().get(TABLE_NAME_3);
    Instance instance = SQLTask.run(
        odps, "alter table " + TABLE_NAME_3
              + " clustered by (c1, c2) sorted by (c3, c4) into 10 shards;");
    instance.waitForSuccess();

    Shard s4 = t4.getShard();
    assertTrue(s4 != null);
    assertTrue(s4.getShardNum() == 10);
    assertTrue(s4.getHubLifecycle() == -1);
    assertTrue(s4.getDistributeColumnNames() != null && s4.getDistributeColumnNames().size() == 2);
    assertTrue(s4.getDistributeColumnNames().get(0).equals("c1"));
    assertTrue(s4.getDistributeColumnNames().get(1).equals("c2"));
    assertTrue(s4.getSortColumnNames() != null & s4.getSortColumnNames().size() == 2);
    assertTrue(s4.getSortColumnNames().get(0).equals("c3:1"));
    assertTrue(s4.getSortColumnNames().get(1).equals("c4:1"));
  }
}
