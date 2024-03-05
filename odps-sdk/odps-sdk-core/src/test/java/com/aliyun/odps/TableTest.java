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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Classification.AttributeDefinition;
import com.aliyun.odps.Classification.BooleanAttributeDefinition;
import com.aliyun.odps.Classification.EnumAttributeDefinition;
import com.aliyun.odps.Classification.IntegerAttributeDefinition;
import com.aliyun.odps.Classification.StringAttributeDefinition;
import com.aliyun.odps.Table.TableModel;
import com.aliyun.odps.Table.TableType;
import com.aliyun.odps.Tags.TagBuilder;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.org.apache.xml.internal.utils.URI;

public class TableTest extends TestBase {

  private static TableSchema schema;

  private static String TABLE_NAME = TableTest.class.getSimpleName() + "_table_name_for_test";
  private static String TABLE_NAME_2 = TableTest.class.getSimpleName() + "_table_name_for_test2";
  private static String TABLE_NAME_3 = TableTest.class.getSimpleName() + "_table_name_for_test3";
  private static String TABLE_NAME_4 = TableTest.class.getSimpleName() + "_table_name_for_test4";

  private static final String BASE_CLASSIFICATION_NAME_PREFIX = TableTest.class.getSimpleName();
  private static final String BASE_TAG_NAME_PREFIX = TableTest.class.getSimpleName();

  private static String
      HUB_TABLE_NAME =
      TableTest.class.getSimpleName() + "_hubtable_name_for_test";
  private static String
      HUB_TABLE_NAME_2 =
      TableTest.class.getSimpleName() + "_table_name_for_test_2";
  private static String
      HUB_TABLE_NAME_3 =
      TableTest.class.getSimpleName() + "_hubtable_name_for_test_3";
  private static String
      HUB_TABLE_NAME_4 =
      TableTest.class.getSimpleName() + "_hubtable_name_for_test_4";
  private static String TRUNCATE_TABLE_NAME = TableTest.class.getSimpleName() + "_truncate_test";
  private static String SOURCE_TABLE_NAME = TableTest.class.getSimpleName() + "_test_table_test";
  private static String NO_CHECK_TALBE = TableTest.class.getSimpleName() + "_no_check_table";
  private static String NON_PARTITION_TABLE =
      TableTest.class.getSimpleName() + "_non_partition_table";
  private static String PARTITIONED_TABLE_NAME =
      TableTest.class.getSimpleName() + "_partitioned_table_1";

  private static String PARTITIONED_TABLE_NAME_2 =
      TableTest.class.getSimpleName() + "_partitioned_table_2";

  @BeforeClass
  public static void setUp() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
    OdpsTestUtils.createTableForTest(SOURCE_TABLE_NAME);

    schema = new TableSchema();
    schema.addColumn(new Column("c1", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("c2", TypeInfoFactory.BOOLEAN));
    schema.addColumn(new Column("c3", TypeInfoFactory.DATETIME));
    schema.addColumn(new Column("c4", TypeInfoFactory.STRING));
    schema.addColumn(new Column("c5", TypeInfoFactory.DECIMAL));
    Column c6 = new Column("c6", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT));
    schema.addColumn(c6);

    Column c7 = new Column(
        "c7",
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.STRING));
    schema.addColumn(c7);

    schema.addPartitionColumn(new Column("p1", TypeInfoFactory.BIGINT));
    schema.addPartitionColumn(new Column("p2", TypeInfoFactory.STRING));

    odps.tables().create(odps.getDefaultProject(), TABLE_NAME, schema, true);
    odps.tables().create(odps.getDefaultProject(), TABLE_NAME_2, schema, true);
    odps.tables().create(odps.getDefaultProject(), TABLE_NAME_3, schema, true);

    odps.tables().create(odps.getDefaultProject(), TABLE_NAME_4, schema, true);

    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME_2, schema, true);
    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME_4, schema, true);
    odps.tables().create(odps.getDefaultProject(), PARTITIONED_TABLE_NAME, schema, true);
    odps.tables().create(odps.getDefaultProject(), PARTITIONED_TABLE_NAME_2, schema, true);

    odps.projects().get().getSecurityManager()
        .runQuery("SET LABEL 2 to TABLE " + TABLE_NAME + "(c1)", false);

    odps.projects().get().getSecurityManager()
        .runQuery("SET LABEL D2 to TABLE " + TABLE_NAME + "(c3)", false);

    // extended labels for column
    odps.projects().get().getSecurityManager()
        .runQuery("SET LABEl P2 TO TABLE " + TABLE_NAME + "(c1)", false);
    // extended labels for column
    odps.projects().get().getSecurityManager()
        .runQuery("SET LABEl P1 TO TABLE " + TABLE_NAME + "(c3)", false);
    // extended labels for table
    odps.projects().get().getSecurityManager()
        .runQuery("SET LABEl P5 TO TABLE " + TABLE_NAME, false);

    odps.tables().delete(TRUNCATE_TABLE_NAME, true);

    Instance i = SQLTask
        .run(odps, "create table " + TRUNCATE_TABLE_NAME + " as select * from " + SOURCE_TABLE_NAME
                   + ";");
    i.waitForSuccess();

    Table partitionedTable = odps.tables().get(PARTITIONED_TABLE_NAME);
    partitionedTable.createPartition(new PartitionSpec("p1=1,p2=foo"), true);
    partitionedTable.createPartition(new PartitionSpec("p1=1,p2=bar"), true);
    partitionedTable.createPartition(new PartitionSpec("p1=1,p2=baz"), true);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    odps.tables().delete(TABLE_NAME, true);
    odps.tables().delete(TABLE_NAME_2, true);
    odps.tables().delete(TABLE_NAME_3, true);
    odps.tables().delete(TABLE_NAME_4, true);
    odps.tables().delete(HUB_TABLE_NAME, true);
    odps.tables().delete(HUB_TABLE_NAME_2, true);
    odps.tables().delete(TRUNCATE_TABLE_NAME, true);
    odps.tables().delete(SOURCE_TABLE_NAME, true);
    odps.tables().delete(HUB_TABLE_NAME_3, true);
    odps.tables().delete(HUB_TABLE_NAME_4, true);
    odps.tables().delete(NON_PARTITION_TABLE, true);
    odps.tables().delete(PARTITIONED_TABLE_NAME, true);
    odps.tables().delete(PARTITIONED_TABLE_NAME_2, true);
  }

  @Test
  public void testGetSchema() {
    Table a = odps.tables().get(TABLE_NAME);
    TableSchema schema = a.getSchema();

    // Primitive types
    assertEquals(OdpsType.BIGINT,
                 schema.getColumn("c1").getTypeInfo().getOdpsType());
    assertEquals(OdpsType.BOOLEAN,
                 schema.getColumn("c2").getTypeInfo().getOdpsType());
    assertEquals(OdpsType.DATETIME,
                 schema.getColumn("c3").getTypeInfo().getOdpsType());
    assertEquals(OdpsType.STRING,
                 schema.getColumn("c4").getTypeInfo().getOdpsType());
    assertEquals(OdpsType.DECIMAL,
                 schema.getColumn("c5").getTypeInfo().getOdpsType());

    // Complex types
    TypeInfo arrayTypeInfo = schema.getColumn("c6").getTypeInfo();
    assertEquals(OdpsType.ARRAY, arrayTypeInfo.getOdpsType());
    assertEquals(OdpsType.BIGINT,
                 ((ArrayTypeInfo) arrayTypeInfo).getElementTypeInfo().getOdpsType());
    TypeInfo mapTypeInfo = schema.getColumn("c7").getTypeInfo();
    assertEquals(OdpsType.MAP, mapTypeInfo.getOdpsType());
    assertEquals(OdpsType.STRING, ((MapTypeInfo) mapTypeInfo).getKeyTypeInfo().getOdpsType());
    assertEquals(OdpsType.STRING, ((MapTypeInfo) mapTypeInfo).getValueTypeInfo().getOdpsType());
  }

  @Test
  public void testPartitionGetMarker() throws OdpsException {
    Table t = odps.tables().get(PARTITIONED_TABLE_NAME_2);
    for (int i = 10; i < 25; i++) {
      t.createPartition(new PartitionSpec("p1=" + i + ",p2=foo"), true);
    }
    Iterator<Partition> iterator = t.getPartitionIterator();

    ListIterator<Partition> listIterator = (ListIterator) iterator;
    List<Partition> partitions = listIterator.list(null, 10);
    assertEquals(10, partitions.size());
    assertNotNull(listIterator.getMarker());
  }

  @Test
  public void testPartitioned() throws OdpsException {
    Table a = odps.tables().get(TABLE_NAME);
    assertTrue(a.isPartitioned());
    Table b = odps.tables().get(TRUNCATE_TABLE_NAME);
    assertFalse(b.isPartitioned());
  }

  @Test
  public void testExistsPartition() throws OdpsException {
    Table a = odps.tables().get(TABLE_NAME);
    assertFalse(a.hasPartition(new PartitionSpec("p1=2,p2=3")));
    PartitionSpec spec = new PartitionSpec("p1=2,p2=3");
    a.createPartition(spec);
    assertTrue(a.hasPartition(new PartitionSpec("p1=2,p2=3")));
    a.deletePartition(spec);
  }


  @Test
  public void testIsExternalTable() throws OdpsException {
    assertFalse(odps.tables().get(TABLE_NAME).isExternalTable());
  }

  @Test
  public void testReadTable() throws OdpsException, IOException {
    Table a = odps.tables().get(SOURCE_TABLE_NAME);
    Record g;
    int count = 0;
    int limit = 5;
    RecordReader rr = a.read(null, null, limit, null);
    while ((g = rr.read()) != null) {
      count++;
      for (int i = 0; i < g.getColumnCount(); ++i) {
        System.out.println(g.getColumns()[i].getName() + ":" + g.get(i));
      }
    }
    Assert.assertEquals(count, limit);
  }

  @Test
  public void testColumnLabel() throws OdpsException {
    Table table = odps.tables().get(TABLE_NAME);
    Assert.assertEquals(table.getSchema().getColumn("c3").getLabel(), null);
    Assert.assertEquals(table.getSchema().getColumn("c1").getLabel(), new Long(2));
    Assert.assertEquals(table.getSchema().getColumn("c2").getLabel(), null);
  }

  @Test
  public void TestColumnNullable() throws Exception {
   Column column = new Column("c0", OdpsType.STRING);
   Assert.assertFalse(column.hasDefaultValue());

   column.setDefaultValue("abc");
   column.setNullable(false);

   Assert.assertTrue(column.hasDefaultValue());
   Assert.assertFalse(column.isNullable());
   Assert.assertEquals("abc", column.getDefaultValue());

   column.setNullable(true);
   column.setDefaultValue(null);

   Assert.assertTrue(column.isNullable());
   Assert.assertFalse(column.hasDefaultValue());
  }

  private static void testWhy() {
    Table table = odps.tables().get(TABLE_NAME);
    System.out.println(table.getSchema().getColumn("c1").getExtendedlabels());
  }


  @Test
  public void testExtendedLabel() throws OdpsException {
    Table table = odps.tables().get(TABLE_NAME);
    Assert.assertEquals(table.getSchema().getColumn("c1").getExtendedlabels(), Arrays.asList(new String [] {"P2"}));
    Assert.assertEquals(table.getSchema().getColumn("c3").getExtendedlabels(), Arrays.asList(new String[] {"D2", "P1"}));
    Assert.assertEquals(table.getTableExtendedLabels(), Arrays.asList(new String[] {"P5"}));

    Assert.assertEquals(table.getMaxExtendedLabel(), "P5");

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
  public void testCreateHubTable() throws OdpsException {
    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME, schema, true, 2L, 2L);
  }

  @Test
  public void testCreateShardOnNormalTable() throws OdpsException {
    odps.tables().create(odps.getDefaultProject(), TABLE_NAME_2, schema, true, 2L, null);
  }

  @Test(expected = OdpsException.class)
  public void testCreateShardsTwiceOnHubTable() throws OdpsException {
    odps.tables().delete(HUB_TABLE_NAME, true);
    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME, schema, true, 1L, 7L);
    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME, schema, false, 1L, 7L);
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
  public void testGetID() throws OdpsException {
    Table a = odps.tables().get(TABLE_NAME);
    assertNotNull(a.getTableID());
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
  public void testGetPartitionTableRecordNumber() throws OdpsException {
    Table a = odps.tables().get(TABLE_NAME);
    PartitionSpec spec = new PartitionSpec();
    spec.set("p1", "2");
    spec.set("p2", "3");
    a.createPartition(spec);

    Instance instance = SQLTask
        .run(
            odps,
            "insert into table "
                + TABLE_NAME
                + " partition(p1=2,p2=3) select 1, true, null, 'string's, cast(1 as decimal),"
                + " array(1,2), str_to_map('1=b,2=d',',','=');");
    instance.waitForSuccess();

    a.reload();
    long recordNum = a.getRecordNum();
    // For partition table, record number is always -1
    assertEquals(-1, recordNum);
    a.deletePartition(spec);
  }

  @Test
  public void testGetTableRecordNumber() throws OdpsException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.BIGINT));
    odps.tables().create(NON_PARTITION_TABLE, schema, true);

    Instance instance = SQLTask
        .run(odps, "insert into table " + NON_PARTITION_TABLE + " select 1;");
    instance.waitForSuccess();
    assertEquals(1, odps.tables().get(NON_PARTITION_TABLE).getRecordNum());
  }

  @Test
  public void testGetPartitionRecordNumber() throws OdpsException {
    Table a = odps.tables().get(TABLE_NAME);
    PartitionSpec spec = new PartitionSpec();
    spec.set("p1", "2");
    spec.set("p2", "3");
    a.createPartition(spec);

    Instance instance = SQLTask
        .run(
            odps,
            "insert into table "
                + TABLE_NAME
                + " partition(p1=2,p2=3) select 1, true, null, 'string's, cast(1 as decimal),"
                + " array(1,2), str_to_map('1=b,2=d',',','=');");
    instance.waitForSuccess();

    long recordNum = a.getPartition(spec).getRecordNum();
    assertEquals(1, recordNum);
    a.deletePartition(spec);
  }

  @Test
  public void testGetShardInfo() throws OdpsException {
    odps.tables().create(odps.getDefaultProject(), HUB_TABLE_NAME_3, schema, true, 3L, 7L);
    Table t1 = odps.tables().get(HUB_TABLE_NAME_3);
    Shard s1 = t1.getShard();
    assertTrue(t1.getHubLifecycle() == 7);
    assertTrue(s1 != null);
    assertTrue(s1.getShardNum() == 3);
    assertTrue(s1.getDistributeColumnNames() != null && s1.getDistributeColumnNames().size() == 0);
    assertTrue(s1.getSortColumnNames() != null && s1.getSortColumnNames().size() == 0);

    Table t2 = odps.tables().get(HUB_TABLE_NAME_4);
    t2.createShards(2);
    Shard s2 = t2.getShard();
    assertTrue(t2.getHubLifecycle() == -1);
    assertTrue(s2 != null);
    assertTrue(s2.getShardNum() == 2);
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
    assertTrue(t4.getHubLifecycle() == -1);
    assertTrue(s4.getDistributeColumnNames() != null && s4.getDistributeColumnNames().size() == 2);
    assertTrue(s4.getDistributeColumnNames().get(0).equals("c1"));
    assertTrue(s4.getDistributeColumnNames().get(1).equals("c2"));
    assertTrue(s4.getSortColumnNames() != null & s4.getSortColumnNames().size() == 2);
    assertTrue(s4.getSortColumnNames().get(0).equals("c3:1"));
    assertTrue(s4.getSortColumnNames().get(1).equals("c4:1"));
  }

  @Test
  public void testMaxLabel() {
    assertEquals("", Table.calculateMaxLabel(Arrays.asList(new String[]{})));
    assertEquals("", Table.calculateMaxLabel(Arrays.asList(new String[]{null, null, null})));
    assertEquals("B2", Table.calculateMaxLabel(Arrays.asList(new String[]{"B2", ""})));
    assertEquals("L2", Table.calculateMaxLabel(Arrays.asList(new String[]{"B2", "2"})));
    assertEquals("B2", Table.calculateMaxLabel(Arrays.asList(new String[]{"B2", "B1"})));
    assertEquals("C3", Table.calculateMaxLabel(Arrays.asList(new String[]{"B2", "C3"})));
    assertEquals("B2", Table.calculateMaxLabel(Arrays.asList(new String[]{"B2", "B1"})));
    assertEquals("C4", Table.calculateMaxLabel(Arrays.asList(new String[]{"C4", "S2"})));
    assertEquals("L4", Table.calculateMaxLabel(Arrays.asList(new String[]{"C4", "B4", "S2"})));
  }

  @Test
  public void testMarshal() throws Exception {
    // Table type should not be marshaled
    TableModel model = new TableModel();
    model.projectName = "foo";
    model.name = "bar";
    // This field should not be included in the xml
    model.type = TableType.MANAGED_TABLE;
    String xml = SimpleXmlUtils.marshal(model);
    String expected =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<Table>\n"
            + "   <Name>bar</Name>\n"
            + "   <Project>foo</Project>\n"
            + "</Table>";
    assertEquals(expected, xml);
  }


  @Test
  public void testTag() throws OdpsException {
    // Create classification
    Map<String, AttributeDefinition> attributes = new HashMap<>();
    attributes.put("str_attr", new StringAttributeDefinition.Builder().maxLength(10)
                                                                      .minLength(10)
                                                                      .build());
    attributes.put("int_attr", new IntegerAttributeDefinition.Builder().minimum(0)
                                                                       .maximum(10)
                                                                       .build());
    attributes.put("enum_attr", new EnumAttributeDefinition.Builder().element("foo")
                                                                     .element("bar")
                                                                     .build());
    attributes.put("bool_attr", new BooleanAttributeDefinition.Builder().build());
    String classificationName = String.format(
        "%s_%s_%s",
        BASE_CLASSIFICATION_NAME_PREFIX,
        "testTableTag",
        OdpsTestUtils.getRandomName());
    odps.classifications().create(classificationName, attributes, true);

    // Create tag
    String tagName = String.format(
        "%s_%s_%s",
        BASE_TAG_NAME_PREFIX,
        "testTableTag",
        OdpsTestUtils.getRandomName());
    TagBuilder builder = new TagBuilder(odps.classifications().get(classificationName), tagName)
        .attribute("str_attr", "1234567890")
        .attribute("int_attr", "7")
        .attribute("enum_attr", "foo")
        .attribute("bool_attr", "true");
    odps.classifications().get(classificationName).tags().create(builder, true);

    Tag tag = odps.classifications().get(classificationName).tags().get(tagName);

    Table table = odps.tables().get(SOURCE_TABLE_NAME);

    // There shouldn't be any table level tag
    assertEquals(0, table.getTags().size());

    // Create table level tags
    odps.tables().get(SOURCE_TABLE_NAME).addTag(tag);
    List<Tag> tags = odps.tables().get(SOURCE_TABLE_NAME).getTags();
    Assert.assertEquals(1, tags.size());
    tag = tags.get(0);
    Assert.assertEquals(4, tag.getAttributes().size());
    Assert.assertEquals("1234567890", tag.getAttributes().get("str_attr"));
    Assert.assertEquals("7", tag.getAttributes().get("int_attr"));
    Assert.assertEquals("foo", tag.getAttributes().get("enum_attr"));
    Assert.assertEquals("true", tag.getAttributes().get("bool_attr"));

    // Remove table level tags
    odps.tables().get(SOURCE_TABLE_NAME).removeTag(tag);
    tags = odps.tables().get(SOURCE_TABLE_NAME).getTags();
    Assert.assertEquals(0, tags.size());

    // There shouldn't be any column level tag
    assertEquals(0, table.getSimpleTags().size());

    // Create column level tag
    List<String> columns = new LinkedList<>();
    columns.add("c1");
    odps.tables().get(SOURCE_TABLE_NAME).addTag(tag, columns);
    tags = odps.tables().get(SOURCE_TABLE_NAME).getTags("c1");
    Assert.assertEquals(1, tags.size());
    tag = tags.get(0);
    Assert.assertEquals(4, tag.getAttributes().size());
    Assert.assertEquals("1234567890", tag.getAttributes().get("str_attr"));
    Assert.assertEquals("7", tag.getAttributes().get("int_attr"));
    Assert.assertEquals("foo", tag.getAttributes().get("enum_attr"));
    Assert.assertEquals("true", tag.getAttributes().get("bool_attr"));

    // Remove column level tag
    odps.tables().get(SOURCE_TABLE_NAME).removeTag(tag, columns);
    tags = odps.tables().get(SOURCE_TABLE_NAME).getTags("c1");
    Assert.assertEquals(0, tags.size());
  }

  @Test
  public void testSimpleTag() throws OdpsException {
    Table table = odps.tables().get(SOURCE_TABLE_NAME);

    // There shouldn't be any table level simple tag
    assertEquals(0, table.getSimpleTags().size());

    // Create table level simple tags
    odps.tables()
        .get(SOURCE_TABLE_NAME)
        .addSimpleTag("test_category", "simple_tag_key", "simple_tag_value");
    Map<String, Map<String, String>> categoryToKvs = odps.tables().get(SOURCE_TABLE_NAME).getSimpleTags();
    assertEquals(1, categoryToKvs.size());
    assertNotNull(categoryToKvs.get("test_category"));
    assertTrue(categoryToKvs.get("test_category").containsKey("simple_tag_key"));
    assertEquals("simple_tag_value",
                 categoryToKvs.get("test_category").get("simple_tag_key"));

    // Remove table level simple tags
    odps.tables()
        .get(SOURCE_TABLE_NAME)
        .removeSimpleTag("test_category", "simple_tag_key", "simple_tag_value");
    categoryToKvs = odps.tables().get(SOURCE_TABLE_NAME).getSimpleTags();
    assertEquals(0, categoryToKvs.size());

    // There shouldn't be any column level simple tag
    assertEquals(0, table.getSimpleTags("c1").size());

    // Create column level simple tags
    List<String> columns = new LinkedList<>();
    columns.add("c1");
    odps.tables()
        .get(SOURCE_TABLE_NAME)
        .addSimpleTag(
            "test_category",
            "simple_tag_key",
            "simple_tag_value",
            columns);
    categoryToKvs = odps.tables()
                        .get(SOURCE_TABLE_NAME)
                        .getSimpleTags("c1");
    assertEquals(1, categoryToKvs.size());
    assertNotNull(categoryToKvs.get("test_category"));
    assertTrue(categoryToKvs.get("test_category").containsKey("simple_tag_key"));
    assertEquals("simple_tag_value",
                 categoryToKvs.get("test_category").get("simple_tag_key"));

    // Remove column level simple tags
    odps.tables()
        .get(SOURCE_TABLE_NAME)
        .removeSimpleTag(
            "test_category",
            "simple_tag_key",
            "simple_tag_value",
            columns);
    categoryToKvs = odps.tables().get(SOURCE_TABLE_NAME).getSimpleTags();
    assertEquals(0, categoryToKvs.size());
  }

  @Test
  public void testGetPartitionIterator() {
    Table table = odps.tables().get(PARTITIONED_TABLE_NAME);
    Iterator<Partition> iter =
        table.getPartitionIterator(null, false, 1L, null);
    List<Partition> partitions = new ArrayList<>(3);
    while (iter.hasNext()) {
      partitions.add(iter.next());
    }
    Assert.assertEquals(3L, partitions.size());
    assertEquals("p1='1',p2='bar'", partitions.get(0).getPartitionSpec().toString());
    assertEquals("p1='1',p2='baz'", partitions.get(1).getPartitionSpec().toString());
    assertEquals("p1='1',p2='foo'", partitions.get(2).getPartitionSpec().toString());
  }

  @Test
  public void testGetReversedPartitionIterator() {
    Table table = odps.tables().get(PARTITIONED_TABLE_NAME);
    Iterator<Partition> iter =
        table.getPartitionIterator(null, false, 1L, null);
    List<Partition> partitions = new ArrayList<>(3);
    while (iter.hasNext()) {
      partitions.add(iter.next());
    }
    Assert.assertEquals(3L, partitions.size());
    assertEquals("p1='1',p2='foo'", partitions.get(2).getPartitionSpec().toString());
    assertEquals("p1='1',p2='baz'", partitions.get(1).getPartitionSpec().toString());
    assertEquals("p1='1',p2='bar'", partitions.get(0).getPartitionSpec().toString());
  }

  @Test
  public void testGetLimitedPartitionIterator() {
    Table table = odps.tables().get(PARTITIONED_TABLE_NAME);
    Iterator<Partition> iter =
        table.getPartitionIterator(null, false, 1L, 1L);
    long numPartitions = 0;
    while (iter.hasNext()) {
      numPartitions += 1;
      Partition p = iter.next();
      Assert.assertEquals("p1='1',p2='bar'", p.getPartitionSpec().toString());
    }
    Assert.assertEquals(1L, numPartitions);
  }

  @Test
  public void testGetLimitedReversedPartitionIterator() {
    Table table = odps.tables().get(PARTITIONED_TABLE_NAME);
    Iterator<Partition> iter =
        table.getPartitionIterator(null, true, 1L, 1L);
    long numPartitions = 0;
    while (iter.hasNext()) {
      numPartitions += 1;
      Partition p = iter.next();
      Assert.assertEquals("p1='1',p2='foo'", p.getPartitionSpec().toString());
    }
    Assert.assertEquals(1L, numPartitions);
  }

  @Test
  public void testGetObjectTags() throws OdpsException {
    Table table = odps.tables().get(TABLE_NAME_4);

    PartitionSpec spec = new PartitionSpec("p1=1,p2=foo");
    table.createPartition(spec, true);

    Assert.assertEquals(0, table.getPartition(spec).getSimpleTags().size());
    Assert.assertEquals(0, table.getSimpleTags().size());


    //schema Revolution
    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.allow.schema.evolution", "true");
    String sql = "alter table " + TABLE_NAME_4 + " drop column c1;";
    SQLTask.run(odps, odps.getDefaultProject(), sql, hints, null).waitForSuccess();
    Assert.assertEquals(0, table.getPartition(spec).getSimpleTags().size());
    Assert.assertEquals(0, table.getSimpleTags().size());

    // new type
    sql = "alter table " + TABLE_NAME_4 + " add column(dd1 date);";
    hints.clear();
    hints.put("odps.sql.type.system.odps2", "true");
    SQLTask.run(odps, odps.getDefaultProject(), sql, hints, null).waitForSuccess();
    Assert.assertEquals(0, table.getPartition(spec).getSimpleTags().size());
    Assert.assertEquals(0, table.getSimpleTags().size());


  }

  @Test
  public void testMvProperties() {
    Table table = odps.tables().get(TABLE_NAME);

    if (!table.isMaterializedView()) {
      Assert.assertFalse(table.isAutoRefreshEnabled());
      Assert.assertNull(table.isAutoSubstituteEnabled());
      Assert.assertNull(table.getRefreshInterval());
      Assert.assertNull(table.getRefreshCron());
    }

    table.isAutoRefreshEnabled();
    table.isAutoSubstituteEnabled();
    table.getRefreshInterval();
    table.getRefreshCron();
  }


  @Test
  public void testMvGetHistroy() {
    Table table = odps.tables().get(TABLE_NAME);

    if (!table.isMaterializedView()) {
      Assert.assertNull(table.getRefreshHistory());
    }

    table.getRefreshHistory();
  }

  @Test
  public void test() {
    Table table = odps.tables().get(TABLE_NAME);
    Assert.assertFalse(table.hasRowAccessPolicy());
  }


  @Test
  public void testLastAccessTime() {
    Table table = odps.tables().get(TABLE_NAME);
    table.getLastDataAccessTime();
  }
}
