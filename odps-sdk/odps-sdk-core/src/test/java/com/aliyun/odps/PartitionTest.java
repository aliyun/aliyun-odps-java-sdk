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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Classification.AttributeDefinition;
import com.aliyun.odps.Classification.BooleanAttributeDefinition;
import com.aliyun.odps.Classification.EnumAttributeDefinition;
import com.aliyun.odps.Classification.IntegerAttributeDefinition;
import com.aliyun.odps.Classification.StringAttributeDefinition;
import com.aliyun.odps.Tags.TagBuilder;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;

public class PartitionTest extends TestBase {

  private static String TABLE_NAME = "test_partition_test";

  private static final String BASE_CLASSIFICATION_NAME_PREFIX = PartitionTest.class.getSimpleName();
  private static final String BASE_TAG_NAME_PREFIX = PartitionTest.class.getSimpleName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String tableName = TABLE_NAME;
    odps.tables().delete(TABLE_NAME, true);
    if (!odps.tables().exists(tableName)) {
      TableSchema schema = new TableSchema();
      schema.addColumn(new Column("key", OdpsType.STRING));
      schema.addColumn(new Column("value", OdpsType.STRING));

      schema.addPartitionColumn(new Column("pt", OdpsType.STRING));
      schema.addPartitionColumn(new Column("ds", OdpsType.STRING));

      odps.tables().create(tableName, schema);

      PartitionSpec partitionSpec = new PartitionSpec("pt='1',ds='1'");
      odps.tables().get(tableName).createPartition(partitionSpec);

      TableTunnel tunnel = OdpsTestUtils.newTableTunnel(odps);
      TableTunnel.UploadSession session = tunnel.createUploadSession(odps.getDefaultProject(),
                                                                     tableName, partitionSpec);
      TunnelRecordWriter rw = (TunnelRecordWriter) session.openRecordWriter(0L);
      Record record;
      for (int i = 0; i < 20; i++) {
        record = session.newRecord();
        record.set(0, "key" + i);
        record.set(1, "val" + i);
        rw.write(record);
      }
      rw.close();
      Long[] blocks = {0L};
      session.commit(blocks);
    }
  }

  @Test
  public void testPartition() {
    Table table = odps.tables().get(TABLE_NAME);
    Partition partition = table.getPartition(new PartitionSpec("pt='1',ds='1'"));
    Assert.assertTrue("size must > 0", partition.getSize() > 0);
    Assert.assertTrue("physical size must > 0", partition.getPhysicalSize() > 0);
    partition.getCreatedTime();
    partition.getLastMetaModifiedTime();
    partition.getLastDataModifiedTime();
    partition.getLastDataAccessTime();
  }

  @Test
  public void testGetExtendInfo() throws OdpsException {
    Table table = odps.tables().get(TABLE_NAME);
    Partition partition = table.getPartition(new PartitionSpec("pt='1', ds='1'"));
    Assert.assertTrue("size must > 0", partition.getSize() > 0);
    Assert.assertTrue(!partition.isArchived());
    partition.isExstore();
    assertEquals(partition.getLifeCycle(), -1L);
    partition.getPhysicalSize();
    partition.getFileNum();
    SQLTask.run(odps, "alter table " + TABLE_NAME + " set lifecycle 10;").waitForSuccess();
    table.reload();
    partition = table.getPartition(new PartitionSpec("pt='1', ds='1'"));
    assertEquals(partition.getLifeCycle(), 10L);

    SQLTask.run(odps, "alter table " + TABLE_NAME + " partition (" + partition.getPartitionSpec().toString() + ") disable lifecycle;").waitForSuccess();
    partition = table.getPartition(new PartitionSpec("pt='1', ds='1'"));
    assertEquals(partition.getLifeCycle(), -1L);
    SQLTask.run(odps, "alter table " + TABLE_NAME + " partition (" + partition.getPartitionSpec().toString() + ") enable lifecycle;").waitForSuccess();
    partition = table.getPartition(new PartitionSpec("pt='1', ds='1'"));
    assertEquals(partition.getLifeCycle(), 10L);
  }

  @Test
  public void testSimpleTag() throws OdpsException {
    Table table = odps.tables().get(TABLE_NAME);
    Partition partition = table.getPartition(new PartitionSpec("pt='1', ds='1'"));

    // There shouldn't be any partition level simple tag
    Assert.assertEquals(0, partition.getSimpleTags().size());

    // Create partition level simple tags
    partition.addSimpleTag("test_category", "simple_tag_key", "simple_tag_value");
    partition.reload();
    Map<String, Map<String, String>> categoryToKvs = partition.getSimpleTags();
    assertEquals(1, categoryToKvs.size());
    assertNotNull(categoryToKvs.get("test_category"));
    assertTrue(categoryToKvs.get("test_category").containsKey("simple_tag_key"));
    assertEquals("simple_tag_value",
                 categoryToKvs.get("test_category").get("simple_tag_key"));

    // Create column level simple tags
    List<String> columns = new LinkedList<>();
    columns.add("key");
    partition.addSimpleTag(
            "test_category",
            "simple_tag_key",
            "simple_tag_value",
            columns);
    partition.reload();
    categoryToKvs = partition.getSimpleTags("key");
    assertEquals(1, categoryToKvs.size());
    assertNotNull(categoryToKvs.get("test_category"));
    assertTrue(categoryToKvs.get("test_category").containsKey("simple_tag_key"));
    assertEquals("simple_tag_value",
                 categoryToKvs.get("test_category").get("simple_tag_key"));
  }
}
