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

package com.aliyun.odps.sqa.commandapi;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.TypeInfoFactory;

public class ArchiveCommandTest extends TestBase {

  private static final String TEST_TABLE_NAME = "archive_task_test_table_name";

  @BeforeClass
  public static void createTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("col2", TypeInfoFactory.STRING));
    schema.addColumn(new Column("col3", TypeInfoFactory.STRING));
    schema.addColumn(new Column("col4", TypeInfoFactory.STRING));
    odps.tables().create(TEST_TABLE_NAME, schema);
  }

  @AfterClass
  public static void deleteTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }
  }

  @Test
  public void archiveCommandTest() throws OdpsException {

    Instance instance = SQLTask.run(odps,
        "insert into " + TEST_TABLE_NAME
            + " (col1, col2, col3, col4) values (10L, 'merge_task_test', 'foo', 'bar'), (10L, 'merge_task_test', 'foo', 'bar'), "
            + "(10L, 'merge_task_test', 'foo', 'bar'), (10L, 'merge_task_test', 'foo', 'bar'), (10L, 'merge_task_test', 'foo', 'bar'),"
            + "(10L, 'merge_task_test', 'foo', 'bar'), (10L, 'merge_task_test', 'foo', 'bar'), (10L, 'merge_task_test', 'foo', 'bar');");
    instance.waitForSuccess();

    String archiveCommand = "alter table " + TEST_TABLE_NAME + " archive;";
    CommandTestUtil.runCommandAndGetResult(odps, archiveCommand);
    Assert.assertTrue(odps.tables().get(TEST_TABLE_NAME).isArchived());
  }
}