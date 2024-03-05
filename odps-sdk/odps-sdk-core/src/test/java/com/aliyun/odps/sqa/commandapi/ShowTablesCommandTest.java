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

import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * 测试包括三个方面： 1. 语法解析是否可以通过 2. 通过语法解析的正确的command能否返回正确的结果 3. 通过语法解析但是错误的command能否及时抛异常
 */
public class ShowTablesCommandTest extends TestBase {

  private static final String TEST_PROJECT_NAME = odps.getDefaultProject();
  private static final String TEST_TABLE_NAME = "show_tables_test_table_name";
  private static final String TEST_PROJECT_NAME_NOT_EXIST = odps.getDefaultProject() + "_not_exist";

  @BeforeClass
  public static void createTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("col2", TypeInfoFactory.STRING));
    odps.tables().create(TEST_TABLE_NAME, schema);
  }

  @AfterClass
  public static void deleteTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }
  }

  @Test
  public void showTablesCommandTest() {
    // 1. 测试语法解析是否可以通过
    String[] positiveCommands = new String[]{
        "show tables like " + TEST_TABLE_NAME + " ;",
        "SHOW tables IN " + TEST_PROJECT_NAME + " ;",
        "show TABLES from " + TEST_PROJECT_NAME + " LIKE " + TEST_TABLE_NAME + " ;"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = new String[]{
        "show table like " + TEST_TABLE_NAME + " ;",
        "SHOW tables IN " + TEST_PROJECT_NAME + ".;",
        "show TABLES from " + TEST_PROJECT_NAME + " LIKE ;"
    };
    Assert.assertEquals(negativeCommands.length, CommandTestUtil.getErrorNum(negativeCommands));

    // 2. 测试通过语法解析的正确的command是否能返回正确的结果
    List<Record> recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommands[0]);
    Assert.assertNotNull(recordList);
    Assert.assertEquals(recordList.size(), 1);
    Assert.assertEquals(recordList.get(0).get(1), TEST_TABLE_NAME);

    recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommands[1]);
    Assert.assertNotNull(recordList);
    Assert.assertNotEquals(recordList.size(), 0);

    recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommands[2]);
    Assert.assertNotNull(recordList);
    Assert.assertEquals(recordList.size(), 1);
    Assert.assertEquals(recordList.get(0).get(1), TEST_TABLE_NAME);

    ResultSet resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommands[0]);
    Assert.assertNotNull(resultSet);
    Assert.assertEquals(resultSet.next().get(1), TEST_TABLE_NAME);

    resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommands[1]);
    Assert.assertNotNull(resultSet);

    resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommands[2]);
    Assert.assertNotNull(resultSet);
    Assert.assertEquals(resultSet.next().get(1), TEST_TABLE_NAME);

    // 3. 测试通过语法解析的错误的command能否及时抛出异常退出
    String[] errorCommands = new String[]{
        "SHOW tables in " + TEST_PROJECT_NAME_NOT_EXIST + " ;",
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(errorCommands));
    int num = 0;
    for (String errorCommand : errorCommands) {
      try {
        CommandTestUtil.runErrorCommand(odps, errorCommand);
      } catch (Exception e) {
        num++;
      }
    }
    Assert.assertEquals(errorCommands.length, num);
  }
}