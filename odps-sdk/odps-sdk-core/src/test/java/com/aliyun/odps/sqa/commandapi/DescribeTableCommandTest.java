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
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * 测试包括三个方面： 1. 语法解析是否可以通过 2. 通过语法解析的正确的command能否返回正确的结果 3. 通过语法解析但是错误的command能否及时抛异常
 */
public class DescribeTableCommandTest extends TestBase {

  private static final String TEST_TABLE_NAME = "desc_table_test_table_name";
  private static final String TEST_PARTITION_NAME = "pt1='hallo'";
  private static final String TEST_TABLE_NAME_NOT_EXIST = "desc_table_test_table_name_not_exist";

  @BeforeClass
  public static void createTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("col2", TypeInfoFactory.STRING));
    schema.addPartitionColumn(new Column("pt1", TypeInfoFactory.STRING));
    odps.tables().create(TEST_TABLE_NAME, schema);

    odps.tables().get(TEST_TABLE_NAME).createPartition(new PartitionSpec(TEST_PARTITION_NAME), true);
  }

  @AfterClass
  public static void deleteTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }
  }

  @Test
  public void describeTableCommandTest() {
    String[] positiveCommands = new String[]{
        "desc extended " + TEST_TABLE_NAME + " ;",
        "DESC " + TEST_TABLE_NAME + " ;",
        "describe EXTENDED " + TEST_TABLE_NAME + " ;"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = new String[]{
        "desc -extended " + TEST_TABLE_NAME + " ;",
        "descr " + TEST_TABLE_NAME + " ;",
        "describe " + TEST_TABLE_NAME + ""
    };
    Assert.assertEquals(negativeCommands.length, CommandTestUtil.getErrorNum(negativeCommands));

    int numOfNull = 0;
    for (String positiveCommand : positiveCommands) {
      List<Record> recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommand);
      if (recordList == null || recordList.size() == 0) {
        numOfNull++;
        continue;
      }
      for (Record record : recordList) {
        Assert.assertEquals(record.get(0), odps.tables().get(TEST_TABLE_NAME).getOwner());
        Assert.assertEquals(record.get(1), odps.tables().get(TEST_TABLE_NAME).getProject());
        Assert.assertEquals(record.get(4), odps.tables().get(TEST_TABLE_NAME).getCreatedTime());
      }
    }
    Assert.assertEquals(numOfNull, 0);

    numOfNull = 0;
    for (String positiveCommand : positiveCommands) {
      ResultSet resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommand);
      if (resultSet == null) {
        numOfNull++;
        continue;
      }
      while (resultSet.hasNext()) {
        Record record = resultSet.next();
        Assert.assertEquals(record.get(0), odps.tables().get(TEST_TABLE_NAME).getOwner());
        Assert.assertEquals(record.get(1), odps.tables().get(TEST_TABLE_NAME).getProject());
        Assert.assertEquals(record.get(4), odps.tables().get(TEST_TABLE_NAME).getCreatedTime());
      }
    }
    Assert.assertEquals(numOfNull, 0);

    String[] errorCommands = new String[]{
        "desc " + TEST_TABLE_NAME_NOT_EXIST + " ;",
        "desc extended " + TEST_TABLE_NAME_NOT_EXIST + " ;"
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

  @Test
  public void describeTableTest() {
    List<Record>
        records =
        CommandTestUtil.runCommandAndGetResult(odps, "desc " + TEST_TABLE_NAME + " ;");
    Assert.assertNotNull(records.get(0).get("metadatajson"));


    records =
        CommandTestUtil.runCommandAndGetResult(odps, "desc extended " + TEST_TABLE_NAME + " ;");
    Assert.assertNotNull(records.get(0).get("metadatajson"));
    Assert.assertNotNull(records.get(0).get("extendedInfojson"));
  }

  @Test
  public void describePartitionTest() {
    List<Record>
        records =
        CommandTestUtil.runCommandAndGetResult(odps, "desc " + TEST_TABLE_NAME + " partition(" + TEST_PARTITION_NAME + ") ;");
    Assert.assertNotNull(records.get(0).get("metadatajson"));

    records =
        CommandTestUtil.runCommandAndGetResult(odps, "desc extended " + TEST_TABLE_NAME + " partition(" + TEST_PARTITION_NAME + ") ;");
    Assert.assertNotNull(records.get(0).get("metadatajson"));
    Assert.assertNotNull(records.get(0).get("extendedInfojson"));
  }
}