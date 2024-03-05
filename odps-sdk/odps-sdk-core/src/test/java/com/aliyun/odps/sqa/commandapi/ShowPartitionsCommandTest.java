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
public class ShowPartitionsCommandTest extends TestBase {

  private static final String TEST_TABLE_NAME = "show_partitions_test_table_name";
  private static final String PARTITION_SPEC_1 = "p1='part1',p2='part2'";
  private static final String PARTITION_SPEC_2 = "p1='part11',p2='part22'";

  @BeforeClass
  public static void createTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("col2", TypeInfoFactory.STRING));
    schema.addPartitionColumn(new Column("p1", TypeInfoFactory.STRING));
    schema.addPartitionColumn(new Column("p2", TypeInfoFactory.STRING));
    odps.tables().create(TEST_TABLE_NAME, schema, true);
    odps.tables().get(TEST_TABLE_NAME).createPartition(new PartitionSpec(PARTITION_SPEC_1), true);
    odps.tables().get(TEST_TABLE_NAME).createPartition(new PartitionSpec(PARTITION_SPEC_2), true);
  }

  @AfterClass
  public static void deleteTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }
  }

  @Test
  public void showPartitionsCommandTest() {
    String[] positiveCommands = new String[]{
        "show partitions " + TEST_TABLE_NAME + " ;",
        "show PARTITIONS " + TEST_TABLE_NAME + " partition(p1='part1',p2='part2');",
        "SHOW partitions " + TEST_TABLE_NAME + " PARTITIon(p1='part11',p2='part22');"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = new String[]{
        "show partition " + TEST_TABLE_NAME + " ;",
        "show PARTITIONS " + TEST_TABLE_NAME + "partition(p1=part1);",
        "SHOW partitions " + "PARTITIon(p2='part2');"
    };
    Assert.assertEquals(negativeCommands.length, CommandTestUtil.getErrorNum(negativeCommands));
    // 2. 测试通过语法解析的正确的command是否能返回正确的结果
    List<Record> recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommands[0]);
    Assert.assertNotNull(recordList);
    Assert.assertEquals(recordList.size(), 2);

    recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommands[1]);
    Assert.assertNotNull(recordList);
    Assert.assertEquals(recordList.get(0).get(0), "p1=part1/p2=part2");

    recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommands[2]);
    Assert.assertNotNull(recordList);
    Assert.assertEquals(recordList.get(0).get(0), "p1=part11/p2=part22");

    ResultSet resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommands[0]);
    Assert.assertNotNull(resultSet);

    resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommands[1]);
    Assert.assertNotNull(resultSet);
    Assert.assertEquals(resultSet.next().get(0), "p1=part1/p2=part2");

    resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommands[2]);
    Assert.assertNotNull(resultSet);
    Assert.assertEquals(resultSet.next().get(0), "p1=part11/p2=part22");

    String[] errorCommands = new String[]{
        "show PARTITIONS not_exist_table;",
        "show PARTITIONS not_exist_table partition(p0='1234');",
        "show PARTITIONS not_exist_table partition(not_exist_partition='no');"
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