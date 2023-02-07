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
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * 测试包括三个方面： 1. 语法解析是否可以通过 2. 通过语法解析的正确的command能否返回正确的结果 3. 通过语法解析但是错误的command能否及时抛异常
 */
public class DescribeInstanceCommandTest extends TestBase {

  private static final String TEST_TABLE_NAME = "desc_instance_test_table_name";

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
  public void describeInstanceCommandTest() throws OdpsException {
    String[] instances = new String[3];
    Instance instance = SQLTask.run(odps,
        "insert into " + TEST_TABLE_NAME + " (col1, col2) values (10L, 'merge_task_test');");
    instance.waitForSuccess();
    instances[0] = instance.getId();
    instance = SQLTask.run(odps, "select * from " + TEST_TABLE_NAME + ";");
    instance.waitForSuccess();
    instances[1] = instance.getId();
    instance = SQLTask.run(odps, "select count(*) from " + TEST_TABLE_NAME + ";");
    instance.waitForSuccess();
    instances[2] = instance.getId();

    // 1. 测试语法解析是否可以通过
    String[] positiveCommands = new String[]{
        "desc instance " + instances[0] + " ;",
        "DEsc INSTANCE " + instances[1] + " ;",
        "describe INSTANCE " + instances[2] + " ;"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = new String[]{
        "desc instances " + instances[0] + " ;",
        "descr instance  " + instances[1] + " ;",
        "describ instance " + instances[2] + " ;"
    };
    Assert.assertEquals(negativeCommands.length, CommandTestUtil.getErrorNum(negativeCommands));

    // 2. 测试通过语法解析的正确的command是否能返回正确的结果
    int numOfNull = 0;
    int index = 0;
    for (String positiveCommand : positiveCommands) {
      List<Record> recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommand);
      if (recordList == null || recordList.size() == 0) {
        numOfNull++;
        continue;
      }
      for (Record record : recordList) {
        Assert.assertEquals(record.get(0), odps.instances().get(instances[index]).getId());
        Assert.assertEquals(record.get(1), odps.instances().get(instances[index]).getOwner());
        Assert.assertEquals(record.get(4),
            odps.instances().get(instances[index]).getStatus().toString());
      }
      index++;
    }
    Assert.assertEquals(numOfNull, 0);

    numOfNull = 0;
    index = 0;
    for (String positiveCommand : positiveCommands) {
      ResultSet resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommand);
      if (resultSet == null) {
        numOfNull++;
        continue;
      }
      while (resultSet.hasNext()) {
        Record record = resultSet.next();
        Assert.assertEquals(record.get(0), odps.instances().get(instances[index]).getId());
        Assert.assertEquals(record.get(1), odps.instances().get(instances[index]).getOwner());
        Assert.assertEquals(record.get(4),
            odps.instances().get(instances[index]).getStatus().toString());
      }
      index++;
    }
    Assert.assertEquals(numOfNull, 0);

    // 3. 测试通过语法解析的错误的command能否及时抛出异常退出
    String[] errorCommands = new String[]{
        "desc instance xxx;",
        "desc instance 1234abc;"
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