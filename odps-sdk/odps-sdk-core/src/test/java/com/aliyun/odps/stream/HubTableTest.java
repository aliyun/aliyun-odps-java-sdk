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

package com.aliyun.odps.stream;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.OdpsTestUtils;

public class HubTableTest {

  private static Odps odps = OdpsTestUtils.newDefaultOdps();
  private static String uuid = String.valueOf(System.currentTimeMillis());
  private static String projectName = odps.getDefaultProject();
  private static String tableName = "hubtable_" + uuid;
  private static boolean enableHubTable = true;
  private static long hubLifeCycle = 2l;
  private static long shardCount = 1l;

  //生成table schema的公共方法
  public TableSchema createTableSchema() throws OdpsException, IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("key", OdpsType.STRING));
    schema.addPartitionColumn(new Column("partition1", OdpsType.STRING));
    return schema;
  }

  //创建1200列的table，创建成功
  @Test
  public void createHubtableWithMaxColumn() throws OdpsException, IOException {
    int columnCount = 1200;
    TableSchema schema = new TableSchema();
    for (int i = 1; i <= 1200; i++) {
      schema.addColumn(new Column("key" + i, OdpsType.STRING));
    }
    schema.addPartitionColumn(new Column("partition1", OdpsType.STRING));
    odps.tables().create(projectName, tableName, schema);
    odps.tables().get(tableName).createShards(shardCount, enableHubTable, hubLifeCycle);
    Assert.assertEquals(columnCount, odps.tables().get(tableName).getSchema().getColumns().size());
    odps.tables().delete(projectName, tableName);
  }

  //创建超过1200列的table，创建失败
  @Test
  public void createHubtableBeyondMaxColumn() throws OdpsException, IOException {
    TableSchema schema = new TableSchema();
    for (int i = 1; i <= 1201; i++) {
      schema.addColumn(new Column("key" + i, OdpsType.STRING));
    }
    schema.addPartitionColumn(new Column("partition1", OdpsType.STRING));

    String actual = "number of column exceeded limit of 1200 in table";
    try {
      odps.tables().create(projectName, tableName, schema);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.toString().contains(actual));
      System.out.println("actual result is " + e.toString());
    }
  }

  //创建Hub table,设置hublifecycle设置非法值：小于1
  @Test
  public void alterHubtableWithIllegalShard() throws OdpsException, IOException {
    long shardCount = 0l;
    TableSchema schema = createTableSchema();

    String actual = "Shard number should be bigger than zero";
    try {
      odps.tables().create(projectName, tableName, schema);
      odps.tables().get(tableName).createShards(shardCount, enableHubTable, hubLifeCycle);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.toString().contains(actual));
      System.out.println("actual result is " + e.toString());
    }

    odps.tables().delete(projectName, tableName);
  }

  //shard数设定后，不能通过alter hub table进行修改
  @Test
  public void alterHubtableTwice() throws OdpsException, IOException {
    TableSchema schema = createTableSchema();
    odps.tables().create(projectName, tableName, schema);
    odps.tables().get(tableName).createShards(shardCount, enableHubTable, hubLifeCycle);

    String actual = "can not enable hubtable with a existing hubtable";
    try {
      odps.tables().get(tableName).createShards(shardCount, enableHubTable, hubLifeCycle);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.toString().contains(actual));
      System.out.println("actual result is " + e.toString());
    }

    odps.tables().delete(projectName, tableName);
  }

  //创建hubtable包含各种column：boolean, bigint, double, string, datetime,创建成功
  @Test
  public void createTableWithAllColumnType() throws OdpsException, IOException {
    int columnCount = 5;
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("key1", OdpsType.BIGINT));
    schema.addColumn(new Column("key2", OdpsType.BOOLEAN));
    schema.addColumn(new Column("key3", OdpsType.DATETIME));
    schema.addColumn(new Column("key5", OdpsType.DOUBLE));
    schema.addColumn(new Column("key6", OdpsType.STRING));
    schema.addPartitionColumn(new Column("partition1", OdpsType.STRING));
    odps.tables().create(projectName, tableName, schema);
    odps.tables().get(tableName).createShards(shardCount, enableHubTable, hubLifeCycle);
    Assert.assertEquals(columnCount, odps.tables().get(tableName).getSchema().getColumns().size());
    odps.tables().delete(projectName, tableName);
  }

  //alter非hub table(无hublifecycle属性)设置shard数,可以成功
  @Test
  public void alterNonHubtable() throws OdpsException, IOException {
    TableSchema schema = createTableSchema();
    odps.tables().create(projectName, tableName, schema);
    odps.tables().get(tableName).createShards(shardCount, false, -1);
    Assert.assertTrue(odps.tables().exists(projectName, tableName));
    odps.tables().delete(projectName, tableName);
  }
}
