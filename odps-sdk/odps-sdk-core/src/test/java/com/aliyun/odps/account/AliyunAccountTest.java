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

package com.aliyun.odps.account;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account.AccountProvider;

public class AliyunAccountTest {

  private static TableSchema schema;
  String tableName = "table_name_for_test";

  Odps odps;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addColumn(new Column("c2", OdpsType.BOOLEAN));
    schema.addColumn(new Column("c3", OdpsType.DATETIME));
    schema.addColumn(new Column("c4", OdpsType.STRING));

    schema.addPartitionColumn(new Column("p1", OdpsType.BIGINT));
    schema.addPartitionColumn(new Column("p2", OdpsType.STRING));

  }

  @Test(expected = IllegalArgumentException.class)
  public void testAliyunNullID() throws Exception {
    odps = createOdpsNull();
    odps.tables().create(odps.getDefaultProject(), tableName, schema);
  }

  @Test
  public void testGetType() {
    AliyunAccount account = new AliyunAccount("a", "b");
    assertEquals(account.getType(), AccountProvider.ALIYUN);
  }

  public static Odps createOdpsNull() {
    String endpoint = "https://service.odps.aliyun.com/api";
    String project = "sandovaltest";
    Odps odps = null;
    Account account = new AliyunAccount(null, null);
    odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);
    return odps;
  }

}
