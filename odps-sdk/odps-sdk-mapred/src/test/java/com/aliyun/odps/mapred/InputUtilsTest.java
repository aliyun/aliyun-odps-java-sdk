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

package com.aliyun.odps.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;

public class InputUtilsTest {

  @Test
  public void testAddInputTable() {
    JobConf conf = new JobConf();
    TableInfo table = TableInfo.builder().tableName("t_in").build();
    InputUtils.addTable(table, conf);
    TableInfo[] tables = InputUtils.getTables(conf);
    assertEquals(tables.length, 1);
    assertEquals(table, tables[0]);
  }

  @Test
  public void testSetInputTables() {
    JobConf conf = new JobConf();
    TableInfo table = TableInfo.builder().tableName("t_in").build();
    InputUtils.setTables(null, conf);
    TableInfo[] tables = InputUtils.getTables(conf);
    assertNull(tables);
    InputUtils.setTables(new TableInfo[0], conf);
    tables = InputUtils.getTables(conf);
    assertNull(tables);
    InputUtils.addTable(table, conf);
    tables = InputUtils.getTables(conf);
    assertEquals(tables.length, 1);
    assertEquals(table, tables[0]);

    InputUtils.setTables(null, conf);
    tables = InputUtils.getTables(conf);
    assertNull(tables);
  }
}
