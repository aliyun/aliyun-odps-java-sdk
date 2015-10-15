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

package com.aliyun.odps.task;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.data.Record;

/**
 * Created by nizheming on 15/4/24.
 */
public class SQLTaskTest extends TestBase {
  @Test
  public void testCreatePriority() throws OdpsException {
    Instance i = SQLTask.run(odps, odps.getDefaultProject(), "select * from src;","testsql", null, null,
                        3);
    assertEquals(i.getPriority(), 3);
  }

  @Test
  public void testSelectSQLTask() throws OdpsException, IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    try {
      odps.tables().create("test_select_sql_result", schema);
    } catch (OdpsException e) {
    }

    String taskName = "test_select_sql_task";
    Instance i = SQLTask.run(odps, odps.getDefaultProject(),
                             "select * from test_select_sql_result;", taskName, null, null, 3);
    i.waitForSuccess();
    List<Record> records = SQLTask.getResult(i, taskName);
    assertEquals(0, records.size());

    i = SQLTask.run(odps, odps.getDefaultProject(), "select * from test_select_sql_result;", null, null);
    i.waitForSuccess();
    records = SQLTask.getResult(i);
    assertEquals(0, records.size());
  }
}