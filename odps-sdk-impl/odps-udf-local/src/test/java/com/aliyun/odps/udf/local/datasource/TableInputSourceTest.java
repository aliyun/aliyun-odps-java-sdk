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

package com.aliyun.odps.udf.local.datasource;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.udf.local.LocalRunException;

public class TableInputSourceTest {

  TableInputSource tableInputSource;

  @Test
  public void testGetNextRow() throws IOException, LocalRunException {
    // ////////////////////////////
    String project = "project_name";
    String table = "wc_in2";
    String[] partitions = null;
    String[] columns = null;
    tableInputSource = new TableInputSource(project, table, partitions, columns);

    Object[] data;
    int count = 0;
    while ((data = tableInputSource.getNextRow()) != null) {
      count++;
    }

    Assert.assertEquals(10, count);

    // ////////////////////////////
    project = "project_name";
    table = "wc_in2";
    partitions = new String[]{"p2=2"};
    columns = null;
    tableInputSource = new TableInputSource(project, table, partitions, columns);

    count = 0;
    while ((data = tableInputSource.getNextRow()) != null) {
      count++;
    }

    Assert.assertEquals(6, count);

    // ////////////////////////////
    project = "project_name";
    table = "wc_in2";
    partitions = new String[]{"p2=1", "p1=2"};
    columns = null;
    tableInputSource = new TableInputSource(project, table, partitions, columns);

    count = 0;
    while ((data = tableInputSource.getNextRow()) != null) {
      count++;
    }

    Assert.assertEquals(3, count);

    // ////////////////////////////
    project = "project_name";
    table = "wc_in2";
    partitions = new String[]{"p2=2"};
    columns = new String[]{"colc", "cola"};
    tableInputSource = new TableInputSource(project, table, partitions, columns);

    count = 0;
    while ((data = tableInputSource.getNextRow()) != null) {
      count++;
    }

    Assert.assertEquals(6, count);

    // ////////////////////////////
    project = "project_name";
    table = "wc_in1";
    partitions = null;
    columns = new String[]{"col2", "col3"};
    tableInputSource = new TableInputSource(project, table, partitions, columns);

    count = 0;
    while ((data = tableInputSource.getNextRow()) != null) {
      count++;
    }

    Assert.assertEquals(4, count);

  }

}
