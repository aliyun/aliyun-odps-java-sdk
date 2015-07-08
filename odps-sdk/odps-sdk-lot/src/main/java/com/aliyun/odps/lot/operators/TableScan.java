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

package com.aliyun.odps.lot.operators;


import com.aliyun.odps.lot.common.ArgumentNullException;

import apsara.odps.lot.DataSourceProtos;
import apsara.odps.lot.Lot;
import apsara.odps.lot.TableScanProtos;

public class TableScan extends DataSource {

  private String project;
  private String table;

  public TableScan(String project, String table) {
    if (project == null) {
      throw new ArgumentNullException("project");
    }

    if (table == null) {
      throw new ArgumentNullException("table");
    }

    this.project = project;
    this.table = table;
  }

  public String getProject() {
    return this.project;
  }

  public String getTable() {
    return this.table;
  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();
    DataSourceProtos.DataSource.Builder ds = DataSourceProtos.DataSource.newBuilder();
    ds.setId(getId());

    TableScanProtos.TableScan.Builder ts = TableScanProtos.TableScan.newBuilder();
    ts.setProject(project);
    ts.setTable(table);

    ds.setTableScan(ts.build());
    builder.setDataSource(ds.build());

    return builder.build();
  }
}
