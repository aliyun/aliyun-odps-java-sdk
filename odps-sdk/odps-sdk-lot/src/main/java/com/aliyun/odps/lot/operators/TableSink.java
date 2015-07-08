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
import com.aliyun.odps.lot.common.PartitionSpecification;

import apsara.odps.lot.DataSinkProtos;
import apsara.odps.lot.Lot;
import apsara.odps.lot.TableSinkProtos;

public class TableSink extends DataSink {

  private String project;
  private String table;
  private boolean overwrite;
  private PartitionSpecification partition;

    /*
    不在这里支持dynamic partition
     */

  public String getProject() {
    return project;
  }

  public String getTable() {
    return table;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public PartitionSpecification getPartition() {
    return partition;
  }

  public TableSink(String project, String table, boolean overwrite,
                   PartitionSpecification partition) {
    if (project == null) {
      throw new ArgumentNullException("project");
    }

    if (table == null) {
      throw new ArgumentNullException("table");
    }

    this.partition = partition;
    this.project = project;
    this.table = table;
    this.overwrite = overwrite;

  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 1);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();

    DataSinkProtos.DataSink.Builder ds = DataSinkProtos.DataSink.newBuilder();
    ds.setId(getId());
    ds.setParentId(getParents().get(0).getId());

    TableSinkProtos.TableSink.Builder ts = TableSinkProtos.TableSink.newBuilder();
    ts.setProject(project);
    ts.setTable(table);
    ts.setIsOverwrite(overwrite);
    if (partition != null) {
      ts.setPartition(partition.toProtoBuf());
    }

    ds.setTableSink(ts.build());
    builder.setDataSink(ds.build());

    return builder.build();
  }

}
