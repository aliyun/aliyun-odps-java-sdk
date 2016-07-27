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

package com.aliyun.odps.local.common;

import java.util.Arrays;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Table;

public class TableMeta {

  private String projName;

  private String tableName;

  private Column[] cols;

  private Column[] partitions;

  public TableMeta(String projName, String tableName, Column[] cols) {
    this.projName = projName;
    this.tableName = tableName;
    this.cols = cols;
  }

  public TableMeta(String projName, String tableName, Column[] cols, Column[] partitions) {
    this.projName = projName;
    this.tableName = tableName;
    this.cols = cols;
    if (partitions != null && partitions.length > 0) {
      this.partitions = partitions;
    }
  }

  public String getProjName() {
    return projName;
  }

  public void setProjName(String projName) {
    this.projName = projName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Column[] getCols() {
    return cols;
  }

  public void setCols(Column[] cols) {
    this.cols = cols;
  }

  public Column[] getPartitions() {
    return partitions;
  }

  public void setPartitions(Column[] partitions) {
    this.partitions = partitions;
  }

  public static TableMeta fromTable(Table table) {
    String projectName = table.getProject();
    String tableName = table.getName();
    List<Column> cols = table.getSchema().getColumns();
    List<Column> partitions = table.getSchema().getPartitionColumns();

    if (partitions == null || partitions.size() == 0) {
      return new TableMeta(projectName, tableName, cols.toArray(new Column[cols.size()]));
    } else {
      return new TableMeta(projectName, tableName, cols.toArray(new Column[cols.size()]),
                           partitions.toArray(new Column[partitions.size()]));
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof TableMeta) {
      TableMeta that = (TableMeta) obj;
      boolean ret = StringUtils.equals(projName, that.projName)
                    && StringUtils.equals(tableName, that.tableName);
      if (ret) {
        ret = Arrays.equals(cols, that.cols);
      }
      if (ret) {
        if (partitions != null && that.partitions != null) {
          ret = Arrays.equals(partitions, that.partitions);
        } else if (partitions == null && that.partitions == null) {
        } else {
          return false;
        }
      }
      return ret;
    }
    return false;
  }

}
