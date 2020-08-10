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

import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.Task;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;

/**
 * 表示执行一个Merge查询的任务。
 *
 * @author fengyin.zym
 */
@Root(name = "Merge", strict = false)
public class MergeTask extends Task {

  @ElementList(entry = "TableName", inline = true, required = false)
  private List<String> tables;

  // Required by SimpleXML
  MergeTask() {
  }

  /**
   * 使用给定任务名构造一个{@link MergeTask}实例。
   *
   * @param name
   *     任务名。
   */
  public MergeTask(String name) {
    this(name, null);
  }

  /**
   * 使用给定任务名和查询语句构造一个{@link MergeTask}实例。
   *
   * @param name
   *     任务名。
   * @param table
   *     表、分区信息。
   */
  public MergeTask(String name, String table) {
    super();
    setName(name);
    tables = new LinkedList<>();
    tables.add(table);
  }

  /**
   * Get table (or partition) to merge.
   *
   * @return Table (or partition) to merge.
   */
  @Deprecated
  public String getTable() {
    if (tables == null || tables.isEmpty()) {
      return null;
    }

    return tables.get(0);
  }

  /**
   * Set table (or partition) to merge.
   *
   * @param table Table (or partition) to merge. For non-partitioned table, the param should be
   *               the table name. For partitioned tables, the param should be in the following
   *               format: [tbl_name] partition([pt_col]=[pt_val], ...). [tbl_name], [pt_col] and
   *               [pt_val] should be replaced with real table name, partition column name and
   *               partition column value respectfully.
   */
  @Deprecated
  public void setTable(String table) {
    if (tables == null) {
      tables = new LinkedList<>();
    }

    tables.add(0, table);
  }

  /**
   * Get tables (or partitions) to merge.
   *
   * @return List of tables (or partitions).
   */
  public List<String> getTables() {
    return tables;
  }

  /**
   * Set tables (or partitions) to merge.
   *
   * @param tables List of tables (or partitions). For non-partitioned table, list entry should be
   *               the table name. For partitioned tables, list entry should be in the following
   *               format: [tbl_name] partition([pt_col]=[pt_val], ...). [tbl_name], [pt_col] and
   *               [pt_val] should be replaced with real table name, partition column name and
   *               partition column value respectfully.
   */
  public void setTables(List<String> tables) {
    this.tables = tables;
  }
}
