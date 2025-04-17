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

import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.options.SQLTaskOption;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.google.gson.GsonBuilder;

/**
 * 表示执行一个Merge查询的任务。
 *
 * @author fengyin.zym
 */
@Root(name = "Merge", strict = false)
public class MergeTask extends Task {

  private static final String DEFAULT_NAME = "AnonymousMergeTask";

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


  public static boolean isCompactCommand(String sql) {
    return true;
  }

  public enum CompactType {
    MAJOR("major_compact"),
    MINOR("minor_compact");
    private String name;

    CompactType(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public static CompactType fromName(String name) {
      if ("major".equalsIgnoreCase(name)) {
        return MAJOR;
      } else if ("minor".equalsIgnoreCase(name)) {
        return MINOR;
      }
      throw new IllegalArgumentException("No enum constant for name: " + name);
    }
  }

  private static final Pattern MERGE_SMALLFILES_PATTERN = Pattern.compile(
      "\\s*ALTER\\s+TABLE\\s+(.*)\\s+(MERGE\\s+SMALLFILES\\s*)$",
      Pattern.CASE_INSENSITIVE
  );

  private static final Pattern COMPACT_PATTERN = Pattern.compile(
      "(?s)\\s*ALTER\\s+TABLE\\s+(.*)\\s+COMPACT\\s+(.*)",
      Pattern.CASE_INSENSITIVE
  );

  public static Instance parseMergeTask(Odps odps, String sql, SQLTaskOption option)
      throws OdpsException {
    sql = sql.trim();
    if (sql.endsWith(";")) {
      sql = sql.substring(0, sql.length() - 1);
    }
    Matcher m = MERGE_SMALLFILES_PATTERN.matcher(sql);
    if (m.find()) {
      String tablePart = m.group(1);
      Map<String, String> settings = new HashMap<>();
      settings.put("odps.merge.cross.paths", "true");
      if (option.getHints() != null) {
        settings.putAll(option.getHints());
      }
      return run(odps, tablePart, settings);
    }

    m = COMPACT_PATTERN.matcher(sql);
    if (m.find()) {
      String tablePart = m.group(1).trim();
      String[] params = m.group(2).trim().split("\\s+");
      CompactType compactType = CompactType.fromName(params[0]);

      Map<String, String> settings = new HashMap<>();
      settings.put("odps.merge.txn.table.compact", compactType.getName());
      settings.put("odps.merge.restructure.action", "hardlink");
      if (option.getHints() != null) {
        settings.putAll(option.getHints());
      }
      return run(odps, tablePart, settings);
    }
    return null;
  }


  public static Instance run(Odps odps, String tableId, Map<String, String> hints)
      throws OdpsException {
    String taskName = DEFAULT_NAME + "_" + Calendar.getInstance().getTimeInMillis();
    Task task = new MergeTask(taskName, tableId);

    if (hints != null) {
      String json = new GsonBuilder().disableHtmlEscaping().create().toJson(hints);
      task.setProperty("settings", json);
    }
    return odps.instances().create(task);
  }
}
