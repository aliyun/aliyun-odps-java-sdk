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

package com.aliyun.odps.io;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.utils.CommonUtils;


/**
 * 表信息，用于定义 ODPS M/R 作业的输入和输出以及表类型的资源.该类仅为了兼容老版本SDK
 */
@Deprecated
public class TableInfo {

  private String tbl;

  private String partSpec;

  /**
   * 给定表名构造一个表信息对象.
   *
   * 给定的tbl可以是带project的全称，如proj.tblname，也可以只带表名，如tblname，
   * 如果不带project则使用Console默认的project
   *
   * @param tbl
   *     表名，可以是带project的全称，如proj.tblname，也可以只带表名，如tblname
   */
  public TableInfo(String tbl) {
    this(tbl, "");
  }

  /**
   * 给定表名和分区构造一个表信息对象.
   *
   * @param tbl
   *     表名，可以是带project的全称，如proj.tblname，也可以只带表名，如tblname
   * @param partSpec
   *     分区，如pt=1或pt=1/ds=2，多级分区之间用“/”分隔
   */
  public TableInfo(String tbl, String partSpec) {
    setTbl(tbl);
    setPartSpec(partSpec);
  }

  /**
   * 给定表名和分区构造一个表信息对象.
   *
   * @param tbl
   *     表名，可以是带project的全称，如proj.tblname，也可以只带表名，如tblname
   * @param partSpec
   *     分区，是一个LinkedHashMap<String, String>，Map的key和value分别是分区的名称和值
   */
  public TableInfo(String tbl, LinkedHashMap<String, String> partSpec) {
    setTbl(tbl);
    setPartSpecMap(partSpec);
  }

  /**
   * 获取表的Project.
   *
   * @return 表的Project
   */
  public String getProjectName() {
    String[] ss = StringUtils.splitPreserveAllTokens(tbl, '.');
    return (ss.length == 2) ? ss[0] : "";
  }

  /**
   * 获取表名，不带Project.
   *
   * @return 表名
   */
  public String getTableName() {
    String[] ss = StringUtils.splitPreserveAllTokens(tbl, '.');
    return (ss.length == 2) ? ss[1] : ss[0];
  }

  /**
   * 设置表名，可以是带project的全称，如proj.tblname，也可以只带表名，如tblname
   *
   * @param tbl
   *     表名
   */
  public void setTbl(String tbl) {
    if (tbl == null || tbl.trim().isEmpty()) {
      throw new RuntimeException(
          "ODPS-0730001: table should not be null or empty: " + tbl);
    }
    String[] ss = StringUtils.splitPreserveAllTokens(tbl.trim(), '.');
    if (ss.length == 2) {
      if (ss[0].trim().isEmpty() || ss[1].trim().isEmpty()) {
        throw new RuntimeException("ODPS-0730001: error table format: " + tbl);
      }
    } else if (ss.length == 1) {
      if (ss[0].trim().isEmpty()) {
        throw new RuntimeException("ODPS-0730001: error table format: " + tbl);
      }
    } else {
      throw new RuntimeException("ODPS-0730001: error table format: " + tbl);
    }
    this.tbl = tbl.trim();
  }

  /**
   * 获取分区，返回结果如pt=1或pt=1/ds=2，多级分区之间用“/”分隔.
   *
   * @return 分区
   */
  public String getPartSpec() {
    return partSpec;
  }

  /**
   * 设置分区，给定的partSpec如pt=1或pt=1/ds=2，多级分区之间用“/”分隔.
   *
   * @param partSpec
   *     分区，如pt=1或pt=1/ds=2，多级分区之间用“/”分隔
   */
  public void setPartSpec(String partSpec) {
    setPartSpecMap(CommonUtils.convertPartSpecToMap(partSpec));
  }

  /**
   * 设置分区，给定一个LinkedHashMap<String, String>.
   *
   * <p>
   * partSpecMap的内容如：：{"pt":"1", "ds":"1"}
   *
   * @param partSpecMap
   *     LinkedHashMap<String, String>表示的分区
   */
  public void setPartSpecMap(LinkedHashMap<String, String> partSpecMap) {
    partSpec = "";
    if (partSpecMap != null) {
      for (Map.Entry<String, String> entry : partSpecMap.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          throw new RuntimeException(
              "ODPS-0730001: partition name or value can't be null: "
              + entry.getKey() + "=" + entry.getValue());
        }
        String name = StringUtils.strip(entry.getKey().trim(), "'\"");
        String value = StringUtils.strip(entry.getValue().trim(), "'\"");
        String part = name + "=" + value;
        partSpec += (partSpec.isEmpty()) ? part : "/" + part;
      }
    }
  }

  /**
   * 获取LinkedHashMap<String, String>表示的分区.
   *
   * <p>
   * 返回的 map 对象内容如：：{"pt":"1", "ds":"1"}
   *
   * @return LinkedHashMap<String, String>表示的分区
   */
  public LinkedHashMap<String, String> getPartSpecMap() {
    return CommonUtils.convertPartSpecToMap(partSpec);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof TableInfo) {
      TableInfo that = (TableInfo) obj;
      return this.tbl.equals(that.tbl) && this.partSpec.equals(that.partSpec);
    }
    return false;
  }

  @Override
  public String toString() {
    return tbl + (!partSpec.isEmpty() ? " [" + partSpec + "]" : "");
  }

}
