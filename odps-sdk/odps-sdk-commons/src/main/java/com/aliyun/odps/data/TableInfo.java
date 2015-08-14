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

package com.aliyun.odps.data;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.PartitionSpec;

/**
 * 表信息，用于定义作业的输入和输出以及表类型的资源.
 */
public class TableInfo {

  public static final String DEFAULT_LABEL = "__default__";

  private String projectName;
  private String tblName;
  private LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
  private String[] cols;
  private String label = DEFAULT_LABEL;

  public static class TableInfoBuilder {

    private TableInfo table = new TableInfo();

    public TableInfoBuilder projectName(String projectName) {
      table.setProjectName(projectName);
      return this;
    }

    public TableInfoBuilder tableName(String tblName) {
      table.setTableName(tblName);
      return this;
    }

    public TableInfoBuilder partSpec(LinkedHashMap<String, String> partSpec) {
      table.setPartSpec(partSpec);
      return this;
    }

    public TableInfoBuilder partSpec(PartitionSpec partSpec) {
      if (partSpec != null && !partSpec.isEmpty()) {
        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
        for (String key : partSpec.keys()) {
          map.put(key, partSpec.get(key));
        }
        table.setPartSpec(map);
      }
      return this;
    }

    public TableInfoBuilder partSpec(String partPath) {
      table.setPartSpec(partPath);
      return this;
    }

    public TableInfoBuilder cols(String[] cols) {
      table.setCols(cols);
      return this;
    }

    public TableInfoBuilder label(String label) {
      table.setLable(label);
      return this;
    }

    public TableInfo build() {
      return new TableInfo(table);
    }

  }

  /**
   * 给定表名和分区构造一个表信息对象
   *
   * @param projectName
   * @param tblName
   * @param partSpec
   * @param cols
   * @param lable
   */
  public TableInfo(String projectName, String tblName, LinkedHashMap<String, String> partSpec,
                   String[] cols, String lable) {
    this.projectName = projectName;
    this.tblName = tblName;
    this.partSpec = partSpec;
    this.cols = cols;
    this.label = lable;
    
    validate();
  }

  public TableInfo() {

  }

  public TableInfo(TableInfo table) {
    this(table.projectName, table.tblName, table.partSpec, table.cols, table.label);
  }

  public static TableInfoBuilder builder() {
    return new TableInfoBuilder();
  }
  
  public void validate() {
    if (tblName == null || tblName.trim().isEmpty()) {
      throw new RuntimeException("ODPS-0730001: table should not be null or empty: " + tblName);
    }
  }

  /**
   * 获取表的Project.
   *
   * @return 表的Project
   */
  public String getProjectName() {
    return projectName;
  }

  /**
   * 获取分区描述，分区描述是一个PartitionSpec对象.
   *
   * @return 分区
   */
  public PartitionSpec getPartitionSpec() {
    PartitionSpec partitionSpec = new PartitionSpec();
    for (String key : partSpec.keySet()) {
      partitionSpec.set(key, partSpec.get(key));
    }
    return partitionSpec;
  }

  /**
   * 获取分区描述，分区描述是一个有序Map, 其中key是分区名，value是对应分区的取值.
   *
   * @return 分区
   */
  public LinkedHashMap<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * 获取分区路径. 分区路径是一个由/分隔,=对应的字符串. 例如ds=20131230/hr=10
   *
   * @return 分区路径
   */
  public String getPartPath() {
    StringBuilder sb = new StringBuilder();
    if (partSpec != null) {
      for (Entry<String, String> e : partSpec.entrySet()) {
        sb.append(e.getKey()).append(DELIMITER).append(e.getValue()).append(SEPERATOR);
      }
    }
    return sb.toString();
  }

  /**
   * 设置分区描述
   *
   * @param partSpec
   */
  public void setPartSpec(LinkedHashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  /**
   * 设置分区描述
   *
   * @param partSpec
   */
  public void setPartSpec(String partPath) {
    this.partSpec = getPartSpecFromPath(partPath);
  }

  /**
   * 获取查询列
   *
   * @return
   */
  public String[] getCols() {
    return cols;
  }

  /**
   * 设置查询列
   *
   * @param cols
   */
  public void setCols(String[] cols) {
    this.cols = cols;
  }

  /**
   * 获取表名
   *
   * @return
   */
  public String getTableName() {
    return tblName;
  }

  /**
   * 设置表名
   *
   * @param name
   */
  public void setTableName(String name) {
    if (name == null || name.trim().isEmpty()) {
      throw new RuntimeException(
          "ODPS-0730001: table should not be null or empty: " + name);
    }
    String[] ss = StringUtils.splitPreserveAllTokens(name.trim(), '.');
    if (ss.length == 2) {
      if (ss[0].trim().isEmpty() || ss[1].trim().isEmpty()) {
        throw new RuntimeException("ODPS-0730001: error table format: " + name);
      }
      this.projectName = ss[0];
      this.tblName = ss[1];
    } else if (ss.length == 1) {
      if (ss[0].trim().isEmpty()) {
        throw new RuntimeException("ODPS-0730001: error table format: " + name);
      }
      this.tblName = ss[0];
    } else {
      throw new RuntimeException("ODPS-0730001: error table format: " + name);
    }
  }

  /**
   * 获取标签
   *
   * @return
   */
  public String getLabel() {
    return label;
  }

  /**
   * 设置标签
   *
   * @param lable
   */
  public void setLable(String lable) {
    this.label = lable;
  }

  /**
   * 设置Project名
   *
   * @param projectName
   */
  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  @Override
  public boolean equals(Object that) {
    if (!(that instanceof TableInfo)) {
      return false;
    }
    TableInfo t = (TableInfo) that;
    if (stringEquals(projectName, t.projectName) && stringEquals(tblName, t.tblName)
        && Arrays.equals(this.cols, t.cols) && stringEquals(label, t.label)
        && this.partSpec.equals(t.partSpec)) {
      return true;
    }
    return false;
  }

  private transient static final String SEPERATOR = "/";
  private transient static final String DELIMITER = "=";

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (projectName != null) {
      sb.append(projectName).append('.');
    }
    sb.append(tblName);
    if (partSpec != null && !partSpec.isEmpty()) {
      sb.append(partSpec.toString());
    }
    return sb.toString();
  }

  private static boolean stringEquals(final String cs1, final String cs2) {
    if (cs1 == cs2) {
      return true;
    }
    if (cs1 == null || cs2 == null) {
      return false;
    }
    return cs1.equals(cs2);
  }

  private LinkedHashMap<String, String> getPartSpecFromPath(String path) {
    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
    if (path != null && !path.trim().isEmpty()) {
      String[] parts = path.split(SEPERATOR);
      for (String part : parts) {
        String[] ss = part.split(DELIMITER);
        if (ss.length != 2) {
          throw new RuntimeException("ODPS-0730001: error part spec format: " + path);
        }
        map.put(ss[0], ss[1]);
      }
    }
    return map;
  }
}
