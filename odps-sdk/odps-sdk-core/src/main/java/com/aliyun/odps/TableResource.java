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

package com.aliyun.odps;

import com.aliyun.odps.Table.TableModel;

/**
 * TableResource表示ODPS中一个表资源
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class TableResource extends Resource {

  /**
   * 构造此类的对象
   */
  public TableResource() {
    this(null, null);
  }


  /**
   * 构造此类的对象
   *
   * @param tableName
   *     对应的表名
   */
  public TableResource(String tableName) {
    this(tableName, null);
  }

  /**
   * 构造此类的对象
   *
   * @param tableName
   *     对应的表名
   * @param projectName
   *     所属的{@link Project}
   */
  public TableResource(String tableName, String projectName) {
    this(tableName, projectName, null);
  }

  /**
   * 构造此类的对象
   *
   * @param tableName
   *     对应的表名
   * @param projectName
   *     所属的{@link Project}
   * @param partition
   *     分区定义 {@link PartitionSpec}
   */
  public TableResource(String tableName, String projectName,
                       PartitionSpec partition) {
    super();
    if (projectName != null) {
      model.sourceTableName = projectName + "." + tableName;
    } else {
      model.sourceTableName = tableName;
    }

    if (partition == null) {
      model.type = Type.TABLE.toString();
    } else {
      model.sourceTableName = model.sourceTableName + " partition("
                              + partition.toString() + ")";
      model.type = Type.TABLE.toString();
    }
  }

  /**
   * 通过{@link Resource}对象构造TableResource
   *
   * @param resource
   *     {@link Resource}对象
   *     建议使用 {@link #TableResource(String)} 替代
   */
  @Deprecated
  public TableResource(Resource resource) {
    super(resource.model, resource.project, resource.odps);

    if (model == null
        || !Type.TABLE.toString().equalsIgnoreCase(getType().toString())) {
      throw new IllegalArgumentException("Resource type is not TABLE");
    }
  }

  /**
   * 获得资源对应的表名
   *
   * @return 表名
   */
  String getSourceTableName() {
    if (model.sourceTableName == null && client != null) {
      lazyLoad();
    }
    return model.sourceTableName;
  }

  /**
   * 获得该资源对应的表对象
   *
   * @return 对应的表{@link Table}对象
   */
  public Table getSourceTable() {
    String src = getSourceTableName();
    if (src == null) {
      return null;
    }

    String[] res = src.split(" partition\\(");
    src = res[0];

    int off = src.indexOf('.');
    if (off == -1) {
      throw new IllegalArgumentException("Malformed source table name:" + src);
    } else {
      String projName = src.substring(0, off).trim();
      String tblName = src.substring(off + 1, src.length()).trim();
      TableModel tm = new TableModel();
      tm.name = tblName;
      return new Table(tm, projName, odps);
    }
  }

  /**
   * 获得该资源对应的表分区对象
   *
   * @return 对应的表分区定义 {@link PartitionSpec}
   */
  public PartitionSpec getSourceTablePartition() {
    String src = getSourceTableName();
    if (src == null) {
      return null;
    }

    String[] res = src.split(" partition\\(");

    if (res.length < 2) {
      return null;
    }

    String partition = res[1];

    int lastindex = partition.lastIndexOf(')');
    if (lastindex >= 0) {
      partition = partition.substring(0, lastindex);
    }

    return new PartitionSpec(partition.trim());
  }

}
