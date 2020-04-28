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

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Attribute;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Partition类的对象表示ODPS分区表中一个特定的分区
 */
public class Partition extends LazyLoad {

  @Root(name = "Partition", strict = false)
  static class PartitionModel {

    @ElementList(entry = "Column", inline = true, required = false)
    private List<ColumnModel> columns = new ArrayList<ColumnModel>();

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.EpochConverter.class)
    Date createdTime;

    @Element(name = "LastDDLTime", required = false)
    @Convert(SimpleXmlUtils.EpochConverter.class)
    Date lastMetaModifiedTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.EpochConverter.class)
    Date lastDataModifiedTime;
  }

  @Root(name = "Column", strict = false)
  static class ColumnModel {

    @Attribute(name = "Name", required = false)
    private String columnName;

    @Attribute(name = "Value", required = false)
    private String columnValue;
  }

  @Root(name = "Partition", strict = false)
  static class PartitionSpecModel{
    @Element(name = "Name", required = false)
    String partitionSpec;
  }

  private PartitionModel model;
  private PartitionSpec spec;
  private String project;
  private String table;
  private RestClient client;
  private long size;
  private long recordNum = -1L;

  private boolean isExtendInfoLoaded;
  private boolean isArchived;
  private boolean isExstore;
  private long lifeCycle;
  private long physicalSize;
  private long fileNum;
  // reserved json string in extended info
  private String reserved;
  private Table.ClusterInfo clusterInfo;

  Partition(PartitionModel model, String projectName, String tableName,
            RestClient client) {
    this.model = model;
    this.client = client;
    this.project = projectName;
    this.table = tableName;

    this.isArchived = false;
    this.isExstore = false;
    this.isExtendInfoLoaded = false;
    this.lifeCycle = -1;
    this.physicalSize = 0;
    this.fileNum = 0;
  }

  Partition(PartitionSpec spec, String projectName, String tableName,
            RestClient client) {
    this.spec = spec;
    this.client = client;
    this.project = projectName;
    this.table = tableName;

    this.isArchived = false;
    this.isExstore = false;
    this.isExtendInfoLoaded = false;
    this.lifeCycle = -1;
    this.physicalSize = 0;
    this.fileNum = 0;
    this.model = new PartitionModel();
  }

  /**
   * 获得{@link PartitionSpec}对象
   *
   * @return {@link PartitionSpec}对象
   */
  public PartitionSpec getPartitionSpec() {
    if (spec != null) {
      return spec;
    }
    spec = new PartitionSpec();
    for (ColumnModel c : model.columns) {
      spec.set(c.columnName, c.columnValue);
    }
    return spec;
  }

  /**
   * 获取分区的创建时间
   *
   * @return 分区创建时间
   */
  public Date getCreatedTime() {
    if (model == null || model.createdTime == null) {
      lazyLoad();
    }

    return model.createdTime;
  }

  /**
   * 获取分区的Meta修改时间
   *
   * @return 分区Meta修改时间
   */
  public Date getLastMetaModifiedTime() {
    if (model == null || model.lastMetaModifiedTime == null) {
      lazyLoad();
    }
    return model.lastMetaModifiedTime;
  }

  /**
   * 获取分区数据的最后修改时间
   *
   * @return 分区最后修改时间
   */
  public Date getLastDataModifiedTime() {
    if (model == null || model.lastDataModifiedTime == null) {
      lazyLoad();
    }
    return model.lastDataModifiedTime;
  }


  /**
   * 获取分区数据的Record数，若无准确数据，则返回-1
   *
   * @return Record数
   */
  public long getRecordNum() {
    lazyLoad();
    return recordNum;
  }

  /**
   * 查看表是否进行了归档
   *
   * @return true表示表已进行过归档，false表示未进行过归档操作
   */
  public boolean isArchived() {
    lazyLoadExtendInfo();
    return isArchived;
  }

  public boolean isExstore() {
    lazyLoadExtendInfo();
    return isExstore;
  }

  /**
   * 获取表的生命周期
   *
   * @return 表的生命周期
   */
  public long getLifeCycle() {
    lazyLoadExtendInfo();
    return lifeCycle;
  }

  /**
   * 获得分区数据的物理大小，单位Byte
   *
   * @return 物理大小
   */
  public long getPhysicalSize() {
    lazyLoadExtendInfo();
    return physicalSize;
  }

  /**
   * 获取分区数据的文件数
   *
   * @return 文件数
   */
  public long getFileNum() {
    lazyLoadExtendInfo();
    return fileNum;
  }

  /**
   * 获取分区内部存储的大小，单位Byte
   *
   * @return 内部存储大小
   */
  public long getSize() {
    lazyLoad();
    return size;
  }

  /**
   * 返回扩展信息的保留字段
   * json 字符串
   *
   * @return 保留字段
   */
  public String getReserved() {
    if (reserved == null) {
      lazyLoadExtendInfo();
    }
    return reserved;
  }

  /**
   * 返回 cluster range partition 的 cluster 信息
   *
   * @return cluster info
   */
  public Table.ClusterInfo getClusterInfo() {
    if (clusterInfo == null) {
      lazyLoadExtendInfo();
    }

    return clusterInfo;
  }

  @Root(name = "Partition", strict = false)
  private static class PartitionMeta {

    @Element(name = "Schema", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String schema;
  }

  @Override
  public void reload() throws OdpsException {
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("partition", getPartitionSpec().toString());

    String resource = ResourceBuilder.buildTableResource(project, table);

    PartitionMeta meta = client.request(PartitionMeta.class,
            resource.toString(), "GET", params);

    try {
      JsonObject tree = new JsonParser().parse(meta.schema).getAsJsonObject();
      if (tree.has("createTime")) {
        model.createdTime = new Date(tree.get("createTime").getAsLong() * 1000);
      }

      if (tree.has("lastDDLTime")) {
        model.lastMetaModifiedTime = new Date(tree.get("lastDDLTime").getAsLong() * 1000);
      }

      if (tree.has("lastModifiedTime")) {
        model.lastDataModifiedTime = new Date(tree.get("lastModifiedTime").getAsLong() * 1000);
      }

      if (tree.has("partitionSize")) {
        size = tree.get("partitionSize").getAsLong();
      }

      if (tree.has("partitionRecordNum")) {
        recordNum = tree.get("partitionRecordNum").getAsLong();
      }

      setLoaded(true);
    } catch (Exception e) {
      throw new OdpsException("Invalid partition meta", e);
    }
  }

  private void lazyLoadExtendInfo() {
    if (!this.isExtendInfoLoaded) {
      Map<String, String> params = new LinkedHashMap<String, String>();
      params.put("extended", null);
      params.put("partition", getPartitionSpec().toString());

      String resource = ResourceBuilder.buildTableResource(project, table);
      try {
        PartitionMeta meta =
                client.request(PartitionMeta.class, resource.toString(), "GET", params);

        JsonObject tree = new JsonParser().parse(meta.schema).getAsJsonObject();

        if (tree.has("IsArchived")) {
          isArchived = tree.get("IsArchived").getAsBoolean();
        }

        if (tree.has("IsExstore")) {
          isExstore = tree.get("IsExstore").getAsBoolean();
        }

        if (tree.has("LifeCycle")) {
          lifeCycle = tree.get("LifeCycle").getAsLong();
        }

        if (tree.has("PhysicalSize")) {
          physicalSize = tree.get("PhysicalSize").getAsLong();
        }

        if (tree.has("FileNum")) {
          fileNum = tree.get("FileNum").getAsLong();
        }

        if (tree.has("Reserved")) {
          reserved = tree.get("Reserved").getAsString();
          loadReservedJson(reserved);
        }
      } catch (Exception e) {
        throw new ReloadException(e.getMessage(), e);
      }
      isExtendInfoLoaded = true;
    }
  }

  private void loadReservedJson(String reserved) {
    JsonObject reservedJson = new JsonParser().parse(reserved).getAsJsonObject();

    // load cluster info
    clusterInfo = Table.parseClusterInfo(reservedJson);
  }
}
