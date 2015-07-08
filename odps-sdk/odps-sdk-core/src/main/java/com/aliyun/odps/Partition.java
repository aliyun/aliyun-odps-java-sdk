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

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.JsonNode;

import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * Partition类的对象表示ODPS分区表中一个特定的分区
 */
public class Partition extends LazyLoad {

  @XmlRootElement(name = "Partition")
  static class PartitionModel {

    @XmlElement(name = "Column")
    private List<ColumnModel> columns = new ArrayList<ColumnModel>();
  }

  @XmlRootElement(name = "Column")
  static class ColumnModel {

    @XmlAttribute(name = "Name")
    private String columnName;
    @XmlAttribute(name = "Value")
    private String columnValue;
  }

  private PartitionModel model;
  private PartitionSpec spec;
  private String project;
  private String table;
  private RestClient client;

  private Date createdTime;
  private Date lastMetaModifiedTime;
  private Date lastDataModifiedTime;
  private long size;

  private boolean isExtendInfoLoaded;
  private boolean isArchived;
  private boolean isExstore;
  private long lifeCycle;
  private long physicalSize;
  private long fileNum;

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
    lazyLoad();
    return createdTime;
  }

  /**
   * 获取分区的Meta修改时间
   *
   * @return 分区Meta修改时间
   */
  public Date getLastMetaModifiedTime() {
    lazyLoad();
    return lastMetaModifiedTime;
  }

  /**
   * 获取分区数据的最后修改时间
   *
   * @return 分区最后修改时间
   */
  public Date getLastDataModifiedTime() {
    lazyLoad();
    return lastDataModifiedTime;
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

  @XmlRootElement(name = "Partition")
  private static class PartitionMeta {

    @XmlElement(name = "Schema")
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
      JsonNode tree = JacksonParser.parse(meta.schema);
      JsonNode node = tree.get("createTime");
      if (node != null && !node.isNull()) {
        createdTime = new Date(node.asLong() * 1000);
      }

      node = tree.get("lastDDLTime");
      if (node != null && !node.isNull()) {
        lastMetaModifiedTime = new Date(node.asLong() * 1000);
      }

      node = tree.get("lastModifiedTime");
      if (node != null && !node.isNull()) {
        lastDataModifiedTime = new Date(node.asLong() * 1000);
      }

      node = tree.get("partitionSize");
      if (node != null && !node.isNull()) {
        size = node.asLong();
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
        PartitionMeta
            meta =
            client.request(PartitionMeta.class, resource.toString(), "GET", params);

        JsonNode tree = JacksonParser.parse(meta.schema);

        JsonNode node = tree.get("IsArchived");
        if (node != null && !node.isNull()) {
          isArchived = node.asBoolean();
        }

        node = tree.get("IsExstore");
        if (node != null && !node.isNull()) {
          isExstore = node.asBoolean();
        }

        node = tree.get("LifeCycle");
        if (node != null && !node.isNull()) {
          lifeCycle = node.asLong();
        }

        node = tree.get("PhysicalSize");
        if (node != null && !node.isNull()) {
          physicalSize = node.asLong();
        }

        node = tree.get("FileNum");
        if (node != null && !node.isNull()) {
          fileNum = node.asLong();
        }
      } catch (Exception e) {
        throw new ReloadException(e.getMessage(), e);
      }
      isExtendInfoLoaded = true;
    }
  }
}
