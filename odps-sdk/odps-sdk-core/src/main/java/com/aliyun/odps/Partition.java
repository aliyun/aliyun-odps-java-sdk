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

import com.aliyun.odps.commons.transport.Params;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Attribute;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.utils.TagUtils;
import com.aliyun.odps.utils.TagUtils.OBJECT_TYPE;
import com.aliyun.odps.utils.TagUtils.OPERATION_TYPE;
import com.aliyun.odps.utils.TagUtils.ObjectRef;
import com.aliyun.odps.utils.TagUtils.ObjectTagInfo;
import com.aliyun.odps.utils.TagUtils.SetObjectTagInput;
import com.aliyun.odps.utils.TagUtils.SimpleTag;
import com.aliyun.odps.utils.TagUtils.TagRef;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Partition类的对象表示ODPS分区表中一个特定的分区
 */
public class Partition extends LazyLoad {

  @Root(name = "Partition", strict = false)
  static class PartitionModel {

    @ElementList(entry = "Column", inline = true, required = false)
    List<ColumnModel> columns = new ArrayList<ColumnModel>();

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
    protected String columnName;

    @Attribute(name = "Value", required = false)
    protected String columnValue;
  }

  @Root(name = "Partition", strict = false)
  static class PartitionSpecModel {
    @Element(name = "Name", required = false)
    String partitionSpec;
  }

  private PartitionModel model;
  private ObjectTagInfo partitionTagInfo;
  private PartitionSpec spec;
  private String projectName;
  private String schemaName;
  private String table;
  private Odps odps;
  private RestClient client;
  protected long size;
  protected long recordNum = -1L;

  private boolean isExtendInfoLoaded;
  private boolean isArchived;
  private boolean isExstore;
  private long lifeCycle;
  private long physicalSize;
  private long fileNum;
  // reserved json string in extended info
  private String reserved;
  private Table.ClusterInfo clusterInfo;

  Partition(
      PartitionModel model,
      String projectName,
      String schemaName,
      String tableName,
      Odps odps) {
    this.model = model;
    this.odps = odps;
    this.client = odps.getRestClient();
    this.projectName = projectName;
    this.schemaName = schemaName;
    this.table = tableName;

    this.isArchived = false;
    this.isExstore = false;
    this.isExtendInfoLoaded = false;
    this.lifeCycle = -1;
    this.physicalSize = 0;
    this.fileNum = 0;
  }

  Partition(
      PartitionSpec spec,
      String projectName,
      String schemaName,
      String tableName,
      Odps odps) {
    this.spec = spec;
    this.odps = odps;
    this.client = odps.getRestClient();
    this.projectName = projectName;
    this.schemaName = schemaName;
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

  /**
   * Get {@link Tag}(s) attached to this partition.
   * @return list of {@link Tag}
   */
  public List<Tag> getTags() {
    reloadTagInfo();
    return TagUtils.getTags(partitionTagInfo, odps);
  }

  /**
   * Get {@link Tag}(s) attached to a column of this partition.
   * @return list of {@link Tag}
   */
  public List<Tag> getTags(String columnName) {
    reloadTagInfo();

    // Make sure specified column exists
    Objects.requireNonNull(columnName);
    TagUtils.validateTaggingColumn(
        odps.tables().get(projectName, table).getSchema(),
        Collections.singletonList(columnName));

    return TagUtils.getTags(partitionTagInfo, columnName, odps);
  }

  /**
   * Get simple tags attached to this partition.
   * @return a map from category to key value pairs
   */
  public Map<String, Map<String, String>> getSimpleTags() {
    reloadTagInfo();
    return TagUtils.getSimpleTags(partitionTagInfo);
  }

  /**
   * Get simple tags attached to a column of this partition.
   *
   * @param columnName column name.
   * @return a map from category to key value pairs.
   */
  public Map<String, Map<String, String>> getSimpleTags(String columnName) {
    reloadTagInfo();

    // Make sure specified column exists
    Objects.requireNonNull(columnName);
    TagUtils.validateTaggingColumn(
        odps.tables().get(projectName, table).getSchema(),
        Collections.singletonList(columnName));

    return TagUtils.getSimpleTags(partitionTagInfo, columnName);
  }

  /**
   * Attach a {@link Tag} to a partition. The partition and tag should be in a same project.
   *
   * @param tag tag to attach
   */
  public void addTag(Tag tag) throws OdpsException {
    addTag(tag, null);
  }

  /**
   * Attach a {@link Tag} to a partition. The partition and tag should be in a same project.
   *
   * @param tag tag to attach
   * @param columnNames column names, could be null.
   */
  public void addTag(Tag tag, List<String> columnNames) throws OdpsException {
    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.TABLE,
        projectName,
        table,
        columnNames);
    TagRef tagRef = new TagRef(tag.getClassification(), tag.getName());
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.SET, objectRef, tagRef, null);

    TagUtils.updateTagInternal(setObjectTagInput, getPartitionSpec(), client);
  }

  /**
   * Attach a simple tag to a partition. A simple tag is a triad consisted of category, tag
   * key, and tag value.
   *
   * @param category simple tag category, could be nul.
   * @param key simple tag key, cannot be null.
   * @param value simple tag value, cannot be null.
   */
  public void addSimpleTag(String category, String key, String value) throws OdpsException {
    addSimpleTag(category, key, value, null);
  }

  /**
   * Attach a simple tag to a partition or some of its columns. A simple tag is a triad consisted of
   * category, tag key, and tag value.
   * @param category simple tag category, could be nul.
   * @param key simple tag key, cannot be null.
   * @param value simple tag value, cannot be null.
   * @param columnNames column names, should not include any partition column, could be null.
   */
  public void addSimpleTag(
      String category,
      String key,
      String value,
      List<String> columnNames) throws OdpsException {
    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.TABLE,
        projectName,
        table,
        columnNames);
    SimpleTag simpleTag = new SimpleTag(category, Collections.singletonMap(key, value));
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.SET, objectRef, null, simpleTag);

    TagUtils.updateTagInternal(setObjectTagInput, getPartitionSpec(), client);
  }

  /**
   * Remove a {@link Tag}.
   *
   * @param tag tag to remove.
   */
  public void removeTag(Tag tag) throws OdpsException {
    removeTag(tag, null);
  }

  /**
   * Remove a {@link Tag} from columns.
   *
   * @param tag tag to remove.
   * @param columnNames column names, should not include any partition column, could be null.
   */
  public void removeTag(Tag tag, List<String> columnNames) throws OdpsException {

    Objects.requireNonNull(tag);

    // Make sure column names are valid
    TagUtils.validateTaggingColumn(
        odps.tables().get(projectName, table).getSchema(),
        columnNames);

    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.TABLE,
        projectName,
        table,
        columnNames);
    TagRef tagRef = new TagRef(tag.getClassification(), tag.getName());
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.UNSET, objectRef, tagRef, null);

    TagUtils.updateTagInternal(setObjectTagInput, getPartitionSpec(), client);
  }

  /**
   * Remove a simple tag. A simple tag is a triad consisted of category, tag key, and tag value.
   *
   * @param category category.
   * @param key key.
   * @param value value.
   * @throws OdpsException
   */
  public void removeSimpleTag(String category, String key, String value) throws OdpsException {
    removeSimpleTag(category, key, value, null);
  }

  /**
   * Remove a simple tag from columns. A simple tag is a triad consisted of category, tag key, and
   * tag value.
   *
   * @param category category.
   * @param key key.
   * @param value value.
   * @param columnNames column names, should not include any partition column, could be null.
   * @throws OdpsException
   */
  public void removeSimpleTag(
      String category,
      String key,
      String value,
      List<String> columnNames) throws OdpsException {

    Objects.requireNonNull(category);
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);

    // Make sure column names are valid
    TagUtils.validateTaggingColumn(
        odps.tables().get(projectName, table).getSchema(),
        columnNames);

    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.TABLE,
        projectName,
        table,
        columnNames);
    SimpleTag simpleTag = new SimpleTag(category, Collections.singletonMap(key, value));
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.UNSET, objectRef, null, simpleTag);
    TagUtils.updateTagInternal(setObjectTagInput, getPartitionSpec(), client);
  }

  @Root(name = "Partition", strict = false)
  private static class PartitionMeta {

    @Element(name = "Schema", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String schema;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildTableResource(projectName, table);
    Map<String, String> params = new LinkedHashMap<>();
    params.put("partition", getPartitionSpec().toString());
    if (!StringUtils.isNullOrEmpty(schemaName)) {
      params.put(Params.ODPS_SCHEMA_NAME, schemaName);
    }

    PartitionMeta meta = client.request(PartitionMeta.class, resource, "GET", params);

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

  private void reloadTagInfo() {
    String resource = ResourceBuilder.buildTableResource(projectName, table);
    // Convert the OdpsException to a ReloadException the keep the consistency of the getter's
    // method signature.
    try {
      partitionTagInfo = TagUtils.getObjectTagInfo(resource, getPartitionSpec(), client);
    } catch (OdpsException e) {
      throw new ReloadException(e);
    }
  }

  private void lazyLoadExtendInfo() {
    if (!this.isExtendInfoLoaded) {
      Map<String, String> params = new LinkedHashMap<String, String>();
      params.put("extended", null);
      params.put("partition", getPartitionSpec().toString());
      if (!StringUtils.isNullOrEmpty(schemaName)) {
        params.put(Params.ODPS_SCHEMA_NAME, schemaName);
      }

      String resource = ResourceBuilder.buildTableResource(projectName, table);
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
