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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.aliyun.odps.Partition.PartitionModel;
import com.aliyun.odps.Partition.PartitionSpecModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.data.DefaultRecordReader;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Attribute;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.Text;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.simpleframework.xml.convert.Converter;
import com.aliyun.odps.simpleframework.xml.stream.InputNode;
import com.aliyun.odps.simpleframework.xml.stream.OutputNode;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.ColumnUtils;
import com.aliyun.odps.utils.NameSpaceSchemaUtils;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.odps.utils.TagUtils;
import com.aliyun.odps.utils.TagUtils.OBJECT_TYPE;
import com.aliyun.odps.utils.TagUtils.OPERATION_TYPE;
import com.aliyun.odps.utils.TagUtils.ObjectRef;
import com.aliyun.odps.utils.TagUtils.ObjectTagInfo;
import com.aliyun.odps.utils.TagUtils.SetObjectTagInput;
import com.aliyun.odps.utils.TagUtils.SimpleTag;
import com.aliyun.odps.utils.TagUtils.TagRef;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

/**
 * Table表示ODPS中的表
 */
public class Table extends LazyLoad {

  public enum TableType {
    /**
     * Regular table managed by ODPS
     */
    MANAGED_TABLE,
    /**
     * Virtual view
     */
    VIRTUAL_VIEW,
    /**
     * External table
     */
    EXTERNAL_TABLE,
    /**
     * Materialized view
     */
    MATERIALIZED_VIEW
  }

  /**
   * Convert {@link TableType} to/from {@link String}
   */
  public static class TableTypeConverter implements Converter<TableType> {

    @Override
    public TableType read(InputNode node) throws Exception {
      String value = node.getValue();
      if (value == null) {
        return null;
      } else {
        try {
          return TableType.valueOf(value);
        } catch (IllegalArgumentException e) {
          // If there is a new table type which cannot be recognized
          return null;
        }
      }
    }

    @Override
    public void write(OutputNode node, TableType value) throws Exception {
      // The server side does not accept this field
      node.remove();
    }
  }

  @Root(name = "Table", strict = false)
  static class TableModel {

    @Root(name = "Schema", strict = false)
    static class Schema {

      @Text(required = false)
      String content;
    }

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "TableId", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String ID;

    @Attribute(name = "format", required = false)
    private String format;

    @Element(name = "Schema", required = false)
    private Schema schema;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "Project", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String projectName;

    @Element(name = "SchemaName", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String schemaName;

    @Element(name = "TableLabel", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String tableLabel;

    @Element(name = "CryptoAlgo", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String cryptoAlgoName;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModifiedTime;

    @Element(name = "Type", required = false)
    @Convert(TableTypeConverter.class)
    TableType type;

    Date lastMetaModifiedTime;
    boolean isVirtualView;
    boolean isMaterializedViewRewriteEnabled;
    boolean isMaterializedViewOutdated;
    boolean isExternalTable;
    long life = -1L;
    long hubLifecycle = -1L;
    String viewText;
    long size;
    long recordNum = -1L;

    boolean isArchived;
    long physicalSize;
    long fileNum;

    boolean isTransactional;
    // reserved json string in extended info
    String reserved;
    Shard shard;

    // for external table extended info
    String storageHandler;
    String location;
    String resources;
    Map<String, String> serDeProperties;

    // for clustered info
    ClusterInfo clusterInfo;
    // for table extended labels
    List<String> tableExtendedLabels;
  }


  public static class ClusterInfo {
    long bucketNum = -1;

    String clusterType;
    List<String> clusterCols;
    List<SortColumn> sortCols;

    public String getClusterType() {
      return clusterType;
    }

    public long getBucketNum() {
      return bucketNum;
    }

    public List<String> getClusterCols() {
      return clusterCols;
    }

    public List<SortColumn> getSortCols() {
      return sortCols;
    }
  }

  public static class SortColumn {
    private String name;
    private  String order;

    SortColumn(String name, String order) {
      this.name = name;
      this.order = order;
    }

    public String getName() {
      return name;
    }

    public String getOrder() {
      return order;
    }

    @Override
    public String toString() {
      return String.format("%s %s", name, order);
    }
  }

  private TableModel model;
  private TableSchema tableSchema;
  private ObjectTagInfo tableTagInfo;
  private RestClient client;
  private boolean isExtendInfoLoaded;
  private boolean isShardInfoLoaded;
  private Odps odps;

  Table(TableModel model, String project, String schemaName, Odps odps) {
    this.model = model;
    this.model.projectName = project;
    this.model.schemaName = schemaName;
    this.odps = odps;
    this.client = odps.getRestClient();
    this.isExtendInfoLoaded = false;
    this.isShardInfoLoaded = false;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
    Map<String, String> params = initParamsWithSchema();

    reload(client.request(TableModel.class, resource, "GET", params));
  }

  public void reload(TableModel model) throws OdpsException {
    this.model = model;
    if (model.schema != null) {
      tableSchema = loadSchemaFromJson(model.schema.content);
    }
    setLoaded(true);
  }

  private void reloadTagInfo() {
    String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
    // Convert the OdpsException to a ReloadException the keep the consistency of the getter's
    // method signature.
    try {
      tableTagInfo = TagUtils.getObjectTagInfo(resource, null, client);
    } catch (OdpsException e) {
      throw new ReloadException(e);
    }
  }

  private void lazyLoadExtendInfo() {
    if (!this.isExtendInfoLoaded) {
      TableModel response;
      try {
        Map<String, String> params = initParamsWithSchema();
        params.put("extended", null);

        String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
        response = client.request(TableModel.class, resource, "GET", params);
      } catch (OdpsException e) {
        throw new ReloadException(e.getMessage(), e);
      }

      loadSchemaFromJson(response.schema.content);
      this.isExtendInfoLoaded = true;
    }
  }

  /**
   * 获取表名
   *
   * @return 表名称
   */
  public String getName() {
    return model.name;
  }

  /**
   * 获取注释
   *
   * @return 表的相关注释信息
   */
  public String getComment() {
    if (model.comment == null) {
      lazyLoad();
    }
    return model.comment;
  }

  /**
   * 获取表所属用户
   *
   * @return 所属用户
   */
  public String getOwner() {
    if (model.owner == null) {
      lazyLoad();
    }
    return model.owner;
  }

  /**
   * table接口目前不能获取table类型，现在只有Tables接口返回的table list中的table有类型
   *
   * @return 表类型
   */
  public TableType getType() {
    return model.type;
  }

  /**
   * 获取创建时间
   *
   * @return 创建时间
   */
  public Date getCreatedTime() {
    if (model.createdTime == null) {
      lazyLoad();
    }
    return model.createdTime;
  }

  public String getTableLabel() {
    if (model.tableLabel == null) {
      lazyLoad();
    }
    return model.tableLabel;
  }

  public List<String> getTableExtendedLabels() {
    if (model.tableExtendedLabels == null) {
      lazyLoad();
    }

    return model.tableExtendedLabels;
  }

  /**
   * Get {@link Tag}(s) attached to this table.
   * @return list of {@link Tag}
   */
  public List<Tag> getTags() {
    reloadTagInfo();
    return TagUtils.getTags(tableTagInfo, odps);
  }

  /**
   * Get {@link Tag}(s) attached to a column of this table.
   * @return list of {@link Tag}
   */
  public List<Tag> getTags(String columnName) {
    reloadTagInfo();

    // Make sure specified column exists
    Objects.requireNonNull(columnName);
    TagUtils.validateTaggingColumn(getSchema(), Collections.singletonList(columnName));

    return TagUtils.getTags(tableTagInfo, columnName, odps);
  }

  /**
   * Get simple tags attached to this table.
   * @return a map from category to key value pairs
   */
  public Map<String, Map<String, String>> getSimpleTags() {
    reloadTagInfo();
    return TagUtils.getSimpleTags(tableTagInfo);
  }

  /**
   * Get simple tags attached to a column of this table.
   *
   * @param columnName column name.
   * @return a map from category to key value pairs.
   */
  public Map<String, Map<String, String>> getSimpleTags(String columnName) {
    reloadTagInfo();

    // Make sure specified column exists
    Objects.requireNonNull(columnName);
    TagUtils.validateTaggingColumn(getSchema(), Collections.singletonList(columnName));

    return TagUtils.getSimpleTags(tableTagInfo, columnName);
  }

  /**
   * Attach a {@link Tag} to this table. The table and tag should be in a same project.
   *
   * @param tag tag to attach
   */
  public void addTag(Tag tag) throws OdpsException {
    addTag(tag, null);
  }


  /**
   * Attach a {@link Tag} to this table. The table and tag should be in a same project.
   *
   * @param tag tag to attach
   * @param columnNames column names, could be null.
   */
  public void addTag(Tag tag, List<String> columnNames) throws OdpsException {
    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.TABLE,
        model.projectName,
        model.name,
        columnNames);
    TagRef tagRef = new TagRef(tag.getClassification(), tag.getName());
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.SET, objectRef, tagRef, null);

    TagUtils.updateTagInternal(setObjectTagInput, null, client);
  }

  /**
   * Attach a simple tag to this table. A simple tag is a triad consisted of category, tag
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
   * Attach a simple tag to this table or some of its columns. A simple tag is a triad consisted of
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
        model.projectName,
        model.name,
        columnNames);
    SimpleTag simpleTag = new SimpleTag(category, Collections.singletonMap(key, value));
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.SET, objectRef, null, simpleTag);

    TagUtils.updateTagInternal(setObjectTagInput, null, client);
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
    TagUtils.validateTaggingColumn(getSchema(), columnNames);

    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.TABLE,
        model.projectName,
        model.name,
        columnNames);
    TagRef tagRef = new TagRef(tag.getClassification(), tag.getName());
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.UNSET, objectRef, tagRef, null);

    TagUtils.updateTagInternal(setObjectTagInput, null, client);
  }

  /**
   * Remove a simple tag. A simple tag is a triad consisted of category, tag key, and tag value.
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
    TagUtils.validateTaggingColumn(getSchema(), columnNames);

    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.TABLE,
        model.projectName,
        model.name,
        columnNames);
    SimpleTag simpleTag = new SimpleTag(category, Collections.singletonMap(key, value));
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.UNSET, objectRef, null, simpleTag);
    TagUtils.updateTagInternal(setObjectTagInput, null, client);
  }

  /**
   * 获取表 ID
   *
   * @return tableId
   */
  public String getTableID() {
    if(model.ID == null) {
      lazyLoad();
    }

    return model.ID;
  }

  /**
   * 获取表加密算法名称
   *
   * @return 算法名称
   */
  public String getCryptoAlgoName() {
    if(model.cryptoAlgoName == null) {
      lazyLoad();
    }

    return model.cryptoAlgoName;
  }


  public String getMaxExtendedLabel() {
    List<String> extendedLabels = new ArrayList<String>();
    if (getTableExtendedLabels() != null) {
      extendedLabels.addAll(getTableExtendedLabels());
    }

    for (Column column : tableSchema.getColumns()) {
      if (column.getExtendedlabels() != null) {
        extendedLabels.addAll(column.getExtendedlabels());
      }
    }

    return calculateMaxLabel(extendedLabels);
  }
  /**
   * 获取最高的label级别
   * Label的定义分两部分：
   * 1. 业务分类：C，S，B
   * 2. 数据等级：1，2，3，4
   *
   * 二者是正交关系，即C1,C2,C3,C4,S1,S2,S3,S4,B1,B2,B3,B4。
   *
   * MaxLabel的语意：
   * 1. MaxLabel=max(TableLabel, ColumnLabel), max(...)函数的语意由Label中的数据等级决定：4>3>2>1
   * 2. MaxLabel显示：
   * 当最高等级Label只出现一次时，MaxLabel=业务分类+数据等级，例如：B4, C3，S2
   * 当最高等级Labe出现多次，但业务分类也唯一，MaxLabel=业务分类+数据等级，例如：B4, C3，S2
   * 当最高等级Labe出现多次，且业务不唯一，MaxLabel=L+数据等级，例如：L4， L3
   *
   * @return 表示最高label，如果没有任何label的设置，返回空字符串
   */
  public String getMaxLabel() {
    List<String> labels = new ArrayList<String>();

    labels.add(getTableLabel());
    for (Column column : tableSchema.getColumns()) {
      labels.add(column.getCategoryLabel());
    }
    return calculateMaxLabel(labels);
  }

  static String calculateMaxLabel(List<String> labels) {
    int maxLevel = 0;
    char category = '-';

    for (String label : labels) {
      if (!StringUtils.isNullOrEmpty(label)) {
        char num = label.charAt(label.length() - 1);
        if (Character.isDigit(num) && num - '0' >= maxLevel) {
          if (num - '0' > maxLevel) {
            maxLevel = num - '0';
            category = '-';
          }

          // label is only one num
          if (label.length() == 1) {
            category = 'L';
            continue;
          }

          // handle one or more letter before the level number
          for (int i = label.length() - 2; i >= 0; i--) {
            char c = label.charAt(i);
            if (Character.isLetter(c)) {
              c = Character.toUpperCase(c);
              if (category == '-') {
                category = c;
              } else if (category != c) {
                category = 'L';
              }
            }
          }
        }
      }
    }

    if (category == '-' && maxLevel == 0) {
      return "";
    }

    if (category == '-') {
      category = 'L';
    }

    return category + "" + maxLevel;
  }

  /**
   * 获取最后修改时间
   *
   * @return 最后修改时间
   */
  public Date getLastMetaModifiedTime() {
    if (model.lastMetaModifiedTime == null) {
      lazyLoad();
    }
    return model.lastMetaModifiedTime;
  }

  /**
   * 获取表所属{@link Project}名称
   *
   * @return Project名称
   */
  public String getProject() {
    return model.projectName;
  }

  /**
   * Get the schema name.
   *
   * @return Schema name.
   */
  public String getSchemaName() {
    return model.schemaName;
  }

  /**
   * 判断表是否为虚拟视图
   *
   * @return 如果是虚拟视图返回true, 否则返回false
   */
  public boolean isVirtualView() {
    // Since reload should always work, if this object is loaded, return the value in the model.
    // If this object is not loaded, but the table type is available, return it. And if this object
    // is not loaded and its table type is unavailable, trigger a reloading.
    // TODO: isVirtualView can be determined by both table type and schema
    if (isLoaded()) {
      return model.isVirtualView;
    } else if (model.type != null) {
      return TableType.VIRTUAL_VIEW.equals(model.type);
    } else {
      lazyLoad();
      return model.isVirtualView;
    }
  }

  /**
   * Return if this table is a materialized view.
   */
  public boolean isMaterializedView() {
    lazyLoad();
    return TableType.MATERIALIZED_VIEW.equals(model.type);
  }

  /**
   * Return if this materialized view could be used by query rewrite.
   *
   * @throws IllegalStateException If this table is not a materialized view.
   */
  public boolean isMaterializedViewRewriteEnabled() {
    lazyLoad();
    if (!isMaterializedView()) {
      throw new IllegalStateException("Not a materialized view");
    }
    return model.isMaterializedViewRewriteEnabled;
  }

  /**
   * Return if this materialized view is outdated.
   *
   * @throws IllegalStateException If this table is not a materialized view.
   */
  public boolean isMaterializedViewOutdated() {
    lazyLoadExtendInfo();
    if (!isMaterializedView()) {
      throw new IllegalStateException("Not a materialized view");
    }
    return model.isMaterializedViewOutdated;
  }

  /**
   * 判断表是否为外部表
   *
   * @return 如果是外部表返回true, 否则返回false
   */
  public boolean isExternalTable() {
    // Since reload should always work, if this object is loaded, return the value in the model.
    // If this object is not loaded, but the table type is available, return it. And if this object
    // is not loaded and its table type is unavailable, trigger a reloading.
    // TODO: isVirtualView can be determined by both table type and schema
    if (isLoaded()) {
      return model.isExternalTable;
    } else if (model.type != null) {
      return TableType.EXTERNAL_TABLE.equals(model.type);
    } else {
      lazyLoad();
      return model.isExternalTable;
    }
  }

  /**
   * 获取视图的文本内容
   *
   * @return 文本内容
   */
  public String getViewText() {
    if (model.viewText == null) {
      lazyLoad();
    }
    return model.viewText;
  }

  /**
   * 获取数据最后修改时间
   *
   * @return 最后修改时间
   */
  public Date getLastDataModifiedTime() {
    if (model.lastModifiedTime == null) {
      lazyLoad();
    }
    return model.lastModifiedTime;
  }

  /**
   * 获取内部存储大小，单位：Byte
   *
   * @return 存储大小
   */
  public long getSize() {
    lazyLoad();
    return model.size;
  }

  /**
   * 获取表的Record数, 若无准确数据，则返回-1
   *
   * @return 表的record数
   */
  public long getRecordNum() {
    lazyLoad();
    return model.recordNum;
  }

  /**
   * 获取表的生命周期值，单位：天
   *
   * @return 生命周期值
   */
  public long getLife() {
    lazyLoad();
    return model.life;
  }

  /**
   * 获取表的datahub生命周期值，单位：天
   *
   * @return datahub生命周期值
   */
  public long getHubLifecycle() {
    lazyLoad();
    return model.hubLifecycle;
  }

  /**
   * 获取表结构定义
   *
   * @return 表示表结构的{@link TableSchema}对象
   */
  public TableSchema getSchema() {
    if (tableSchema == null) {
      lazyLoad();
    }
    return tableSchema;
  }

  public String getJsonSchema() {
    if (model.schema == null || model.schema.content == null) {
      lazyLoad();
    }

    return model.schema == null ? null : model.schema.content;
  }

  /**
   * 查看表是否进行过归档操作
   *
   * @return 返回true表示进行过archive操作，false表示未进行过
   */
  public boolean isArchived() {
    lazyLoadExtendInfo();
    return model.isArchived;
  }


  /**
   * 查看表是否事务化
   *
   * @return 返回true表示进行过 transactional 操作，false表示未进行过
   */
  public boolean isTransactional() {
    lazyLoadExtendInfo();
    return model.isTransactional;
  }

  /**
   * 查看表所占磁盘的物理大小
   *
   * @return 物理大小
   */
  public long getPhysicalSize() {
    lazyLoadExtendInfo();
    return model.physicalSize;
  }

  /**
   * 返回表数据所占的盘古文件数
   *
   * @return 文件数
   */
  public long getFileNum() {
    lazyLoadExtendInfo();
    return model.fileNum;
  }

  /**
   * 返回外部表数据存储位置
   *
   * @return 外部表数据存储位置
   */
  public String getLocation() {
    if (model.location == null) {
      lazyLoadExtendInfo();
    }

    return model.location;
  }

  /**
   * 返回外部表数据处理句柄
   *
   * @return 外部表数据处理句柄
   */
  public String getStorageHandler() {
    if (model.storageHandler == null) {
      lazyLoadExtendInfo();
    }

    return model.storageHandler;
  }

  /**
   * 返回外部表使用的资源
   *
   * @return 外部表使用的资源
   */
  public String getResources() {
    if (model.resources == null) {
      lazyLoadExtendInfo();
    }

    return model.resources;
  }

  /**
   * 返回外部表序列化和反序列化属性
   *
   * @return 外部表序列化和反序列化属性
   */
  public Map<String, String> getSerDeProperties() {
    if (model.serDeProperties == null) {
      lazyLoadExtendInfo();
    }

    return model.serDeProperties;
  }


  /**
   * 返回扩展信息的保留字段
   * json 字符串
   *
   * @return 保留字段
   */
  public String getReserved() {
    if (model.reserved == null) {
      lazyLoadExtendInfo();
    }
    return model.reserved;
  }

  /**
   * 返回 cluster range 表的 cluster 信息
   *
   * @return cluster info
   */
  public ClusterInfo getClusterInfo() {
    if (model.clusterInfo == null) {
      lazyLoadExtendInfo();
    }

    return model.clusterInfo;
  }

  /**
   * 返回Shard
   *
   * @return shard 如果没有shard返回null
   */
  public Shard getShard() {
    if (model.shard == null) {
      lazyLoad();
    }
    return model.shard;
  }

  /**
   * 读取表内的数据
   *
   * @param limit
   *     最多读取的记录行数
   * @return {@link RecordReader}对象
   * @throws OdpsException
   */
  public RecordReader read(int limit) throws OdpsException {
    //TODO FIX DATE
    return read(null, null, limit);
  }

  /**
   * 读取表内的数据 <br />
   * 读取数据时，最多返回 1W 条记录，若超过，数据将被截断。<br />
   * 另外，读取的数据大小不能超过 10MB，否则将抛出异常。<br />
   *
   * @param partition
   *     表的分区{@link PartitionSpec}。如不指定分区可传入null。
   * @param columns
   *     所要读取的列名的列表。如果读取全表可传入null
   * @param limit
   *     最多读取的记录行数。
   *
   * @return {@link RecordReader}对象
   * @throws OdpsException
   */
  public RecordReader read(PartitionSpec partition, List<String> columns, int limit)
      throws OdpsException {
    //TODO FIX DATE
    return read(partition, columns, limit, null);
  }

  /**
   * 读取表内的数据 <br />
   * 读取数据时，最多返回 1W 条记录，若超过，数据将被截断。<br />
   * 另外，读取的数据大小不能超过 10MB，否则将抛出异常。<br />
   *
   * @param partition
   *     表的分区{@link PartitionSpec}。如不指定分区可传入null。
   * @param columns
   *     所要读取的列名的列表。如果读取全表可传入null
   * @param limit
   *     最多读取的记录行数。
   * @param timezone
   *     设置 datetime 类型数据的时区
   *
   * @return {@link RecordReader}对象
   * @throws OdpsException
   */
  public RecordReader read(PartitionSpec partition, List<String> columns, int limit, String timezone)
      throws OdpsException {
    //TODO FIX DATE
    if (limit < 0) {
      // TODO: should throw IllegalArgumentException
      throw new OdpsException("limit number should >= 0.");
    }
    Map<String, String> params = initParamsWithSchema();
    params.put("data", null);


    if (partition != null && partition.keys().size() > 0) {
      params.put("partition", partition.toString());
    }

    if (columns != null && columns.size() != 0) {
      String column = "";
      for (String temp : columns) {
        column += temp;
        column += ",";
      }
      column = column.substring(0, column.lastIndexOf(","));
      params.put("cols", column);
    }

    if (limit != -1) {
      params.put("linenum", String.valueOf(limit));
    }

    Map<String, String> header = null;
    if (timezone != null) {
      header = new HashMap<String, String>();
      header.put("x-odps-sql-timezone", timezone);
    }

    String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
    Response resp = client.request(resource, "GET", params, header, null);
    return new DefaultRecordReader(new ByteArrayInputStream(resp.getBody()), getSchema());
  }

  private TableSchema loadSchemaFromJson(String json) {
    TableSchema s = new TableSchema();
    try {
      JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

      if (tree.has("comment")) {
        model.comment = tree.get("comment").getAsString();
      }

      if (tree.has("owner")) {
        model.owner = tree.get("owner").getAsString();
      }

      if (tree.has("createTime")) {
        model.createdTime = new Date(tree.get("createTime").getAsLong() * 1000);
      }

      if (tree.has("lastModifiedTime")) {
        model.lastModifiedTime = new Date(tree.get("lastModifiedTime").getAsLong() * 1000);
      }

      if (tree.has("lastDDLTime")) {
        model.lastMetaModifiedTime = new Date(tree.get("lastDDLTime").getAsLong() * 1000);
      }

      if (tree.has("isVirtualView")) {
        model.isVirtualView = tree.get("isVirtualView").getAsBoolean();
      }

      if (tree.has("isMaterializedView") && tree.get("isVirtualView").getAsBoolean()) {
        model.type = TableType.MATERIALIZED_VIEW;
      }

      if (tree.has("isMaterializedViewRewriteEnabled")) {
        model.isMaterializedViewRewriteEnabled =
            tree.get("isMaterializedViewRewriteEnabled").getAsBoolean();
      }

      if (tree.has("IsMaterializedViewOutdated")) {
        model.isMaterializedViewOutdated = tree.get("IsMaterializedViewOutdated").getAsBoolean();
      }

      if (tree.has("isExternal")) {
        model.isExternalTable = tree.get("isExternal").getAsBoolean();
      }

      if (tree.has("lifecycle")) {
        model.life = tree.get("lifecycle").getAsLong();
      }

      if (tree.has("hubLifecycle")) {
        model.hubLifecycle = tree.get("hubLifecycle").getAsLong();
      }

      if (tree.has("viewText")) {
        model.viewText = tree.get("viewText").getAsString();
      }

      if (tree.has("size")) {
        model.size = tree.get("size").getAsLong();
      }

      if (tree.has("IsArchived")) {
        model.isArchived = tree.get("IsArchived").getAsBoolean();
      }

      if (tree.has("PhysicalSize")) {
        model.physicalSize = tree.get("PhysicalSize").getAsLong();
      }

      if (tree.has("FileNum")) {
        model.fileNum = tree.get("FileNum").getAsLong();
      }

      if (tree.has("recordNum")) {
        model.recordNum = tree.get("recordNum").getAsLong();
      }

      if (tree.has("storageHandler")) {
        model.storageHandler = tree.get("storageHandler").getAsString();
      }

      if (tree.has("location")) {
        model.location = tree.get("location").getAsString();
      }

      if (tree.has("resources")) {
        model.resources = tree.get("resources").getAsString();
      }

      if (tree.has("serDeProperties")) {
        model.serDeProperties = new GsonBuilder().disableHtmlEscaping().create()
                .fromJson(tree.get("serDeProperties").getAsString(),
                new TypeToken<Map<String, String>>() {}.getType());
      }

      if (tree.has("shardExist")) {
        boolean shardExist = tree.get("shardExist").getAsBoolean();
        if (shardExist && tree.has("shardInfo")) {
          model.shard = Shard.parseShard(tree.get("shardInfo").getAsJsonObject());
        } else {
          model.shard = null;
        }
      }

      if (tree.has("tableLabel")) {
        model.tableLabel = tree.get("tableLabel").getAsString();
        // Service will return 0 if nothing set
        if (model.tableLabel.equals("0")) {
          model.tableLabel = "";
        }
      }

      if (tree.has("columns") && tree.get("columns") != null) {
        JsonArray columnsNode = tree.get("columns").getAsJsonArray();
        for (int i = 0; i < columnsNode.size(); ++i) {
          JsonObject n = columnsNode.get(i).getAsJsonObject();
          s.addColumn(ColumnUtils.fromJson(n.toString()));
        }
      }

      if (tree.has("extendedLabel")) {
        JsonArray tableExtendedLabels = tree.get("extendedLabel").getAsJsonArray();
        if (tableExtendedLabels.size() != 0) {
          List<String> labelList = new LinkedList<String>();
          for (JsonElement label : tableExtendedLabels) {
            labelList.add(label.getAsString());
          }
          model.tableExtendedLabels = labelList;
        }
      }

      if (tree.has("partitionKeys") && tree.get("partitionKeys") != null) {
        JsonArray columnsNode = tree.get("partitionKeys").getAsJsonArray();
        for (int i = 0; i < columnsNode.size(); ++i) {
          JsonObject n = columnsNode.get(i).getAsJsonObject();
          s.addPartitionColumn(ColumnUtils.fromJson(n.toString()));
        }
      }

      if (tree.has("Reserved")) {
        model.reserved = tree.get("Reserved").getAsString();
        loadReservedJson(model.reserved);
      }

    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    return s;
  }

  private void loadReservedJson(String reserved) {
    JsonObject reservedJson = new JsonParser().parse(reserved).getAsJsonObject();

    // load cluster info
    model.clusterInfo = parseClusterInfo(reservedJson);
    model.isTransactional = parseTransactionalInfo(reservedJson);
  }

  private static boolean parseTransactionalInfo(JsonObject jsonObject) {
    if (!jsonObject.has("Transactional")) {
      return false;
    }

    return Boolean.parseBoolean(jsonObject.get("Transactional").getAsString());
  }

  public static ClusterInfo parseClusterInfo(JsonObject jsonObject) {
    if (!jsonObject.has("ClusterType")) {
      return null;
    }

    ClusterInfo clusterInfo = new ClusterInfo();
    clusterInfo.clusterType = jsonObject.has("ClusterType") ? jsonObject.get("ClusterType").getAsString() : null;
    clusterInfo.bucketNum = jsonObject.has("BucketNum") ? jsonObject.get("BucketNum").getAsLong() : 0L;
    JsonArray array = jsonObject.has("ClusterCols") ? jsonObject.get("ClusterCols").getAsJsonArray() : null;
    if (array != null) {
      clusterInfo.clusterCols = new ArrayList<String>();
      for (int i = 0; i < array.size(); ++i) {
        clusterInfo.clusterCols.add(array.get(i).getAsString());
      }
    }

    if (jsonObject.has("SortCols")) {
      array = jsonObject.get("SortCols").getAsJsonArray();
      clusterInfo.sortCols = new ArrayList<SortColumn>();
      for (int i = 0; i < array.size(); ++i) {
        JsonObject obj = array.get(i).getAsJsonObject();
        if (obj != null) {
          clusterInfo.sortCols.add(new SortColumn(obj.get("col").getAsString(), obj.get("order").getAsString()));
        }
      }
    }

    return clusterInfo;
  }

  /**
   * 增加分区
   *
   * @param spec
   *     分区定义 {@link PartitionSpec}
   * @throws OdpsException
   */
  public void createPartition(PartitionSpec spec) throws OdpsException {
    createPartition(spec, false);
  }

  /**
   * 增加分区
   *
   * @param spec
   *     分区定义 {@link PartitionSpec}
   * @param ifNotExists
   *     在创建分区时，如果为 false 而存在同名分区，则返回出错；若为 true，则无论是否存在同名分区，即使分区结构与要创建的目标分区结构不一致，均返回成功。已存在的同名分区的元信息不会被改动。
   * @throws OdpsException
   */
  public void createPartition(PartitionSpec spec, boolean ifNotExists) throws OdpsException {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ").append(getCoordinate());

    sb.append(" ADD");

    if (ifNotExists) {
      sb.append(" IF NOT EXISTS");
    }

    sb.append(" PARTITION (");

    String[] keys = spec.keys().toArray(new String[0]);
    for (int i = 0; i < keys.length; i++) {
      sb.append(keys[i]).append("='").append(spec.get(keys[i])).append("'");

      if (i + 1 < keys.length) {
        sb.append(',');
      }
    }
    sb.append(");");
    // new SQLTask
    String taskName = "SQLAddPartitionTask";
    runSQL(taskName, sb.toString());
  }


  /**
   * 删除指定分区
   *
   * @param spec
   *     分区定义 {@link PartitionSpec}
   * @throws OdpsException
   */
  public void deletePartition(PartitionSpec spec) throws OdpsException {
    deletePartition(spec, false);
  }

  /**
   * 删除指定分区
   *
   * @param spec
   *     分区定义 {@link PartitionSpec}
   * @param ifExists
   *     如果 false 而分区不存在，则返回异常；若为 true，无论分区是否存在，皆返回成功。
   * @throws OdpsException
   */
  public void deletePartition(PartitionSpec spec, boolean ifExists) throws OdpsException {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ").append(getCoordinate());
    sb.append(" DROP");

    if (ifExists) {
      sb.append(" IF EXISTS");
    }

    sb.append(" PARTITION(");

    String[] keys = spec.keys().toArray(new String[0]);
    for (int i = 0; i < keys.length; i++) {
      sb.append(keys[i]).append("='").append(spec.get(keys[i])).append("'");

      if (i + 1 < keys.length) {
        sb.append(',');
      }
    }
    sb.append(");");

    // new SQLTask
    String taskName = "SQLDropPartitionTask";
    runSQL(taskName, sb.toString());
  }

  // for list partition response
  @Root(name = "Partitions", strict = false)
  private static class ListPartitionsResponse {

    @ElementList(entry = "Partition", inline = true, required = false)
    private List<PartitionModel> partitions = new LinkedList<PartitionModel>();

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;
  }


  /**
   * 在Table上创建Shards
   *
   * @param shardCount
   *     创建Shard的个数
   */
  public void createShards(long shardCount) throws OdpsException {
    StringBuilder sb = new StringBuilder();
    // TODO: not sure this sql support schema
    sb.append("ALTER TABLE ").append(getCoordinate());
    sb.append(String.format(" INTO %d SHARDS;", shardCount));
    String taskName = "SQLCreateShardsTask";
    runSQL(taskName, sb.toString());
  }

  /**
   * 获取分区迭代器
   *
   * @return {@link Partition} 分区迭代器
   */
  public Iterator<Partition> getPartitionIterator() {
    return getPartitionIterator(null);
  }

  /**
   * 获取分区迭代器
   *
   * @param spec
   *     指定的上级分区 {@link PartitionSpec}
   * @return {@link Partition}迭代器
   */
  public Iterator<Partition> getPartitionIterator(final PartitionSpec spec) {
    return getPartitionIterator(spec, false, 1000L, Long.MAX_VALUE);
  }

  /**
   * Get a partition iterator.
   *
   * @param spec Specify the values of some of the partition columns. The specified columns'
   * indices should be continuous and start from 0.
   * @param reverse Reverse the result. The original
   * @param batchSize Max number of partitions to get per request. In case of null, the batch size
   * will be decided by the server.
   * @param limit Limit the number of returned partitions. In case of null, {@link Long#MAX_VALUE}
   * will be used.
   * @return A partition iterator.
   */
  public Iterator<Partition> getPartitionIterator(
      PartitionSpec spec,
      boolean reverse,
      Long batchSize,
      Long limit) {
    if (limit != null && limit <= 0) {
      throw new IllegalArgumentException("Argument 'limit' should be greater than 0");
    }
    if (batchSize != null && batchSize <= 0) {
      throw new IllegalArgumentException("Argument 'batchSize' should be greater than 0");
    }

    final long finalLimit = limit == null ? Long.MAX_VALUE : limit;
    return new ListIterator<Partition>() {
      long numPartitions = 0;
      Map<String, String> params = new HashMap<>();

      @Override
      public boolean hasNext() {
        return super.hasNext() && numPartitions < finalLimit;
      }

      @Override
      public Partition next() {
        Partition partition = super.next();
        numPartitions += 1;
        return partition;
      }

      @Override
      protected List<Partition> list() {
        ArrayList<Partition> partitions = new ArrayList<>();
        params.put("partitions", null);
        params.put("expectmarker", "true"); // since sprint-11
        if (spec != null && !spec.isEmpty()) {
          params.put("partition", spec.toString());
        }
        if (reverse) {
          params.put("reverse", null);
        }
        if (batchSize != null) {
          params.put("maxitems", batchSize.toString());
        }

        String lastMarker = params.get("marker");
        if (params.containsKey("marker") && lastMarker.length() == 0) {
          return null;
        }

        String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
        try {

          params.putAll(initParamsWithSchema());
          ListPartitionsResponse
              resp =
              client.request(ListPartitionsResponse.class, resource, "GET", params);

          for (PartitionModel partitionModel : resp.partitions) {
            Partition t = new Partition(
                partitionModel,
                model.projectName,
                model.schemaName,
                model.name,
                odps);
            partitions.add(t);
          }

          params.put("marker", resp.marker);
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }

        return partitions;
      }
    };
  }

  /**
   * 获取所有分区信息
   *
   * @return {@link Partition}列表
   */
  public List<Partition> getPartitions() {
    ArrayList<Partition> parts = new ArrayList<>();
    Iterator<Partition> it = getPartitionIterator();
    while (it.hasNext()) {
      parts.add(it.next());
    }
    return parts;
  }


  /**
   * 获取指定分区信息
   *
   * @param spec
   *     分区定义 {@link PartitionSpec}
   * @return 分区信息 {@link Partition}
   */
  public Partition getPartition(PartitionSpec spec) {
    return new Partition(spec, model.projectName, model.schemaName, model.name, odps);
  }

  /**
   * 判断指定分区是否存在
   *
   * @param spec
   *     分区定义 {@link PartitionSpec}
   * @return 如果指定分区存在，则返回true，否则返回false
   * @throws OdpsException
   */
  public boolean hasPartition(PartitionSpec spec) throws OdpsException {
    try {
      Partition part = getPartition(spec);
      part.reload();
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  /**
   * 删除表数据
   *
   * @throws OdpsException
   */
  public void truncate() throws OdpsException {
    StringBuilder sb = new StringBuilder();
    sb.append("TRUNCATE TABLE ").append(getCoordinate()).append(";");

    String taskName = "SQLTruncateTask";
    runSQL(taskName, sb.toString());
  }

  /**
   * 判断是否  Partition 表
   *
   * @return 是否为 Partition 表
   * @throws OdpsException
   */
  public boolean isPartitioned() throws OdpsException {
    if (isVirtualView()) {
      return false;
    }
    return getSchema().getPartitionColumns().size() > 0;
  }

  private void runSQL(String taskName, String query) throws OdpsException {
    Map<String, String> hints = NameSpaceSchemaUtils.setSchemaFlagInHints(null, model.schemaName);
    Instance i = SQLTask.run(odps, odps.getDefaultProject(), query, taskName, hints, null);
    i.waitForSuccess();
  }

  private HashMap<String, String> initParamsWithSchema() throws OdpsException {
    return NameSpaceSchemaUtils.initParamsWithSchema(model.schemaName);
  }

  private String getCoordinate() throws OdpsException {
    return NameSpaceSchemaUtils.getFullName(model.projectName, model.schemaName, model.name);
  }
}
