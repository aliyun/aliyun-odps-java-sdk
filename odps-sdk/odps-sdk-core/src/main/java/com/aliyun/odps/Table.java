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

import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.apache.arrow.vector.ipc.ArrowStreamReader;

import com.aliyun.odps.Partition.PartitionModel;
import com.aliyun.odps.Partition.PartitionSpecModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.data.ArrowReaderWrapper;
import com.aliyun.odps.data.ArrowStreamRecordReader;
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
import com.aliyun.odps.table.StreamIdentifier;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.aliyun.odps.utils.ColumnUtils;
import com.aliyun.odps.utils.CommonUtils;
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
import com.google.gson.Gson;
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
    MATERIALIZED_VIEW,
    /**
     * Object table
     */
    OBJECT_TABLE
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

    @Element(name = "TableMaskInfo", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String tableMaskInfo;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModifiedTime;

    @Element(name = "LastAccessTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastAccessTime;

    @Element(name = "Type", required = false)
    @Convert(TableTypeConverter.class)
    TableType type;

    Date lastMetaModifiedTime;
    Date lastMajorCompactTime;
    boolean isVirtualView;
    boolean isMaterializedViewRewriteEnabled;
    boolean isMaterializedViewOutdated;
    boolean isExternalTable;
    boolean isObjectTable;
    long life = -1L;
    long hubLifecycle = -1L;
    String viewText;
    String viewExpandedText;
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
    String schemaVersion;
    String storageHandler;
    String location;
    String resources;
    Map<String, String> serDeProperties;

    // for clustered info
    ClusterInfo clusterInfo;
    // for table extended labels
    List<String> tableExtendedLabels;

    Map<String, String> mvProperties;

    List<Map<String, String>> refreshHistory;

    boolean hasRowAccessPolicy;
    List<String> primaryKey;
    int acidDataRetainHours;
    StorageTierInfo storageTierInfo;
    TableLifecycleConfig tableLifecycleConfig;

    List<ColumnMaskInfo> columnMaskInfoList;

    long cdcSize = -1;
    long cdcRecordNum = -1;
    long cdcLatestVersion = -1;
    Date cdcLatestTimestamp;
  }

  public static class ColumnMaskInfo {

    private final String name;
    private final List<String> policyNameList;

    public String getName() {
      return name;
    }

    public List<String> getPolicyNameList() {
      return policyNameList;
    }

    ColumnMaskInfo(String name, List<String> policyNameList) {
      this.name = name;
      this.policyNameList = policyNameList;
    }
  }


  /**
   * ClusterInfo is used to express the Shuffle and Sort properties of the table when creating a clustered table.
   */
  public static class ClusterInfo {

    enum ClusterType {
      HASH,
      RANGE
    }

    long bucketNum = -1;

    ClusterType clusterType;
    List<String> clusterCols;
    List<SortColumn> sortCols;

    ClusterInfo() {}

    /**
     * @param clusterType Clustering tables are divided into two types: Hash clustering tables and Range clustering tables.
     * @param clusterCols Specify cluster by. MaxCompute will perform Hash/Range operations on the specified columns and distribute them to various Buckets.
     * @param sortCols Specify the sorting method of fields in the Bucket. It is recommended that sorted by and clustered by be consistent to achieve better performance.
     * @param bucketNum Specify the number of hash buckets. Required when using Hash Cluster.
     */
    public ClusterInfo(ClusterType clusterType, List<String> clusterCols, List<SortColumn> sortCols, long bucketNum) {
      this.clusterType = clusterType;
      this.clusterCols = clusterCols;
      this.sortCols = sortCols;
      this.bucketNum = bucketNum;
    }

    public String getClusterType() {
      // to lower case is to keep consistency with old version
      return clusterType.name().toLowerCase();
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

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      if (clusterType == ClusterType.HASH) {
        stringBuilder.append(" CLUSTERED BY ");
      } else {
        stringBuilder.append(" RANGE CLUSTERED BY ");
      }
      stringBuilder.append("(").append(
              clusterCols.stream().map(CommonUtils::quoteRef).collect(Collectors.joining(", ")))
          .append(")");
      if (sortCols != null && sortCols.size() > 0) {
        stringBuilder.append(" SORTED BY ").append("(")
            .append(sortCols.stream().map(SortColumn::toStringWithQuote).collect(Collectors.joining(", ")))
            .append(")");
      }
      if (bucketNum > 0) {
        stringBuilder.append(" INTO ").append(bucketNum).append(" BUCKETS");
      }
      return stringBuilder.toString();
    }
  }

  /**
   * Used in ClusterInfo to specify the sorting method of fields in the Bucket.
   */
  public static class SortColumn {
    enum Order {
      ASC,
      DESC
    }

    private String name;
    private Order order;

    SortColumn(String name, String order) {
      this.name = name;
      this.order = Order.valueOf(order.toUpperCase());
    }

    public SortColumn(String name, Order order) {
      this.name = name;
      this.order = order;
    }

    public String getName() {
      return name;
    }

    public String getOrder() {
      return order.name();
    }

    @Override
    public String toString() {
      return String.format("%s %s", name, order);
    }

    public String toStringWithQuote() {
      return String.format("%s %s", CommonUtils.quoteRef(name), order);
    }
  }

  private TableModel model;
  private TableSchema tableSchema;
  private ObjectTagInfo tableTagInfo;
  private RestClient client;
  private boolean isExtendInfoLoaded;
  private boolean isShardInfoLoaded;
  private Odps odps;

  // Raw Json metadata
  private String metadataJson;
  private String extendedInfoJson;

  Table(TableModel model, String project, String schemaName, Odps odps) {
    this.model = model;
    this.model.projectName = project;
    this.model.schemaName = schemaName;
    this.odps = odps;
    this.client = odps.getRestClient();
    this.isExtendInfoLoaded = false;
    this.isShardInfoLoaded = false;
  }


  protected void setIsExtendInfoLoaded(boolean isExtendInfoLoaded) {
    this.isExtendInfoLoaded = isExtendInfoLoaded;
  }
  @Override
  public void reload() throws OdpsException {
    odps.projects().get(model.projectName).executeIfEpv2(
        () -> {
          tableSchema = loadEpv2TableFromJson(model.projectName, model.schemaName,model.name);
          this.setLoaded(true);
          return null;
        },
        () -> {
          String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
          Map<String, String> params = initParamsWithSchema();
          reload(client.request(TableModel.class, resource, "GET", params));
          this.metadataJson = model.schema.content;
          return null;
        }
    );
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

  public void reloadExtendInfo() {
    TableModel response;
    try {
      Map<String, String> params = initParamsWithSchema();
      params.put("extended", null);

      String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
      response = client.request(TableModel.class, resource, "GET", params);
    } catch (OdpsException e) {
      throw new ReloadException(e.getMessage(), e);
    }
    this.extendedInfoJson = response.schema.content;
    loadSchemaFromJson(response.schema.content);
  }

  private void lazyLoadExtendInfo() {
    if (!this.isExtendInfoLoaded) {
      try {
        odps.projects().get(model.projectName).executeIfEpv2(() -> {
          this.isExtendInfoLoaded = true;
          return null;
        }, () -> {
          reloadExtendInfo();
          this.isExtendInfoLoaded = true;
          return null;
        });
      } catch (OdpsException e) {
        throw new UncheckedOdpsException(e);
      }
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
   * @return 表类型
   */
  public TableType getType() {
    if (model.type == null) {
      lazyLoad();
    }
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

  public String getSchemaVersion() {
    // no cache
    reloadExtendInfo();
    return model.schemaVersion;
  }

  /**
   * 获取分层存储的相关信息，包括类型，大小，修改时间等等
   *
   * @return StorageTierInfo 分层存储信息
   */
  public StorageTierInfo getStorageTierInfo() {
    if (isPartitioned()) {
      throw new UnsupportedOperationException(
          "Partitioned table does not support get storage tier info, use Partition.getStorageTierInfo() instead.");
    }
    if (model.storageTierInfo == null) {
      reloadExtendInfo();
      isExtendInfoLoaded = true;
    }
    return model.storageTierInfo;
  }

  /**
   * 获取分层存储的lifecycle配置
   *
   * @return TableLifecycleConfig
   * */
  public TableLifecycleConfig getTableLifecycleConfig() {
    if (model.tableLifecycleConfig == null) {
      reloadExtendInfo();
      isExtendInfoLoaded = true;
    }
    return model.tableLifecycleConfig;
  }

  /**
   * Get {@link Tag}(s) attached to this table.
   *
   * @return list of {@link Tag}
   */
  public List<Tag> getTags() {
    reloadTagInfo();
    return TagUtils.getTags(tableTagInfo, odps);
  }

  /**
   * Get {@link Tag}(s) attached to a column of this table.
   *
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
   *
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
   * @param tag         tag to attach
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
   * @param key      simple tag key, cannot be null.
   * @param value    simple tag value, cannot be null.
   */
  public void addSimpleTag(String category, String key, String value) throws OdpsException {
    addSimpleTag(category, key, value, null);
  }

  /**
   * Attach a simple tag to this table or some of its columns. A simple tag is a triad consisted of
   * category, tag key, and tag value.
   *
   * @param category    simple tag category, could be nul.
   * @param key         simple tag key, cannot be null.
   * @param value       simple tag value, cannot be null.
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
   * @param tag         tag to remove.
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
   *
   * @param category category.
   * @param key      key.
   * @param value    value.
   * @throws OdpsException
   */
  public void removeSimpleTag(String category, String key, String value) throws OdpsException {
    removeSimpleTag(category, key, value, null);
  }

  /**
   * Remove a simple tag from columns. A simple tag is a triad consisted of category, tag key, and
   * tag value.
   *
   * @param category    category.
   * @param key         key.
   * @param value       value.
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
    if (model.ID == null) {
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
    if (model.cryptoAlgoName == null) {
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
   * <p>
   * 二者是正交关系，即C1,C2,C3,C4,S1,S2,S3,S4,B1,B2,B3,B4。
   * <p>
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

  public boolean isObjectTable() {
    if (isLoaded()) {
      return model.isObjectTable;
    } else if (model.type != null) {
      return TableType.OBJECT_TABLE.equals(model.type);
    } else {
      lazyLoad();
      return model.isObjectTable;
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

  public String getViewExpandedText() {
    if (model.viewExpandedText == null) {
      lazyLoad();
    }
    return model.viewExpandedText;
  }

//  {
//    "columnMaskInfoList": [
//          {
//            "name": "s1",
//            "policyNameList": ["wuyue_mask_ba"]
//          }
//          ]
//  }
  public List<ColumnMaskInfo> getColumnMaskInfo() {
    if (model.columnMaskInfoList != null) {
      return model.columnMaskInfoList;
    }
    if (model.tableMaskInfo == null) {
      lazyLoad();
    }
    if (StringUtils.isNullOrEmpty(model.tableMaskInfo)) {
      return null;
    }
    Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    JsonArray jsonArray = JsonParser.parseString(model.tableMaskInfo)
            .getAsJsonObject()
            .getAsJsonArray("columnMaskInfoList");

    model.columnMaskInfoList = new ArrayList<>(jsonArray.size());
    for (int i = 0; i < jsonArray.size(); i++) {
      model.columnMaskInfoList.add(gson.fromJson(jsonArray.get(i), ColumnMaskInfo.class));
    }
    return model.columnMaskInfoList;
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
   * 获取数据最后访问时间
   *
   * @return 最后访问时间
   */
  public Date getLastDataAccessTime() {
    if (model.lastAccessTime == null) {
      lazyLoad();
    }
    return model.lastAccessTime;
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

  public String getMetadataJson() {
    lazyLoad();
    return metadataJson;
  }

  public String getExtendedInfoJson() {
    lazyLoadExtendInfo();
    return extendedInfoJson;
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
   * 查看表是否为 delta table
   */
  public boolean isDeltaTable() {
    List<String> primaryKey = getPrimaryKey();
    return isTransactional() && primaryKey != null && primaryKey.size() > 0;
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
   * @return 最后一次major compact的时间
   */
  public Date getLastMajorCompactTime() {
    if (model.lastMajorCompactTime == null) {
      lazyLoadExtendInfo();
    }
    return model.lastMajorCompactTime;
  }

  /**
   * 读取表内的数据
   *
   * @param limit 最多读取的记录行数
   * @return {@link RecordReader}对象
   * @throws OdpsException
   */
  public RecordReader read(int limit) throws OdpsException {
    return read(null, null, limit);
  }

  /**
   * 读取表内的数据 <br />
   * 读取数据时，最多返回 1w 条记录 (project read 默认值），若超过，数据将被截断。<br />
   *
   * @param partition 表的分区{@link PartitionSpec}。如不指定分区可传入null。
   * @param columns   所要读取的列名的列表。如果读取全表可传入null
   * @param limit     最多读取的记录行数。
   * @return {@link RecordReader}对象
   * @throws OdpsException
   */
  public RecordReader read(PartitionSpec partition, List<String> columns, int limit)
      throws OdpsException {
    return read(partition, columns, limit, null);
  }

  /**
   * 读取表内的数据 <br />
   * 读取数据时，最多返回 1w 条记录 (project read 默认值），若超过，数据将被截断。<br />
   *
   * @param partition 表的分区{@link PartitionSpec}。如不指定分区可传入null。
   * @param columns   所要读取的列名的列表。如果读取全表可传入null
   * @param limit     最多读取的记录行数。
   * @param timezone  设置 datetime 类型数据的时区，新接口使用Java8无时区类型，指定Timezone无效。
   * @return {@link RecordReader}对象
   * @throws OdpsException
   */
  public RecordReader read(PartitionSpec partition, List<String> columns, int limit,
                           String timezone) throws OdpsException {
    return read(partition, columns, limit, timezone, false);
  }

  /**
   * 读取表内的数据 <br />
   * 读取数据时，最多返回 1w 条记录 (project read 默认值），若超过，数据将被截断。<br />
   *
   * @param partition 表的分区{@link PartitionSpec}。如不指定分区可传入null。
   * @param columns   所要读取的列名的列表。如果读取全表可传入null
   * @param limit     最多读取的记录行数。
   * @param timezone  设置 datetime 类型数据的时区，新接口使用Java8无时区类型，故指定Timezone无效
   * @param useLegacyMode  是否使用兼容旧Read接口模式，默认为false。老接口性能较差，不推荐。
   * @see <a href="https://aliyuque.antfin.com/odps/sdk/pi9ec3d5tly76af8?singleDoc#">《新老read接口区别》</a>
   * @return {@link ArrowStreamRecordReader}
   * @throws OdpsException
   */
  public RecordReader read(PartitionSpec partition, List<String> columns, int limit,
                           String timezone, boolean useLegacyMode)
      throws OdpsException {
    return read(partition, columns, limit, timezone, useLegacyMode, null);
  }

  /**
   * 读取表内的数据 <br />
   * 读取数据时，最多返回 1w 条记录 (project read 默认值），若超过，数据将被截断。<br />
   *
   * @param partition 表的分区{@link PartitionSpec}。如不指定分区可传入null。
   * @param columns   所要读取的列名的列表。如果读取全表可传入null
   * @param limit     最多读取的记录行数。
   * @param timezone  设置 datetime 类型数据的时区，新接口使用Java8无时区类型，故指定Timezone无效
   * @param useLegacyMode  是否使用兼容旧Read接口模式，默认为false。老接口性能较差，不推荐。
   * @see <a href="https://aliyuque.antfin.com/odps/sdk/pi9ec3d5tly76af8?singleDoc#">《新老read接口区别》</a>
   * @return {@link ArrowStreamRecordReader}
   * @throws OdpsException
   */
  public RecordReader read(PartitionSpec partition, List<String> columns, int limit,
                           String timezone, boolean useLegacyMode, String tunnelEndpoint)
      throws OdpsException {
    if (limit <= 0) {
      throw new OdpsException("ODPS-0420061: Invalid parameter in HTTP request - 'linenum' must be bigger than zero!");
    }
    TableSchema schema = getSchema();

    // apply odps network settings to table tunnel
    int readTimeout = odps.getRestClient().getReadTimeout();
    int connectTimeout = odps.getRestClient().getConnectTimeout();
    int socketRetryTimes = odps.getRestClient().getRetryTimes();
    Configuration tunnelConfig = new Configuration(odps);
    tunnelConfig.setSocketTimeout(readTimeout);
    tunnelConfig.setSocketConnectTimeout(connectTimeout);
    tunnelConfig.setSocketRetryTimes(socketRetryTimes);

    TableTunnel tableTunnel = new TableTunnel(odps, tunnelConfig);
    if (!StringUtils.isNullOrEmpty(tunnelEndpoint)) {
      tableTunnel.setEndpoint(tunnelEndpoint);
    }
    String partitionName = null;
    if (partition != null && !partition.keys().isEmpty()) {
      partitionName = partition.toString().replace("'", "");
    }
    ArrowStreamReader arrowReader;
    arrowReader = tableTunnel.preview(getProject(), getSchemaName(), model.name, partitionName,
                                      (long) limit);
    ArrowStreamRecordReader recordReader =
        new ArrowStreamRecordReader(new ArrowReaderWrapper(arrowReader), schema,
                                    (columns == null ? null : new HashSet<>(columns)), true, true);
    if (!StringUtils.isNullOrEmpty(timezone)) {
      try {
        recordReader.setTimeZone(ZoneId.of(timezone));
      } catch (Exception e) {
        throw new OdpsException("invalid timezone name: " + timezone, e);
      }
    } else {
      try {
        String defaultTimezone = odps.projects().get(getProject()).getProperty("odps.sql.timezone");
        recordReader.setTimeZone(ZoneId.of(defaultTimezone));
      } catch (Exception ignored) {}
    }
    recordReader.setUseLegacyOutputFormat(useLegacyMode);
    return recordReader;
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

      if (tree.has("lastAccessTime")) {
        long timestamp = tree.get("lastAccessTime").getAsLong() * 1000;
        model.lastAccessTime = timestamp == 0 ? null : new Date(timestamp);
      }

      if (tree.has("isVirtualView")) {
        model.isVirtualView = tree.get("isVirtualView").getAsBoolean();
      }

      if (tree.has("isMaterializedView") && tree.get("isMaterializedView").getAsBoolean()) {
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

      if (tree.has("isObjectTable")) {
        model.isObjectTable = tree.get("isObjectTable").getAsBoolean();
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

      if (tree.has("viewExpandedText")) {
        model.viewExpandedText = tree.get("viewExpandedText").getAsString();
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
                      new TypeToken<Map<String, String>>() {
                      }.getType());
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

      if (tree.has("props") && tree.get("props") != null) {
        JsonObject props = tree.get("props").getAsJsonObject();
        model.mvProperties = new HashMap<>();
        model.mvProperties.put("enable_auto_refresh",
                               props.has("enable_auto_refresh") ? props.get("enable_auto_refresh")
                                   .getAsString() : "false");

        if (props.has("refresh_interval_minutes")) {
          model.mvProperties.put("refresh_interval_minutes",
                                 props.get("refresh_interval_minutes").getAsString());
        }

        if (props.has("refresh_cron")) {
          model.mvProperties.put("refresh_cron", props.get("refresh_cron").getAsString());
        }

        if (props.has("enable_auto_substitute")) {
          model.mvProperties.put("enable_auto_substitute",
                                 props.get("enable_auto_substitute").getAsString());
        }
      }

      if (tree.has("RefreshHistory")) {
        String refreshHistoryStr = tree.get("RefreshHistory").getAsString();
        JsonArray refreshHistoryList = new JsonParser().parse(refreshHistoryStr).getAsJsonArray();
        model.refreshHistory = new LinkedList<>();
        for (int i = 0; i < refreshHistoryList.size(); i++) {

          JsonObject info = refreshHistoryList.get(i).getAsJsonObject();
          Map<String, String> infoMap = new HashMap<>();
          infoMap.put("InstanceId",
                      info.has("InstanceId") ? info.get("InstanceId").getAsString() : null);
          infoMap.put("Status", info.has("Status") ? info.get("Status").getAsString() : null);
          infoMap.put("StartTime",
                      info.has("StartTime") ? info.get("StartTime").getAsString() : null);
          infoMap.put("EndTime", info.has("EndTime") ? info.get("EndTime").getAsString() : null);

          model.refreshHistory.add(infoMap);

          if (model.refreshHistory.size() >= 10) {
            break;
          }
        }
      }

    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    return s;
  }

  private TableSchema loadEpv2TableFromJson(String projectName, String schemaName,
                                            String tableName) throws OdpsException {
    InputStream is = null;
    Map<String, String> queryHint = new HashMap<>();
    try {
      is = Table.class.getResourceAsStream("/com/aliyun/odps/core/base.conf");
      Properties properties = new Properties();
      properties.load(is);
      String majorVersion = properties.getProperty("epv2flighting");
      if (majorVersion != null && !majorVersion.isEmpty() && !"default".equals(majorVersion)) {
        queryHint.put("odps.task.major.version", majorVersion);
      }
    } catch (Exception e) {
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(is);
    }

    String jsonString = "";
    queryHint.put("odps.namespace.schema", "true");
    queryHint.put("odps.sql.allow.namespace.schema", "true");
    queryHint.put("odps.default.schema", schemaName);
    queryHint.put("odps.sql.select.output.format", "json");
    Instance instance =
        SQLTask.run(odps, projectName, "desc extended " + tableName + ";", queryHint, null);
    instance.waitForSuccess();
    Instance.InstanceResultModel.TaskResult taskResult = instance.getRawTaskResults().get(0);
    jsonString = taskResult.result.getString();

    TableSchema tableSchema = new TableSchema();
    JsonObject tree = new JsonParser().parse(jsonString).getAsJsonObject();

    if (tree.has("Owner") && !tree.get("Owner").isJsonNull()) {
      model.owner = tree.get("Owner").getAsString();
    }

    if (tree.has("Project") && !tree.get("Project").isJsonNull()) {
      model.projectName = tree.get("Project").getAsString();
    }

    if (tree.has("Schema") && !tree.get("Schema").isJsonNull()) {
      model.schemaName = tree.get("Schema").getAsString();
    }

    if (tree.has("TableComment") && !tree.get("TableComment").isJsonNull()) {
      model.comment = tree.get("TableComment").getAsString();
    }

    if (tree.has("CreateTime") && !tree.get("CreateTime").isJsonNull()) {
      model.createdTime = new Date(tree.get("CreateTime").getAsLong() * 1000);
    }

    if (tree.has("LastModifiedTime") && !tree.get("LastModifiedTime").isJsonNull()) {
      model.lastModifiedTime = new Date(tree.get("LastModifiedTime").getAsLong() * 1000);
    }

    if (tree.has("ExternalTable") && !tree.get("ExternalTable").isJsonNull()) {
      model.type = TableType.EXTERNAL_TABLE;
      model.isExternalTable = "YES".equalsIgnoreCase(tree.get("ExternalTable").getAsString());
    }

    if (tree.has("Size") && !tree.get("Size").isJsonNull()) {
      model.size = tree.get("Size").getAsLong();
    }

    if (tree.has("NativeColumns") && !tree.get("NativeColumns").isJsonNull()) {
      JsonArray columnsNode = tree.get("NativeColumns").getAsJsonArray();
      for (int i = 0; i < columnsNode.size(); ++i) {
        JsonParser parser = new JsonParser();
        JsonObject n = columnsNode.get(i).getAsJsonObject();
        JsonObject node = parser.parse(n.toString()).getAsJsonObject();
        String name = "";
        String typeString = "";
        String comment = "";
        String nullable = "";
        String defaultValue = "";
        TypeInfo typeInfo = null;

        if (node.has("Name") && !node.get("Name").isJsonNull()) {
          name = node.get("Name").getAsString();
        }
        if (node.has("Type") && !node.get("Type").isJsonNull()) {
          typeString = node.get("Type").getAsString();
          typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeString);

        }
        if (node.has("Comment") && !node.get("Comment").isJsonNull()) {
          comment = node.get("Comment").getAsString();
        }

        if (node.has("Nullable") && !node.get("Nullable").isJsonNull()) {
          nullable = node.get("Nullable").getAsString();
        }

        if (node.has("DefaultValue") && !node.get("DefaultValue").isJsonNull()) {
          defaultValue = node.get("DefaultValue").getAsString();
        }

        Column column = new Column(name, typeInfo, comment);
        column.setNullable(nullable.equals("true"));
        column.setDefaultValue(defaultValue);
        tableSchema.addColumn(column);
        // other attribute set null
      }
    }

    if (tree.has("PartitionColumns") && !tree.get("PartitionColumns").isJsonNull()) {
      JsonArray columnsNode = tree.get("PartitionColumns").getAsJsonArray();
      for (int i = 0; i < columnsNode.size(); ++i) {
        JsonParser parser = new JsonParser();
        JsonObject n = columnsNode.get(i).getAsJsonObject();
        JsonObject node = parser.parse(n.toString()).getAsJsonObject();
        String name = "";
        String typeString = "";
        String comment = "";
        TypeInfo typeInfo = null;
        if (node.has("Name") && !node.get("Name").isJsonNull()) {
          name = node.get("Name").getAsString();
        }
        if (node.has("Type") && !node.get("Type").isJsonNull()) {
          typeString = node.get("Type").getAsString();
          typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeString);

        }
        if (node.has("Comment") && !node.get("Comment").isJsonNull()) {
          comment = node.get("Type").getAsString();
        }
        Column column = new Column(name, typeInfo, comment);
        tableSchema.addPartitionColumn(column);
        // other attribute set null
      }
    }
    return tableSchema;
  }



  private void loadReservedJson(String reserved) {
    JsonObject reservedJson = new JsonParser().parse(reserved).getAsJsonObject();

    // load cluster info
    model.clusterInfo = parseClusterInfo(reservedJson);
    model.isTransactional = parseTransactionalInfo(reservedJson);
    model.hasRowAccessPolicy =
        reservedJson.has("HasRowAccessPolicy") ? reservedJson.get("HasRowAccessPolicy")
            .getAsBoolean() : false;
    if (reservedJson.has("PrimaryKey")) {
      model.primaryKey = new ArrayList<>();
      JsonArray element = reservedJson.get("PrimaryKey").getAsJsonArray();
      for (JsonElement e : element) {
        model.primaryKey.add(e.getAsString());
      }
    }
    model.acidDataRetainHours =
        reservedJson.has("acid.data.retain.hours") ? Integer.parseInt(
            reservedJson.get("acid.data.retain.hours").getAsString()) : -1;

    model.cdcSize = reservedJson.has("cdc_size") ? Long.parseLong(
            reservedJson.get("cdc_size").getAsString()) : -1;

    model.cdcRecordNum = reservedJson.has("cdc_record_num") ? Long.parseLong(
        reservedJson.get("cdc_record_num").getAsString()) : -1;

    model.cdcLatestVersion = reservedJson.has("cdc_latest_version") ? Long.parseLong(
        reservedJson.get("cdc_latest_version").getAsString()) : -1;

    if (reservedJson.has("cdc_latest_timestamp")) {
        long ts = Long.parseLong(reservedJson.get("cdc_latest_timestamp").getAsString()) * 1000;
        model.cdcLatestTimestamp = new Date(ts);
    }

    // load storageTier info
    model.storageTierInfo = StorageTierInfo.getStorageTierInfo(reservedJson);

    // load table lifecycle configuration
    model.tableLifecycleConfig = TableLifecycleConfig.parse(reservedJson);

    model.lastMajorCompactTime =
        reservedJson.has("LastMajorCompactionTime") ? Date.from(Instant.ofEpochMilli(Long.parseLong(
            reservedJson.get("LastMajorCompactionTime").getAsString()) * 1000)) : null;

    model.schemaVersion =
        reservedJson.has("schema_version") ? reservedJson.get("schema_version").getAsString() : null;
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
    clusterInfo.clusterType =
        jsonObject.has("ClusterType") ? ClusterInfo.ClusterType.valueOf(
            jsonObject.get("ClusterType").getAsString().toUpperCase()) : null;
    clusterInfo.bucketNum =
        jsonObject.has("BucketNum") ? jsonObject.get("BucketNum").getAsLong() : 0L;
    JsonArray
        array =
        jsonObject.has("ClusterCols") ? jsonObject.get("ClusterCols").getAsJsonArray() : null;
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
          clusterInfo.sortCols.add(
              new SortColumn(obj.get("col").getAsString(), obj.get("order").getAsString()));
        }
      }
    }

    return clusterInfo;
  }

  /**
   * 增加分区
   *
   * @param spec 分区定义 {@link PartitionSpec}
   * @throws OdpsException
   */
  public void createPartition(PartitionSpec spec) throws OdpsException {
    createPartition(spec, false);
  }

  /**
   * 增加分区
   *
   * @param spec        分区定义 {@link PartitionSpec}
   * @param ifNotExists 在创建分区时，如果为 false 而存在同名分区，则返回出错；若为 true，则无论是否存在同名分区，即使分区结构与要创建的目标分区结构不一致，均返回成功。已存在的同名分区的元信息不会被改动。
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
   * @param spec 分区定义 {@link PartitionSpec}
   * @throws OdpsException
   */
  public void deletePartition(PartitionSpec spec) throws OdpsException {
    deletePartition(spec, false);
  }

  /**
   * 删除指定分区
   *
   * @param spec     分区定义 {@link PartitionSpec}
   * @param ifExists 如果 false 而分区不存在，则返回异常；若为 true，无论分区是否存在，皆返回成功。
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

  @Root(strict = false)
  private static class ListPartitionSpecsResponse {

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;

    @ElementList(entry = "Partition", inline = true, required = false)
    private List<PartitionSpecModel> partitionSpecs = new LinkedList<>();
  }

  /**
   * Get list of partition specs. The returned partition specs are ordered lexicographically.
   *
   * @return list of {@link PartitionSpec}
   */
  public List<PartitionSpec> getPartitionSpecs() throws OdpsException {
    Map<String, String> params = initParamsWithSchema();
    params.put("partitions", null);
    params.put("name", null);

    String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
    List<PartitionSpec> partitionSpecs = new ArrayList<>();

    ListPartitionSpecsResponse resp = client.request(ListPartitionSpecsResponse.class,
                                                     resource,
                                                     "GET",
                                                     params);
    for (PartitionSpecModel partitionSpecModel : resp.partitionSpecs) {
      partitionSpecs.add(new PartitionSpec(partitionSpecModel.partitionSpec, false));
    }

    return partitionSpecs;
  }


  /**
   * 在Table上创建Shards
   *
   * @param shardCount 创建Shard的个数
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
   * @param spec 指定的上级分区 {@link PartitionSpec}
   * @return {@link Partition}迭代器
   */
  public Iterator<Partition> getPartitionIterator(final PartitionSpec spec) {
    return getPartitionIterator(spec, false, 1000L, Long.MAX_VALUE);
  }

  /**
   * Get a partition iterator.
   *
   * @param spec      Specify the values of some of the partition columns. The specified columns'
   *                  indices should be continuous and start from 0.
   * @param reverse   Reverse the result. The original
   * @param batchSize Max number of partitions to get per request. In case of null, the batch size
   *                  will be decided by the server.
   * @param limit     Limit the number of returned partitions. In case of null, {@link Long#MAX_VALUE}
   *                  will be used.
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
      public String getMarker() {
        return params.get("marker");
      }

      @Override
      public List<Partition> list(String marker, long maxItems) {
        if (marker != null) {
          params.put("marker", marker);
        }
        if (maxItems >= 0) {
          params.put("maxitems", String.valueOf(maxItems));
        }
        return list();
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
        if (params.get("maxitems") == null && batchSize != null) {
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
   * @param spec 分区定义 {@link PartitionSpec}
   * @return 分区信息 {@link Partition}
   */
  public Partition getPartition(PartitionSpec spec) {
    return new Partition(spec, model.projectName, model.schemaName, model.name, odps);
  }

  /**
   * 判断指定分区是否存在
   *
   * @param spec 分区定义 {@link PartitionSpec}
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
   */
  public boolean isPartitioned() {
    if (isVirtualView()) {
      return false;
    }
    return !getSchema().getPartitionColumns().isEmpty();
  }

  private void runSQL(String query) throws OdpsException {
    runSQL("AnonymousSQLTask", query);
  }
  private void runSQL(String taskName, String query) throws OdpsException {
    Map<String, String> hints = NameSpaceSchemaUtils.setSchemaFlagInHints(null, model.schemaName);
    Instance i = SQLTask.run(odps, odps.getDefaultProject(), query, taskName, hints, null);
    i.waitForSuccess();
  }

  private HashMap<String, String> initParamsWithSchema() throws OdpsException {
    return NameSpaceSchemaUtils.initParamsWithSchema(model.schemaName);
  }

  private String getCoordinate() {
    return NameSpaceSchemaUtils.getFullName(model.projectName, model.schemaName, model.name);
  }

  private Map<String, String> getMvProperties() {
    lazyLoad();
    if (model.mvProperties == null) {
      model.mvProperties = new HashMap<>();
    }
    return model.mvProperties;
  }

  public boolean isAutoRefreshEnabled() {
    return Boolean.parseBoolean(getMvProperties().getOrDefault("enable_auto_refresh", "false"));
  }

  public Boolean isAutoSubstituteEnabled() {
    String
        autoSubstituteEnabledStr =
        getMvProperties().getOrDefault("enable_auto_substitute", null);
    return autoSubstituteEnabledStr == null ? null : Boolean.valueOf(autoSubstituteEnabledStr);
  }

  public Integer getRefreshInterval() {
    String refreshIntervalStr = getMvProperties().getOrDefault("refresh_interval_minutes", null);
    return refreshIntervalStr == null ? null : Integer.valueOf(refreshIntervalStr);
  }

  public String getRefreshCron() {
    return getMvProperties().getOrDefault("refresh_cron", null);
  }

  public List<Map<String, String>> getRefreshHistory() {
    lazyLoadExtendInfo();
    return model.refreshHistory;
  }

  public boolean hasRowAccessPolicy() {
    lazyLoadExtendInfo();
    return model.hasRowAccessPolicy;
  }

  public List<String> getPrimaryKey() {
    lazyLoadExtendInfo();
    return model.primaryKey;
  }

  public int getAcidDataRetainHours() {
    lazyLoadExtendInfo();
    return model.acidDataRetainHours;
  }

  public long getCdcSize() {
    lazyLoadExtendInfo();
    return model.cdcSize;
  }

  public long getCdcRecordNum() {
    lazyLoadExtendInfo();
    return model.cdcRecordNum;
  }

  public long getCdcLatestVersion() {
    lazyLoadExtendInfo();
    return model.cdcLatestVersion;
  }

  public Date getCdcLatestTimestamp() {
    lazyLoadExtendInfo();
    return model.cdcLatestTimestamp;
  }

  public Stream newStream(String streamName) throws OdpsException {
    if (!isTransactional()) {
      throw new IllegalArgumentException("only transactional table can attach stream");
    }
    StreamIdentifier
        identifier =
        StreamIdentifier.of(model.projectName, streamName);
    odps.streams().create(identifier,
                          TableIdentifier.of(model.projectName, model.schemaName, model.name));
    return odps.streams().get(identifier);
  }

  // update table methods

  /**
   * Modify the life cycle of an existing partitioned table or non-partitioned table.
   */
  public void setLifeCycle(int days) throws OdpsException {
    String sql = String.format("ALTER TABLE %s SET LIFECYCLE %d;", getCoordinate(), days);
    runSQL(sql);
  }

  /**
   * Only the Project Owner or users with the Super_Administrator role can execute commands that modify the table Owner.
   */
  public void changeOwner(String newOwner) throws OdpsException {
    String target = "table";
    if (isVirtualView() || isMaterializedView()) {
      target = "view";
    }
    runSQL(String.format("ALTER %s %s CHANGEOWNER TO %s;", target, getCoordinate(), CommonUtils.quoteStr(newOwner)));
  }

  /**
   *  ChangeComment Modify the comment content of the table.
   */
  public void changeComment(String newComment) throws OdpsException {
    runSQL(String.format("ALTER TABLE %s SET COMMENT %s;", getCoordinate(), CommonUtils.quoteStr(newComment)));
  }

  /**
   * Touch can modify the LastModifiedTime of the table, making LastModifiedTime change to the current time
   */
  public void touch() throws OdpsException {
    runSQL(String.format("ALTER TABLE %s TOUCH;", getCoordinate()));
  }

  /**
   * ChangeClusterInfo Modify the cluster information of the table.
   */
  public void changeClusterInfo(ClusterInfo clusterInfo) throws OdpsException {
    runSQL(String.format("ALTER TABLE %s %s;", getCoordinate(), clusterInfo.toString()));
  }

  /**
   * Rename the table.
   */
  public void rename(String newName) throws Exception {
    String target = "table";
    if (isVirtualView()) {
      target = "view";
    }
    runSQL(String.format("ALTER %s %s RENAME TO %s;", target, getCoordinate(), CommonUtils.quoteRef(newName)));
    model.name = newName;
  }

  /**
   * Add new columns to the table.
   */
  public void addColumns(List<Column> columns, boolean ifNotExists) throws Exception {
    runSQL(generateAddColumnsSQL(columns, ifNotExists));
  }

  private String generateAddColumnsSQL(List<Column> columns, boolean ifNotExists) {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ").append(getCoordinate()).append(" ADD COLUMNS ");
    if (ifNotExists) {
      sb.append("IF NOT EXISTS ");
    }
    sb.append("(");
    StringJoiner joiner = new StringJoiner(", ");
    for (Column column : columns) {
      StringBuilder columnDef = new StringBuilder();
      columnDef.append(String.format("%s %s", CommonUtils.quoteRef(column.getName()), column.getTypeInfo().getTypeName()));
      if (column.getComment() != null && !column.getComment().isEmpty()) {
        columnDef.append(String.format(" COMMENT %s", CommonUtils.quoteStr(column.getComment())));
      }
      joiner.add(columnDef.toString());
    }
    sb.append(joiner).append(");");
    return sb.toString();
  }

  /**
   * Drop columns from the table.
   */
  public void dropColumns(List<String> columnNames) throws Exception {
    runSQL(generateDropColumnsSQL(columnNames));
  }

  private String generateDropColumnsSQL(List<String> columnNames) {
    StringJoiner joiner = new StringJoiner(", ");
    for (String columnName : columnNames) {
      joiner.add(CommonUtils.quoteRef(columnName));
    }
    return String.format("ALTER TABLE %s DROP COLUMNS %s;", getCoordinate(), joiner);
  }

  /**
   * Change the type of an existing column in the table.
   */
  public void alterColumnType(String columnName, TypeInfo columnType) throws Exception {
    runSQL(String.format("ALTER TABLE %s CHANGE COLUMN %s %s %s;", getCoordinate(), columnName, columnName, columnType.getTypeName()));
  }

  /**
   * Change the name of an existing column in the table.
   */
  public void changeColumnName(String oldColumnName, String newColumnName) throws Exception {
    runSQL(String.format("ALTER TABLE %s CHANGE COLUMN %s RENAME TO %s;", getCoordinate(), oldColumnName, newColumnName));
  }
}
