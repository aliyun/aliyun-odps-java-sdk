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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Partition.PartitionModel;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.data.DefaultRecordReader;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;

/**
 * Table表示ODPS中的表
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class Table extends LazyLoad {

  @XmlRootElement(name = "Table")
  static class TableModel {

    @XmlRootElement(name = "Schema")
    static class Schema {

      @XmlValue
      String content;
    }

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "TableId")
    String ID;

    @XmlAttribute(name = "format")
    private String format;

    @XmlElement(name = "Schema")
    private Schema schema;

    @XmlElement(name = "Comment")
    String comment;

    @XmlElement(name = "Owner")
    String owner;

    @XmlElement(name = "Project")
    String projectName;

    @XmlElement(name = "TableLabel")
    String tableLabel;

    @XmlElement(name = "CryptoAlgo")
    String cryptoAlgoName;

    @XmlElement(name = "CreationTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date createdTime;

    @XmlElement(name = "LastModifiedTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date lastModifiedTime;

    Date lastMetaModifiedTime;
    boolean isVirtualView;
    boolean isExternalTable;
    long life = -1L;
    long hubLifecycle = -1L;
    String viewText;
    long size;

    boolean isArchived;
    long physicalSize;
    long fileNum;
    Shard shard;

    // for external table extended info
    String storageHandler;
    String location;
    String resources;
    Map<String, String> serDeProperties;
  }

  private TableModel model;
  private TableSchema tableSchema;
  private RestClient client;
  private boolean isExtendInfoLoaded;
  private boolean isShardInfoLoaded;
  private Odps odps;

  Table(TableModel model, String project, Odps odps) {
    this.model = model;
    this.model.projectName = project;
    this.odps = odps;
    this.client = odps.getRestClient();
    this.isExtendInfoLoaded = false;
    this.isShardInfoLoaded = false;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
    reload(client.request(TableModel.class, resource, "GET"));
  }

  public void reload(TableModel model) throws OdpsException {
    this.model = model;
    if (model.schema != null) {
      tableSchema = loadSchemaFromJson(model.schema.content);
    }
    setLoaded(true);
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
   * 判断表是否为虚拟视图
   *
   * @return 如果是虚拟视图返回true, 否则返回false
   */
  public boolean isVirtualView() {
    lazyLoad();
    return model.isVirtualView;
  }

  /**
   * 判断表是否为外部表
   *
   * @return 如果是外部表返回true, 否则返回false
   */
  public boolean isExternalTable() {
    lazyLoad();
    return model.isExternalTable;
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
    return model.schema.content;
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
   * @return {@link RecordReader}对象
   * @throws OdpsException
   */
  public RecordReader read(PartitionSpec partition, List<String> columns, int limit)
      throws OdpsException {
    if (limit < 0) {
      throw new OdpsException("limit number should >= 0.");
    }
    Map<String, String> params = new HashMap<String, String>();
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

    String resource = ResourceBuilder.buildTableResource(model.projectName, getName());
    Response resp = client.request(resource, "GET", params, null, null);
    return new DefaultRecordReader(new ByteArrayInputStream(resp.getBody()), getSchema());
  }

  private TableSchema loadSchemaFromJson(String json) {
    TableSchema s = new TableSchema();
    try {
      JSONObject tree = JSON.parseObject(json);
      String node = tree.getString("comment");

      if (node != null) {
        model.comment = node;
      }

      node = tree.getString("owner");
      if (node != null) {
        model.owner = node;
      }

      Long node2 = tree.getLong("createTime");
      if (node2 != null) {
        model.createdTime = new Date(node2 * 1000);
      }

      node2 = tree.getLong("lastModifiedTime");
      if (node2 != null) {
        model.lastModifiedTime = new Date(node2 * 1000);
      }

      node2 = tree.getLong("lastDDLTime");
      if (node2 != null) {
        model.lastMetaModifiedTime = new Date(node2 * 1000);
      }

      Boolean node3 = tree.getBoolean("isVirtualView");
      if (node3 != null) {
        model.isVirtualView = node3;
      }

      node3 = tree.getBoolean("isExternal");
      if (node3 != null) {
        model.isExternalTable = node3;
      }

      node2 = tree.getLong("lifecycle");
      if (node2 != null) {
        model.life = node2;
      }

      node2 = tree.getLong("hubLifecycle");
      if (node2 != null) {
        model.hubLifecycle = node2;
      }

      node = tree.getString("viewText");
      if (node != null) {
        model.viewText = node;
      }

      node2 = tree.getLong("size");
      if (node2 != null) {
        model.size = node2;
      }

      node3 = tree.getBoolean("IsArchived");
      if (node3 != null) {
        model.isArchived = node3;
      }

      node2 = tree.getLong("PhysicalSize");
      if (node2 != null) {
        model.physicalSize = node2;
      }

      node2 = tree.getLong("FileNum");
      if (node2 != null) {
        model.fileNum = node2;
      }

      node = tree.getString("storageHandler");
      if (node != null) {
        model.storageHandler = node;
      }

      node = tree.getString("location");
      if (node != null) {
        model.location = node;
      }

      node = tree.getString("resources");
      if (node != null) {
        model.resources = node;
      }

      node = tree.getString("serDeProperties");
      if (node != null) {
        model.serDeProperties = (Map)JSON.parse(node);
      }

      node3 = tree.getBoolean("shardExist");
      if (node3 != null) {
        boolean shardExist = node3;

        JSONObject node4 = tree.getJSONObject("shardInfo");
        if (shardExist && node4 != null) {
          model.shard = Shard.parseShard(node4);
        } else {
          model.shard = null;
        }
      }

      node = tree.getString("tableLabel");
      if (node != null) {
        model.tableLabel = node;
        // Service will return 0 if nothing set
        if (model.tableLabel.equals("0")) {
          model.tableLabel = "";
        }
      }

      JSONArray columnsNode = tree.getJSONArray("columns");
      if (columnsNode != null) {
        for (int i = 0; i < columnsNode.size(); ++i) {
          JSONObject n = columnsNode.getJSONObject(i);
          s.addColumn(parseColumn(n));
        }
      }

      columnsNode = tree.getJSONArray("partitionKeys");
      if (columnsNode != null) {
          for (int i = 0; i < columnsNode.size(); ++i) {
             JSONObject n = columnsNode.getJSONObject(i);
             s.addPartitionColumn(parseColumn(n));
        }
      }

    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    return s;
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
    sb.append("ALTER TABLE ").append(getProject()).append(".").append(getName());

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
    sb.append("ALTER TABLE ").append(getProject()).append(".").append(getName());
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
  @XmlRootElement(name = "Partitions")
  private static class ListPartitionsResponse {

    @XmlElement(name = "Partition")
    private List<PartitionModel> partitions = new LinkedList<PartitionModel>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
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
    sb.append("ALTER TABLE ").append(getProject()).append(".").append(getName());
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
    return new ListIterator<Partition>() {

      Map<String, String> params = new HashMap<String, String>();

      @Override
      protected List<Partition> list() {
        ArrayList<Partition> partitions = new ArrayList<Partition>();

        params.put("partitions", null);
        params.put("expectmarker", "true"); // since sprint-11
        if (spec != null && !spec.isEmpty()) {
          params.put("partition", spec.toString());
        }

        String lastMarker = params.get("marker");
        if (params.containsKey("marker") && lastMarker.length() == 0) {
          return null;
        }

        String resource = ResourceBuilder.buildTableResource(model.projectName, getName());
        try {

          ListPartitionsResponse
              resp =
              client.request(ListPartitionsResponse.class, resource, "GET", params);

          for (PartitionModel partitionModel : resp.partitions) {
            Partition t = new Partition(partitionModel, model.projectName, getName(), client);
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
    ArrayList<Partition> parts = new ArrayList<Partition>();
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
    return new Partition(spec, model.projectName, getName(), client);
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
    sb.append("TRUNCATE TABLE ").append(getProject()).append(".").append(getName()).append(";");
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
    SQLTask task = new SQLTask();
    task.setName(taskName);
    task.setQuery(query);
    Instances instances = odps.instances();
    Instance instance = instances.create(task);

    instance.waitForSuccess();
  }

  /* private */
  private Column parseColumn(JSONObject node) {
    String name = node.getString("name");
    String typeString = node.getString("type").toUpperCase();
    List<OdpsType> genericTypeList = null;
    if (typeString.contains("<")) {
      String[] result = typeString.split("<", 2);
      typeString = result[0];
      String genericTypeListString = result[1];

      genericTypeListString = genericTypeListString.replace(">", "");
      genericTypeList = new ArrayList<OdpsType>();
      for (String st : genericTypeListString.split(",")) {
        genericTypeList.add(OdpsType.valueOf(st));
      }
    }
    OdpsType type = OdpsType.valueOf(typeString);
    String comment = node.getString("comment");
    String label = null;
    if (node.containsKey("label") && (!node.getString("label").isEmpty())) {
      label = node.getString("label");
    }
    return new Column(name, type, comment, label, genericTypeList);
  }

  private void lazyLoadExtendInfo() {
    if (!this.isExtendInfoLoaded) {
      Map<String, String> params = new LinkedHashMap<String, String>();
      params.put("extended", null);

      String resource = ResourceBuilder.buildTableResource(model.projectName, model.name);
      TableModel response;
      try {
        response = client.request(TableModel.class, resource, "GET", params);
      } catch (OdpsException e) {
        throw new ReloadException(e.getMessage(), e);
      }

      loadSchemaFromJson(response.schema.content);
      this.isExtendInfoLoaded = true;
    }
  }
}
