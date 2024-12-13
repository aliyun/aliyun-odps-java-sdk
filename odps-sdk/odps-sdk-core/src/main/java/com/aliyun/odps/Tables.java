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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.odps.Table.TableModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.data.GenerateExpression;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.CommonUtils;
import com.aliyun.odps.utils.ExceptionUtils;
import com.aliyun.odps.utils.NameSpaceSchemaUtils;
import com.aliyun.odps.utils.OdpsCommonUtils;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.collect.ImmutableList;

/**
 * Tables表示ODPS中所有{@link Table}的集合
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class Tables implements Iterable<Table> {

  @Root(name = "Tables", strict = false)
  private static class ListTablesResponse {

    @ElementList(entry = "Table", inline = true, required = false)
    private List<TableModel> tables = new ArrayList<TableModel>();

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;
  }

  @Root(name = "Tables", strict = false)
  private static class QueryTables {

    @Root(name = "Table", strict = false)
    private static class QueryTable {

      @Element(name = "Project", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      private String projectName;

      @Element(name = "Name", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      private String tableName;

      @Element(name = "Schema", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      private String schemaName;

      QueryTable() {
      }

      QueryTable(String projectName, String schemaName, String tableName) {
        this.projectName = projectName;
        this.schemaName = schemaName;
        this.tableName = tableName;
      }
    }

    @ElementList(entry = "Table", inline = true, required = false)
    private List<QueryTable> tables = new ArrayList<QueryTable>();
  }

  private RestClient client;
  private Odps odps;

  Tables(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * 获得指定表信息
   *
   * @param tableName
   *     表名
   * @return 指定表的信息 {@link Table}
   */
  public Table get(String tableName) {
    return get(getDefaultProjectName(), tableName);
  }

  /**
   * 获得指定表信息
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param tableName
   *     表名
   * @return 指定表的信息 {@link Table}
   */
  public Table get(String projectName, String tableName) {
    return get(projectName, odps.getCurrentSchema(), tableName);
  }

  /**
   * Get designated table.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param tableName Table name.
   * @return {@link Table}
   */
  public Table get(String projectName, String schemaName, String tableName) {
    TableModel model = new TableModel();
    model.name = tableName;
    return new Table(model, projectName, schemaName, odps);
  }

  /**
   * 判断指定表是否存在
   *
   * @param tableName
   *     表名
   * @return 存在返回true, 否则返回false
   * @throws OdpsException
   */
  public boolean exists(String tableName) throws OdpsException {
    return exists(getDefaultProjectName(), tableName);
  }

  /**
   * 判断指定表是否存在
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param tableName
   *     表名
   * @return 存在返回true, 否则返回flase
   * @throws OdpsException
   */
  public boolean exists(String projectName, String tableName) throws OdpsException {
    return exists(projectName, odps.getCurrentSchema(), tableName);
  }

  /**
   * Check if designated table exists.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param tableName Table name.
   * @return True if the table exists, else false.
   * @throws OdpsException
   */
  public boolean exists(
      String projectName,
      String schemaName,
      String tableName) throws OdpsException {
    if (StringUtils.isNullOrEmpty(tableName)) {
      return false;
    }
    try {
      Table t = get(projectName, schemaName, tableName);
      t.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  /**
   * 获取默认{@link Project}的所有表信息迭代器
   *
   * @return {@link Table}迭代器
   */
  @Override
  public Iterator<Table> iterator() {
    return iterator(getDefaultProjectName(), null);
  }

  /**
   * 获取表信息迭代器
   *
   * @param projectName
   *     指定{@link Project}名称
   * @return {@link Table}迭代器
   */
  public Iterator<Table> iterator(final String projectName) {
    return iterator(projectName, null);
  }

  /**
   * 获取默认Project的表信息迭代器
   *
   * @param filter
   *     过滤条件
   * @return {@link Table}迭代器
   */
  public Iterator<Table> iterator(final TableFilter filter) {
    return iterator(getDefaultProjectName(), filter);
  }

  /**
   * 获得表信息迭代器
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param filter
   *     过滤条件
   * @return {@link Table}迭代器
   */
  public Iterator<Table> iterator(final String projectName, final TableFilter filter) {
    return iterator(projectName, filter, false);
  }

  /**
   * 获得表信息迭代器
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param filter
   *     过滤条件
   * @param extended
   *     是否同时获取额外信息
   * @return {@link Table}迭代器
   */
  public Iterator<Table> iterator(
      final String projectName,
      final TableFilter filter,
      boolean extended) {
    return new TableListIterator(projectName, odps.getCurrentSchema(), filter, extended);
  }

  /**
   * Get a table iterator of the given schema in the given project.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param filter Table filter, see {@link TableFilter}.
   * @param extended Get extended fields or not. Extended fields are "type" and "comment".
   * @return A table iterator.
   */
  public Iterator<Table> iterator(
      final String projectName,
      final String schemaName,
      final TableFilter filter,
      boolean extended) {
    return new TableListIterator(projectName, schemaName, filter, extended);
  }

  /**
   * 获取默认{@link Project}的所有表信息迭代器 iterable
   *
   * @return {@link Table}迭代器
   */
  public Iterable<Table> iterable() {
    return iterable(getDefaultProjectName());
  }

  /**
   * 获取表信息迭代器 iterable
   *
   * @param projectName
   *     指定{@link Project}名称
   * @return {@link Table}迭代器
   */
  public Iterable<Table> iterable(final String projectName) {
    return iterable(projectName, null);
  }

  /**
   * 获取默认Project的表信息迭代器 iterable
   *
   * @param filter
   *     过滤条件
   * @return {@link Table}迭代器
   */
  public Iterable<Table> iterable(final TableFilter filter) {
    return iterable(getDefaultProjectName(), filter);
  }

  /**
   * 获得表信息迭代器 iterable
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param filter
   *     过滤条件
   * @return {@link Table}迭代器
   */
  public Iterable<Table> iterable(final String projectName, final TableFilter filter) {
    return iterable(projectName, filter, false);
  }

  /**
   * 获得表信息迭代器 iterable
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param filter
   *     过滤条件
   * @param extended
   *     是否同时获取额外信息
   * @return {@link Table}迭代器
   */
  public Iterable<Table> iterable(final String projectName,
                                  final TableFilter filter,
                                  boolean extended) {
    return iterable(projectName, odps.getCurrentSchema(), filter, extended);
  }

  /**
   * Get a table iterable of the given schema in the given project.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param filter Table filter, see {@link TableFilter}.
   * @param extended Get extended fields or not. Extended fields are "type" and "comment".
   * @return A table iterable.
   */
  public Iterable<Table> iterable(
      final String projectName,
      final String schemaName,
      final TableFilter filter,
      boolean extended) {
    return () -> new TableListIterator(projectName, schemaName, filter, extended);
  }

  private class TableListIterator extends ListIterator<Table> {

    Map<String, String> params = new HashMap<>();

    private TableFilter filter;
    private String projectName;
    private String schemaName;
    private boolean extended;

    TableListIterator(
        String projectName,
        String schemaName,
        TableFilter filter,
        boolean extended) {
      this.filter = filter;
      this.projectName = projectName;
      this.schemaName = schemaName;
      this.extended = extended;

      params = NameSpaceSchemaUtils.initParamsWithSchema(schemaName);
    }

    @Override
    public List<Table> list(String marker, long maxItems) {
      if (marker != null) {
        params.put("marker", marker);
      }
      if (maxItems >= 0) {
        params.put("maxitems", String.valueOf(maxItems));
      }
      return list();
    }

    @Override
    public String getMarker() {
      return params.get("marker");
    }

    @Override
    protected List<Table> list() {
      ArrayList<Table> tables = new ArrayList<Table>();
      params.put("expectmarker", "true"); // since sprint-11

      String lastMarker = params.get("marker");
      if (params.containsKey("marker") && lastMarker.length() == 0) {
        return null;
      }

      if (filter != null) {
        if (filter.getName() != null) {
          params.put("name", filter.getName());
        }

        if (filter.getOwner() != null) {
          params.put("owner", filter.getOwner());
        }

        if (filter.getType() != null) {
          params.put("type", filter.getType().toString());
        }
      }

      if (extended) {
        params.put("extended", null);
      }

      String resource = ResourceBuilder.buildTablesResource(projectName);
      try {

        ListTablesResponse resp = client.request(ListTablesResponse.class, resource, "GET",
                                                 params);

        for (TableModel model : resp.tables) {
          Table t = new Table(model, projectName, schemaName, odps);
          tables.add(t);
        }

        params.put("marker", resp.marker);
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      return tables;
    }
  }

  /**
   * 创建表
   *
   * @param tableName
   *     表名
   * @param schema
   *     表结构 {@link TableSchema}
   * @throws OdpsException
   */
  public void create(String tableName, TableSchema schema) throws OdpsException {
    create(client.getDefaultProject(), tableName, schema);
  }

  /**
   * 创建表
   *
   * @param tableName
   *     表名
   * @param schema
   *     表结构 {@link TableSchema}
   * @param ifNotExists
   *     在创建表时，如果为 false 而存在同名表，则返回出错；若为 true，则无论是否存在同名表，即使原表结构与要创建的目标表结构不一致，均返回成功。已存在的同名表的元信息不会被改动。
   * @throws OdpsException
   */
  public void create(String tableName, TableSchema schema, boolean ifNotExists)
      throws OdpsException {
    create(client.getDefaultProject(), tableName, schema, ifNotExists);
  }

  /**
   * 创建表
   *
   * @param projectName
   *     目标表所在{@link Project}名称
   * @param tableName
   *     表名
   * @param schema
   *     表结构 {@link TableSchema}
   * @param ifNotExists
   *     在创建表时，如果为 false 而存在同名表，则返回出错；若为 true，则无论是否存在同名表，即使原表结构与要创建的目标表结构不一致，均返回成功。已存在的同名表的元信息不会被改动。
   * @param shardNum
   *     表中shard数量，小于0表示未设置
   * @param hubLifecycle
   *     Hub表生命周期，小于0表示未设置
   * @throws OdpsException
   */
  public void create(String projectName, String tableName, TableSchema schema, boolean ifNotExists,
                     Long shardNum, Long hubLifecycle)
      throws OdpsException {
    create(projectName, tableName, schema, null, ifNotExists, shardNum, hubLifecycle);
  }

  /**
   * 创建表
   *
   * @param projectName
   *     目标表所在{@link Project}名称
   * @param tableName
   *     所要创建的{@link Table}名称
   * @param schema
   *     表结构 {@link TableSchema}
   */
  public void create(String projectName, String tableName, TableSchema schema)
      throws OdpsException {
    create(projectName, tableName, schema, false);
  }

  /**
   * 创建表
   *
   * @param projectName
   *     目标表所在{@link Project}名称
   * @param tableName
   *     所要创建的{@link Table}名称
   * @param schema
   *     表结构 {@link TableSchema}
   * @param ifNotExists
   *     在创建表时，如果为 false 而存在同名表，则返回出错；若为 true，则无论是否存在同名表，即使原表结构与要创建的目标表结构不一致，均返回成功。已存在的同名表的元信息不会被改动。
   */
  public void create(String projectName, String tableName, TableSchema schema, boolean ifNotExists)
      throws OdpsException {
    create(projectName, tableName, schema, null, ifNotExists);
  }

  /**
   * 创建表
   *
   * @param projectName
   *     目标表所在{@link Project}名称
   * @param tableName
   *     所要创建的{@link Table}名称
   * @param schema
   *     表结构 {@link TableSchema}
   * @param comment
   *     表注释, 其中不能带有单引号
   * @param ifNotExists
   *     在创建表时，如果为 false 而存在同名表，则返回出错；若为 true，则无论是否存在同名表，即使原表结构与要创建的目标表结构不一致，均返回成功。已存在的同名表的元信息不会被改动。
   * @param shardNum
   *     表中shard数量，小于0表示未设置
   * @param hubLifecycle
   *     Hub表生命周期，小于0表示未设置
   */
  public void create(String projectName, String tableName, TableSchema schema,
                     String comment, boolean ifNotExists, Long shardNum, Long hubLifecycle)
      throws OdpsException {
    DataHubInfo dataHubInfo = new DataHubInfo(shardNum, hubLifecycle);

    TableCreator tableCreator = newTableCreator(projectName, tableName, schema).withSchemaName(odps.getCurrentSchema())
        .withComment(comment).withDataHubInfo(dataHubInfo);
    if (ifNotExists) {
      tableCreator = tableCreator.ifNotExists();
    }
    tableCreator.create();
  }

  /**
   * 创建表
   *
   * @param projectName
   *     目标表所在{@link Project}名称
   * @param tableName
   *     所要创建的{@link Table}名称
   * @param schema
   *     表结构 {@link TableSchema}
   * @param comment
   *     表注释, 其中不能带有单引号
   * @param ifNotExists
   */
  public void create(String projectName, String tableName, TableSchema schema, String comment,
                     boolean ifNotExists)
      throws OdpsException {
    createTableWithLifeCycle(projectName, tableName, schema, comment, ifNotExists, null);
  }


  /**
   * 创建表
   *
   * @param projectName
   *     目标表所在{@link Project}名称
   * @param tableName
   *     所要创建的{@link Table}名称
   * @param schema
   *     表结构 {@link TableSchema}
   * @param comment
   *     表注释, 其中不能带有单引号
   * @param ifNotExists
   * @param lifeCycle
   *     表生命周期
   */
  public void createTableWithLifeCycle(String projectName, String tableName, TableSchema schema,
                                       String comment, boolean ifNotExists, Long lifeCycle)
      throws OdpsException {
    create(projectName, tableName, schema, comment, ifNotExists, lifeCycle, null, null);
  }

  /**
   * 创建表
   *
   * @param projectName
   *     目标表所在{@link Project}名称
   * @param tableName
   *     所要创建的{@link Table}名称
   * @param schema
   *     表结构 {@link TableSchema}
   * @param comment
   *     表注释, 其中不能带有单引号
   * @param ifNotExists
   * @param lifeCycle
   *     表生命周期
   * @param hints
   *     能够影响SQL执行的Set信息，例如：odps.mapred.map.split.size等
   * @param aliases
   *     Alias信息。详情请参考用户手册中alias命令的相关介绍
   */
  public void create(
      String projectName,
      String tableName,
      TableSchema schema,
      String comment,
      boolean ifNotExists,
      Long lifeCycle,
      Map<String, String> hints,
      Map<String, String> aliases) throws OdpsException {
    create(
        projectName,
        odps.getCurrentSchema(),
        tableName,
        schema,
        comment,
        ifNotExists,
        lifeCycle,
        hints,
        aliases);
  }

  /**
   * Create a table.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param tableName Table name
   * @param schema Table schema.
   * @param comment Comment, should not contain single quotes.
   * @param ifNotExists Create only if the table doesn't exist.
   * @param lifeCycle Lifecycle.
   * @param hints Hints.
   * @param aliases Aliases.
   */
  public void create(
      String projectName,
      String schemaName,
      String tableName,
      TableSchema schema,
      String comment,
      boolean ifNotExists,
      Long lifeCycle,
      Map<String, String> hints,
      Map<String, String> aliases) throws OdpsException {
    TableCreator tableCreator = newTableCreator(projectName, tableName, schema)
        .withSchemaName(schemaName).withComment(comment).withLifeCycle(lifeCycle)
        .withHints(hints).withAliases(aliases);
    if (ifNotExists) {
      tableCreator = tableCreator.ifNotExists();
    }
    tableCreator.create();
  }

  /**
   * 创建外部表
   *
   * @param projectName
   *     目标表所在{@link Project}名称
   * @param tableName
   *     所要创建的{@link Table}名称
   * @param schema
   *     表结构 {@link TableSchema}
   * @param location
   *     外部数据存储地址URI String，比如"oss://path/to/directory/", 具体格式参考外部表使用手册
   * @param storedBy
   *     处理外部数据使用的StorageHandler名字, 比如"com.aliyun.odps.TsvStorageHandler"
   * @param usingJars (nullable)
   *     如果是自定义的StorageHandler, 这里指定其所依赖的jar名字。这些jar必须事先用ADD JAR命令添加
   * @param serdeProperties (nullable)
   *     对StorageHandler的参数指定(if any)，每个参数为一个String/String的key value
   * @param comment (nullable)
   *     表注释, 其中不能带有单引号
   * @param ifNotExists
   * @param lifeCycle (nullable)
   *     表生命周期
   * @param hints (nullable)
   *     能够影响SQL执行的Set信息，例如：odps.mapred.map.split.size等
   * @param aliases (nullable)
   *     Alias信息。详情请参考用户手册中alias命令的相关介绍
   * @throws OdpsException
   */
  public void createExternal(
      String projectName,
      String tableName,
      TableSchema schema,
      String location,
      String storedBy,
      List<String> usingJars,
      Map<String, String> serdeProperties,
      String comment,
      boolean ifNotExists,
      Long lifeCycle,
      Map<String, String> hints,
      Map<String, String> aliases) throws OdpsException {
    createExternal(
        projectName,
        odps.getCurrentSchema(),
        tableName,
        schema,
        location,
        storedBy,
        usingJars,
        serdeProperties,
        comment,
        ifNotExists,
        lifeCycle,
        hints,
        aliases);
  }

  /**
   * Create an external table.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param tableName Table name
   * @param schema Table schema.
   * @param location Location, like "oss://path/to/directory/".
   * @param storedBy Name of the storage handler to use, like "com.aliyun.odps.TsvStorageHandler".
   * @param usingJars Names of jar needed by the storage handler, could be null.
   * @param serdeProperties Arguments of the storage handler, could be null.
   * @param comment Comment, should not contain single quotes.
   * @param ifNotExists Create only if the table doesn't exist.
   * @param lifeCycle Lifecycle.
   * @param hints Hints.
   * @param aliases Aliases.
   * @throws OdpsException
   */
  public void createExternal(
      String projectName,
      String schemaName,
      String tableName,
      TableSchema schema,
      String location,
      String storedBy,
      List<String> usingJars,
      Map<String, String> serdeProperties,
      String comment,
      boolean ifNotExists,
      Long lifeCycle,
      Map<String, String> hints,
      Map<String, String> aliases) throws OdpsException {
    TableCreator
        tableCreator =
        newTableCreator(projectName, tableName, schema).externalTable().withSchemaName(schemaName)
            .withStorageHandler(storedBy).withResources(usingJars).withLocation(location)
            .withSerdeProperties(serdeProperties).withComment(comment)
            .withLifeCycle(lifeCycle).withHints(hints).withAliases(aliases);
    if (ifNotExists) {
      tableCreator = tableCreator.ifNotExists();
    }
    tableCreator.create();
  }


  /**
   * 删除表
   *
   * @param tableName
   *     表名
   * @throws OdpsException
   */
  public void delete(String tableName) throws OdpsException {
    delete(client.getDefaultProject(), tableName);
  }

  /**
   * 删除表
   *
   * @param tableName
   *     表名
   * @param ifExists
   *     如果为 false 表不存在，则返回异常；若为 true，无论表是否存在，皆返回成功。
   * @throws OdpsException
   */
  public void delete(String tableName, boolean ifExists) throws OdpsException {
    delete(client.getDefaultProject(), tableName, ifExists);
  }

  /**
   * 删除表
   *
   * @param projectName
   *     表所在{@link Project}
   * @param tableName
   *     表名
   * @throws OdpsException
   */
  public void delete(String projectName, String tableName) throws OdpsException {
    delete(projectName, tableName, false);
  }


  /**
   * 删除表
   *
   * @param projectName
   *     表所在{@link Project}
   * @param tableName
   *     表名
   * @param ifExists
   *     如果为 false 表不存在，则返回异常；若为 true，无论表是否存在，皆返回成功。
   * @throws OdpsException
   */
  public void delete(
      String projectName,
      String tableName,
      boolean ifExists) throws OdpsException {
    delete(projectName, odps.getCurrentSchema(), tableName, ifExists);
  }

  /**
   * Delete designated table.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param tableName Table name.
   * @param ifExists Delete only if the table exists.
   * @throws OdpsException
   */
  public void delete(
      String projectName,
      String schemaName,
      String tableName,
      boolean ifExists) throws OdpsException {

    if (projectName == null || tableName == null) {
      throw new IllegalArgumentException("Argument 'projectName' or 'tableName' cannot be null");
    }
    StringBuilder sb = new StringBuilder();
    sb.append("DROP TABLE ");
    if (ifExists) {
      sb.append(" IF EXISTS ");
    }

    sb.append(NameSpaceSchemaUtils.getFullName(projectName, schemaName, tableName));
    sb.append(";");

    // new SQLTask
    String taskName = "SQLDropTableTask";
    SQLTask task = new SQLTask();
    task.setName(taskName);
    task.setQuery(sb.toString());

    submitCreateAndWait(schemaName, sb.toString(), "SQLDropTableTask", null, null);
  }

  /**
   * 批量加载表信息
   *
   * @param tableNames
   *     表名
   * @return 加载后的 {@link Table} 列表
   * @throws OdpsException
   */
  public List<Table> loadTables(final Collection<String> tableNames) throws OdpsException {
    return loadTables(getDefaultProjectName(), tableNames);
  }

  /**
   * 批量加载表信息
   *
   * @param projectName
   *     指定{@link Project}名称
   * @param tableNames
   *     表名
   * @return 加载后的 {@link Table} 列表
   * @throws OdpsException
   */
  public List<Table> loadTables(
      final String projectName,
      final Collection<String> tableNames) throws OdpsException {
    return loadTables(projectName, odps.getCurrentSchema(), tableNames);
  }

  /**
   * Batch loading tables.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param tableNames Table names.
   * @return List of {@link Table}.
   * @throws OdpsException
   */
  public List<Table> loadTables(
      final String projectName,
      final String schemaName,
      final Collection<String> tableNames) throws OdpsException {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Invalid project name.");
    }
    if (tableNames == null) {
      throw new IllegalArgumentException("Invalid table names.");
    }

    QueryTables queryTables = new QueryTables();
    for (String name : tableNames) {
      queryTables.tables.add(new QueryTables.QueryTable(projectName, schemaName, name));
    }

    return loadTablesInternal(queryTables);
  }

  /**
   * 批量加载表信息<br />
   *
   * rest api 对请求数量有限制, 目前一次操作最多可请求 100 张表信息; <br />
   * 返回的表数据,与操作权限有关.<br />
   *
   * @param tables
   *     请求表的容器
   * @return 加载后的 {@link Table} 列表
   * @throws OdpsException
   */
  public List<Table> reloadTables(final Collection<Table> tables) throws OdpsException {
    if (tables == null) {
      throw new IllegalArgumentException("Invalid tables.");
    }

    return reloadTables(tables.iterator());
  }

  /**
   * 批量加载表信息<br />
   *
   * rest api 对请求数量有限制, 目前一次操作最多可请求 100 张表信息; <br />
   * 返回的表数据,与操作权限有关.<br />
   *
   * @param tables
   *     请求表的迭代器
   * @return 加载后的 {@link Table} 列表
   * @throws OdpsException
   */
  public List<Table> reloadTables(final Iterator<Table> tables) throws OdpsException {
    if (tables == null) {
      throw new IllegalArgumentException("Invalid tables.");
    }

    List<Table> loadedTables = new ArrayList<Table>();
    if (!tables.hasNext()) {
      return loadedTables;
    }

    QueryTables queryTables = new QueryTables();
    while (tables.hasNext()) {
      Table t = tables.next();

      if (t.isLoaded()) {
        // table is loaded, do not need to request again
        loadedTables.add(t);
      } else {
        queryTables.tables.add(
            new QueryTables.QueryTable(t.getProject(), t.getSchemaName(), t.getName()));
      }
    }

    if (!queryTables.tables.isEmpty()) {
      loadedTables.addAll(loadTablesInternal(queryTables));
    }

    return loadedTables;
  }

  private List<Table> loadTablesInternal(QueryTables queryTables) throws OdpsException {
    ArrayList<Table> reloadTables = new ArrayList<>();
    if (queryTables.tables.isEmpty()) {
      return reloadTables;
    }

    Map<String, String> params = new HashMap<>();
    params.put("query", null);

    String resource = ResourceBuilder.buildTablesResource(getDefaultProjectName());
    HashMap<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    String xml = null;
    try {
      xml = SimpleXmlUtils.marshal(queryTables);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

    ListTablesResponse resp =
        client.stringRequest(ListTablesResponse.class, resource, "POST", params, headers, xml);
    for (TableModel model : resp.tables) {
      Table t = new Table(model, model.projectName, model.schemaName, odps);
      t.reload(model);
      reloadTables.add(t);
    }

    return reloadTables;
  }

  /* private */
  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }

  private void submitCreateAndWait(String schemaName,
                                   String sql,
                                   String taskName,
                                   Map<String, String> hints,
                                   Map<String, String> aliases
  ) throws OdpsException {
    hints = NameSpaceSchemaUtils.setSchemaFlagInHints(hints, schemaName);
    Instance i = SQLTask.run(odps, odps.getDefaultProject(), sql, taskName, hints, aliases);
    i.waitForSuccess();
  }

  public TableCreator newTableCreator(String tableName, TableSchema schema) {
    return new TableCreator(odps, odps.getDefaultProject(), tableName, schema);
  }

  public TableCreator newTableCreator(String projectName, String tableName, TableSchema schema) {
    return new TableCreator(odps, projectName, tableName, schema);
  }

  public static class TableCreator {

    // must have
    private Odps odps;
    private String projectName;
    private String tableName;
    private TableSchema tableSchema;

    // optional common
    private String schemaName;
    private String comment;
    private boolean ifNotExists = false;
    private Long lifeCycle;
    private Map<String, String> hints;
    private Map<String, String> aliases;
    private List<String> primaryKeys;
    private boolean debug = false;

    // table type
    private TableFormat tableFormat = TableFormat.APPEND;

    public enum TableFormat {
      APPEND,
      TRANSACTION,
      DELTA,
      EXTERNAL,
      VIEW,
    }

    // for managed table (append, transaction, delta)
    private Table.ClusterInfo clusterInfo;
    private DataHubInfo dataHubInfo;
    private Map<String, String> tblProperties;

    // for external table
    private String storageHandler;
    private List<String> usingResources;
    private Map<String, String> serdeProperties;
    private String location;

    // for virtual view, and create table as statement
    private String selectStatement;

    // used for table creator
    private Map<String, String> preSetTblProperties = new HashMap<>();

    private TableCreator(Odps odps, String projectName, String tableName, TableSchema tableSchema) {
      ExceptionUtils.checkArgumentNotNull("odps", odps);
      ExceptionUtils.checkStringArgumentNotNull("projectName", projectName);
      ExceptionUtils.checkStringArgumentNotNull("tableName", tableName);
      this.odps = odps;
      this.projectName = projectName;
      this.tableName = tableName;
      this.tableSchema = tableSchema;
      this.schemaName = odps.getCurrentSchema();
    }

    public TableCreator withSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public TableCreator withComment(String comment) {
      this.comment = comment;
      return this;
    }

    public TableCreator ifNotExists() {
      this.ifNotExists = true;
      return this;
    }

    public TableCreator virtualView() {
      this.tableFormat = TableFormat.VIEW;
      return this;
    }

    public TableCreator transactionTable() {
      preSetTblProperties.put("transactional", "true");
      this.tableFormat = TableFormat.TRANSACTION;
      return this;
    }

    public TableCreator deltaTable() {
      preSetTblProperties.put("transactional", "true");
      this.tableFormat = TableFormat.DELTA;
      return this;
    }

    public TableCreator externalTable() {
      this.tableFormat = TableFormat.EXTERNAL;
      return this;
    }

    public TableCreator autoPartitionBy(GenerateExpression expression, String aliasName) {
      Column autoPtColumn = Column.newBuilder(aliasName, TypeInfoFactory.STRING)
          .withGenerateExpression(expression)
          .build();
      this.tableSchema.setPartitionColumns(ImmutableList.of(autoPtColumn));
      return this;
    }

    public TableCreator withLifeCycle(Long lifeCycle) {
      this.lifeCycle = lifeCycle;
      return this;
    }

    public TableCreator withClusterInfo(Table.ClusterInfo clusterInfo) {
      this.clusterInfo = clusterInfo;
      return this;
    }

    public TableCreator withPrimaryKeys(List<String> primaryKeys) {
      this.primaryKeys = primaryKeys;
      return this;
    }

    public TableCreator withDeltaTableBucketNum(Integer bucketNum) {
      Preconditions.checkInteger(bucketNum, 1, "bucketNum");
      preSetTblProperties.put("write.bucket.num", String.valueOf(bucketNum));
      return this;
    }

    public TableCreator withTblProperties(Map<String, String> tblProperties) {
      this.tblProperties = tblProperties;
      return this;
    }

    public TableCreator withStorageHandler(String storageHandler) {
      this.storageHandler = storageHandler;
      return this;
    }

    public TableCreator withResources(List<String> usingResources) {
      this.usingResources = usingResources;
      return this;
    }

    public TableCreator withLocation(String location) {
      this.location = location;
      return this;
    }

    public TableCreator withSerdeProperties(Map<String, String> serdeProperties) {
      this.serdeProperties = serdeProperties;
      return this;
    }

    public TableCreator withHints(Map<String, String> hints) {
      this.hints = hints;
      return this;
    }

    public TableCreator withAliases(Map<String, String> aliases) {
      this.aliases = aliases;
      return this;
    }

    public TableCreator withDataHubInfo(DataHubInfo dataHubInfo) {
      this.dataHubInfo = dataHubInfo;
      return this;
    }

    public TableCreator withSelectStatement(String selectStatement) {
      this.selectStatement = selectStatement;
      return this;
    }

    public TableCreator debug() {
      this.debug = true;
      return this;
    }

    public String getSQL() {
      return generateCreateTableSql();
    }

    public void create() throws OdpsException {
      checkParams();
      hints = NameSpaceSchemaUtils.setSchemaFlagInHints(hints, schemaName);
      Instance i =
          SQLTask.run(odps, projectName, generateCreateTableSql(), "SQLCreateTableTask", hints,
                      aliases);
      if (debug) {
        String logView = odps.logview().generateLogView(i, 24);
        System.out.println(logView);
      }
      i.waitForSuccess();
    }

    private void checkParams() {
      if (tableFormat == TableFormat.EXTERNAL) {
        ExceptionUtils.checkArgumentNotNull("storageHandler", storageHandler);
      } else if (tableFormat == TableFormat.VIEW) {
        ExceptionUtils.checkStringArgumentNotNull("selectStatement", selectStatement);
      } else if (tableFormat == TableFormat.DELTA) {
        ExceptionUtils.checkCollectionArgumentNotNull("primaryKey", primaryKeys);
      }
    }

    private String handleSelectStatementCause() {
      StringBuilder sql = new StringBuilder();
      switch (tableFormat) {
        case APPEND:
          sql.append("CREATE TABLE ");
          if (ifNotExists) {
            sql.append("IF NOT EXISTS ");
          }
          sql.append(NameSpaceSchemaUtils.getFullName(projectName, schemaName, tableName));
          if (lifeCycle != null) {
            sql.append(" LIFECYCLE ").append(lifeCycle);
          }
          sql.append(" AS (").append(selectStatement).append(");");
          return sql.toString();
        case VIEW:
          sql.append("CREATE VIEW ");
          if (ifNotExists) {
            sql.append("IF NOT EXISTS ");
          }
          sql.append(NameSpaceSchemaUtils.getFullName(projectName, schemaName, tableName));
          if (tableSchema != null && !tableSchema.getColumns().isEmpty()) {
            List<Column> columns = tableSchema.getColumns();
            sql.append(" (");
            for (int i = 0; i < columns.size(); i++) {
              Column column = columns.get(i);
              sql.append(CommonUtils.quoteRef(column.getName())).append(" ")
                  .append(getTypeName(column.getTypeInfo()));
              if (column.getComment() != null) {
                sql.append(" COMMENT ").append(CommonUtils.quoteStr(column.getComment()));
              }
              if (i + 1 < columns.size()) {
                sql.append(',');
              }
            }
            sql.append(')');
          }
          sql.append(" AS (").append(selectStatement).append(");");
          return sql.toString();
        default:
          throw new IllegalArgumentException("Only 'Append Table' and 'Virtual View' support 'create table as' statement.");
      }
    }

    private String generateCreateTableSql() {
      if (!StringUtils.isNullOrEmpty(selectStatement)) {
        return handleSelectStatementCause();
      }

      StringBuilder sql = new StringBuilder();
      // create [external] table [if not exists] <table_name>
      if (tableFormat == TableFormat.EXTERNAL) {
        sql.append("CREATE EXTERNAL TABLE ");
      } else {
        sql.append("CREATE TABLE ");
      }
      if (ifNotExists) {
        sql.append("IF NOT EXISTS ");
      }
      sql.append(NameSpaceSchemaUtils.getFullName(projectName, schemaName, tableName));

      // [(<col_name> <data_type> [not null] [default <default_value>] [comment <col_comment>], ...)]
      List<Column> columns = tableSchema.getColumns();
      sql.append(" (");
      for (int i = 0; i < columns.size(); i++) {
        Column column = columns.get(i);
        sql.append(CommonUtils.quoteRef(column.getName())).append(" ")
            .append(getTypeName(column.getTypeInfo()));
        if (!column.isNullable()) {
          sql.append(" NOT NULL");
        }
        if (StringUtils.isNotBlank(column.getDefaultValue())) {
          sql.append(" DEFAULT ").append(CommonUtils.quoteStr(column.getDefaultValue()));
        }
        if (column.getComment() != null) {
          sql.append(" COMMENT ").append(CommonUtils.quoteStr(column.getComment()));
        }
        if (i + 1 < columns.size()) {
          sql.append(',');
        }
      }
      if (primaryKeys != null && !primaryKeys.isEmpty()) {
        sql.append(", PRIMARY KEY(").append(primaryKeys.stream().map(CommonUtils::quoteRef).collect(
            Collectors.joining(","))).append(")");
      }
      sql.append(')');

      // [comment <table_comment>]
      if (comment != null) {
        sql.append(" COMMENT ").append(CommonUtils.quoteStr(comment)).append(" ");
      }

      // [partitioned by (<col_name> <data_type> [comment <col_comment>], ...)]
      // [auto partitioned by (<generate_expression> AS <col_name>)]
      List<Column> partitionColumns = tableSchema.getPartitionColumns();
      if (!partitionColumns.isEmpty()) {
        // Currently, the auto-partition table only has one partition column (there may be more in the future),
        // so here we only take the first column to check to determine whether the table is an auto-partition table.
        if (partitionColumns.get(0).getGenerateExpression() != null) {
          sql.append(" AUTO PARTITIONED BY (").append(partitionColumns.get(0).getGenerateExpression())
              .append(" AS ").append(CommonUtils.quoteRef(partitionColumns.get(0).getName()))
              .append(")");
        } else {
          sql.append(" PARTITIONED BY (");
          for (int i = 0; i < partitionColumns.size(); i++) {
            Column column = partitionColumns.get(i);
            sql.append(CommonUtils.quoteRef(column.getName())).append(" ")
                .append(getTypeName(column.getTypeInfo()));
            if (column.getComment() != null) {
              sql.append(" COMMENT ").append(CommonUtils.quoteStr(column.getComment()));
            }
            if (i + 1 < partitionColumns.size()) {
              sql.append(',');
            }
          }
          sql.append(')');
        }
      }
      // [clustered by | range clustered by (<col_name> [, <col_name>, ...]) [sorted by (<col_name> [asc | desc] [, <col_name> [asc | desc] ...])] into <number_of_buckets> buckets]
      if (clusterInfo != null) {
        sql.append(clusterInfo);
      }
      // [stored by StorageHandler]
      if (storageHandler != null) {
        sql.append(" STORED BY ").append(CommonUtils.quoteStr(storageHandler));
      }
      // [with serdeproperties (options)]
      if (serdeProperties != null && !serdeProperties.isEmpty()) {
        sql.append(" WITH SERDEPROPERTIES (");
        int index = 0;
        for (Map.Entry<String, String> entry : serdeProperties.entrySet()) {
          index++;
          sql.append(CommonUtils.quoteStr(entry.getKey())).append("=").append(CommonUtils.quoteStr(entry.getValue()));
          if (index != serdeProperties.size()) {
            sql.append(" , ");
          }
        }
        sql.append(")");
      }
      // [location <osslocation>]
      if (!StringUtils.isNullOrEmpty(location)) {
        sql.append(" LOCATION ").append(CommonUtils.quoteStr(location));
      }
      // [USING '<resource_name>']
      if (usingResources != null && !usingResources.isEmpty()) {
        sql.append(" USING ").append(CommonUtils.quoteStr(String.join(",", usingResources)));
      }
      // [tblproperties("transactional"="true")]
      Map<String, String> allTblProperties = getTblProperties();
      if (!allTblProperties.isEmpty()) {
        sql.append(" TBLPROPERTIES(");
        for (Map.Entry<String, String> entry : allTblProperties.entrySet()) {
          sql.append(CommonUtils.quoteStr(entry.getKey())).append("=").append(CommonUtils.quoteStr(entry.getValue())).append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
      }
      // [lifecycle <days>]
      if (lifeCycle != null) {
        sql.append(" LIFECYCLE ").append(lifeCycle);
      }

      if (dataHubInfo != null) {
        sql.append(dataHubInfo);
      }

      sql.append(';');
      if (debug) {
        System.out.println(sql);
      }
      return sql.toString();
    }

    private String getTypeName(TypeInfo typeInfo) {
      if (typeInfo instanceof StructTypeInfo) {
        return ((StructTypeInfo) typeInfo).getTypeName(true);
      }
      return typeInfo.getTypeName();
    }

    private Map<String, String> getTblProperties() {
      Map<String, String> allTblProperties = new HashMap<>();
      if (tblProperties != null) {
        allTblProperties.putAll(tblProperties);
      }
      allTblProperties.putAll(preSetTblProperties);
      return allTblProperties;
    }
  }

  /**
   * DataHub Service (DHS) is a built-in service of ODPS, which need to create special tables to write data.
   * To be determined if there are still users using it.
   */
  public static class DataHubInfo {
    Long shardNum;
    Long hubLifecycle;

    /**
     *
     * @param shardNum represents a concurrent channel for data transmission on a table.
     * @param hubLifecycle The Online Data in the Hub table will be recycled periodically. The default value is 7 days.
     */
    public DataHubInfo(Long shardNum, Long hubLifecycle) {
      this.shardNum = shardNum;
      this.hubLifecycle = hubLifecycle;
    }

    @Override
    public String toString() {
      StringBuilder sql = new StringBuilder();
      if (shardNum != null) {
        sql.append(" INTO ").append(shardNum).append(" SHARDS");
      }

      if (hubLifecycle != null) {
        sql.append(" HUBLIFECYCLE ").append(hubLifecycle);
      }
      return sql.toString();
    }
  }
}
