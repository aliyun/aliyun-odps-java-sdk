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

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.Table.TableModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;

/**
 * Tables表示ODPS中所有{@link Table}的集合
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class Tables implements Iterable<Table> {

  @XmlRootElement(name = "Tables")
  private static class ListTablesResponse {

    @XmlElement(name = "Table")
    private List<TableModel> tables = new ArrayList<TableModel>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;
  }

  @XmlRootElement(name = "Tables")
  private static class QueryTables {

    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlRootElement(name = "Table")
    private static class QueryTable {

      @XmlElement(name = "Project")
      private String projectName;

      @XmlElement(name = "Name")
      private String tableName;

      QueryTable() {
      }

      QueryTable(String projectName, String tableName) {
        this.projectName = projectName;
        this.tableName = tableName;
      }
    }

    @XmlElement(name = "Table")
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
   * @return 指定表的信息{@link Table}
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
   * @return 指定表的信息{@link Table}
   */
  public Table get(String projectName, String tableName) {
    TableModel model = new TableModel();
    model.name = tableName;
    Table t = new Table(model, projectName, odps);
    return t;
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
    try {
      Table t = get(projectName, tableName);
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
    return new TableListIterator(projectName, filter);
  }


  /**
   * 获取默认{@link Project}的所有表信息迭代器 iterable
   *
   * @return {@link Table}迭代器
   */
  public Iterable<Table> iterable() {
    return new Iterable<Table>() {
      @Override
      public Iterator<Table> iterator() {
        return new TableListIterator(getDefaultProjectName(), null);
      }
    };
  }

  /**
   * 获取表信息迭代器 iterable
   *
   * @param projectName
   *     指定{@link Project}名称
   * @return {@link Table}迭代器
   */
  public Iterable<Table> iterable(final String projectName) {
    return new Iterable<Table>() {
      @Override
      public Iterator<Table> iterator() {
        return new TableListIterator(projectName, null);
      }
    };
  }

  /**
   * 获取默认Project的表信息迭代器 iterable
   *
   * @param filter
   *     过滤条件
   * @return {@link Table}迭代器
   */
  public Iterable<Table> iterable(final TableFilter filter) {
    return new Iterable<Table>() {
      @Override
      public Iterator<Table> iterator() {
        return new TableListIterator(getDefaultProjectName(), filter);
      }
    };
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
    return new Iterable<Table>() {
      @Override
      public Iterator<Table> iterator() {
        return new TableListIterator(projectName, filter);
      }
    };
  }

  private class TableListIterator extends ListIterator<Table> {

    Map<String, String> params = new HashMap<String, String>();

    private TableFilter filter;
    private String projectName;

    TableListIterator(String projectName, TableFilter filter) {
      this.filter = filter;
      this.projectName = projectName;
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
      }

      String resource = ResourceBuilder.buildTablesResource(projectName);
      try {

        ListTablesResponse resp = client.request(ListTablesResponse.class, resource, "GET",
                                                 params);

        for (TableModel model : resp.tables) {
          Table t = new Table(model, projectName, odps);
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
    // new SQLTask
    String taskName = "SQLCreateTableTask";
    SQLTask task = new SQLTask();
    task.setName(taskName);
    task.setQuery(
        getHubString(projectName, tableName, schema, comment, ifNotExists, shardNum, hubLifecycle));
    Instances instances = new Instances(odps);
    Instance instance = instances.create(task);

    instance.waitForSuccess();
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
    // new SQLTask
    String taskName = "SQLCreateTableTask";
    SQLTask task = new SQLTask();
    task.setName(taskName);
    task.setQuery(getSQLString(projectName, tableName, schema, comment, ifNotExists, lifeCycle));
    Instances instances = new Instances(odps);
    Instance instance = instances.create(task);

    instance.waitForSuccess();
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
  public void delete(String projectName, String tableName, boolean ifExists) throws OdpsException {
    if (projectName == null || tableName == null) {
      throw new IllegalArgumentException();
    }
    StringBuilder sb = new StringBuilder();
    sb.append("DROP TABLE ");
    if (ifExists) {
      sb.append(" IF EXISTS ");
    }

    sb.append(projectName).append(".").append(tableName).append(";");

    // new SQLTask
    String taskName = "SQLDropTableTask";
    SQLTask task = new SQLTask();
    task.setName(taskName);
    task.setQuery(sb.toString());
    Instances instances = new Instances(odps);
    Instance instance = instances.create(task);

    instance.waitForSuccess();
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
  public List<Table> loadTables(final String projectName, final Collection<String> tableNames)
      throws OdpsException {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Invalid project name.");
    }
    if (tableNames == null) {
      throw new IllegalArgumentException("Invalid table names.");
    }

    QueryTables queryTables = new QueryTables();
    for (String name : tableNames) {
      queryTables.tables.add(new QueryTables.QueryTable(projectName, name));
    }

    return loadTablesInternal(queryTables);
  }

  /**
   * 批量加载表信息<br />
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
        queryTables.tables.add(new QueryTables.QueryTable(t.getProject(), t.getName()));
      }
    }

    if (!queryTables.tables.isEmpty()) {
      loadedTables.addAll(loadTablesInternal(queryTables));
    }

    return loadedTables;
  }

  private List<Table> loadTablesInternal(QueryTables queryTables) throws OdpsException {
    ArrayList<Table> reloadTables = new ArrayList<Table>();
    if (queryTables.tables.isEmpty()) {
      return reloadTables;
    }

    Map<String, String> params = new HashMap<String, String>();
    params.put("query", null);

    String resource = ResourceBuilder.buildTablesResource(getDefaultProjectName());
    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    String xml = null;
    try {
      xml = JAXBUtils.marshal(queryTables, QueryTables.class);
    } catch (JAXBException e) {
      throw new OdpsException(e.getMessage(), e);
    }

    ListTablesResponse resp =
        client.stringRequest(ListTablesResponse.class, resource, "POST", params, headers, xml);
    for (TableModel model : resp.tables) {
      Table t = new Table(model, model.projectName, odps);
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

  private String getHubString(String projectName, String tableName, TableSchema schema,
                              String comment, boolean ifNotExists, Long shardNum,
                              Long hubLifecycle) {
    StringBuilder sb = new StringBuilder();
    sb.append(getSQLString(projectName, tableName, schema, comment, ifNotExists, null));
    if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }

    if (null != shardNum) {
      sb.append(" INTO " + String.valueOf(shardNum) + " SHARDS");
    }

    if (null != hubLifecycle) {
      sb.append(" HUBLIFECYCLE " + String.valueOf(hubLifecycle));
    }

    sb.append(';');

    return sb.toString();
  }

  private String getSQLString(
      String projectName, String tableName, TableSchema schema,
      String comment, boolean ifNotExists, Long lifeCycle) {
    if (projectName == null || tableName == null || schema == null) {
      throw new IllegalArgumentException();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    if (ifNotExists) {
      sb.append(" IF NOT EXISTS ");
    }

    sb.append(projectName).append(".`").append(tableName).append("` (");

    List<Column> columns = schema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      Column c = columns.get(i);
      sb.append("`").append(c.getName()).append("` ")
          .append(c.getTypeInfo().getTypeName());
      if (c.getComment() != null) {
        sb.append(" COMMENT '").append(c.getComment()).append("'");
      }
      if (i + 1 < columns.size()) {
        sb.append(',');
      }
    }

    sb.append(')');

    if (comment != null) {
      sb.append(" COMMENT '" + comment + "' ");
    }

    List<Column> pcolumns = schema.getPartitionColumns();
    if (pcolumns.size() > 0) {
      sb.append(" PARTITIONED BY (");
      for (int i = 0; i < pcolumns.size(); i++) {
        Column c = pcolumns.get(i);
        sb.append(c.getName()).append(" ")
            .append(c.getTypeInfo().getTypeName());
        if (c.getComment() != null) {
          sb.append(" COMMENT '").append(c.getComment()).append("'");
        }
        if (i + 1 < pcolumns.size()) {
          sb.append(',');
        }
      }
      sb.append(')');
    }

    if (lifeCycle != null) {
      sb.append(" LIFECYCLE ").append(lifeCycle);
    }

    sb.append(';');

    return sb.toString();
  }
}
