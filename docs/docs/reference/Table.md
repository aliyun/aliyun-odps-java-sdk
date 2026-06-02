---
title: Table
description: MaxCompute 表对象，提供表元数据查询、数据操作、DDL、分区及标签管理
sidebar_position: 2
module: odps-sdk-core
apis: [Table]
keywords: [Table, 表, 分区, Schema, DDL, 标签]
---

# Table

`Table` 类代表 MaxCompute 中的表，提供表元数据查询、数据读取、DDL 操作、分区管理和标签管理功能。

## 获取实例

```java
Table table = odps.tables().get("table_name");
Table table = odps.tables().get("project_name", "table_name");
// 三层模型
Table table = odps.tables().get("project_name", "schema_name", "table_name");
```

获取表实例为 lazy 操作，只有调用其他方法时才会请求元数据。可手动触发加载：

```java
table.reload();
```

## Tables 集合操作

以下方法属于 `Tables` 类，通过 `odps.tables()` 访问。

### get

获取表对象（延迟加载，不立即请求服务端）。

```java
public Table get(String tableName)
public Table get(String projectName, String tableName)
public Table get(String projectName, String schemaName, String tableName)
```

---

### exists

判断表是否存在。

```java
public boolean exists(String tableName) throws OdpsException
public boolean exists(String projectName, String tableName) throws OdpsException
public boolean exists(String projectName, String schemaName, String tableName) throws OdpsException
```

---

### create

创建表。推荐使用 `newTableCreator` 的 Builder 模式。

```java
public void create(String tableName, TableSchema schema) throws OdpsException
public void create(String tableName, TableSchema schema, boolean ifNotExists) throws OdpsException
public void create(String projectName, String tableName, TableSchema schema, boolean ifNotExists) throws OdpsException
```

---

### newTableCreator

使用 Builder 模式创建表，支持分区表、Delta 表、外部表等高级建表选项。

```java
public TableCreator newTableCreator(String tableName, TableSchema schema)
public TableCreator newTableCreator(String projectName, String tableName, TableSchema schema)
```

TableCreator 链式方法：

| 方法 | 说明 |
|------|------|
| `withSchemaName(String)` | 指定 Schema |
| `withComment(String)` | 表注释 |
| `withLifecycle(long)` | 生命周期（天） |
| `ifNotExists()` | 表存在时不报错 |
| `withPartitionDef(TableSchema)` | 分区定义 |
| `withPrimaryKeys(List<String>)` | 主键列（Delta 表） |
| `transactionTable()` | 标记为事务表 |
| `withStorageHandler(String)` | 外部表 StorageHandler |
| `withLocation(String)` | 外部表数据位置 |
| `withSerDeProperties(Map)` | 序列化属性 |
| `withJars(List<String>)` | 关联 JAR 资源 |
| `withClusterInfo(ClusterInfo)` | 聚簇信息 |
| `create()` | 执行建表 |

示例：
```java
odps.tables().newTableCreator("my_table", schema)
    .withComment("demo table")
    .withLifecycle(30)
    .ifNotExists()
    .create();
```

---

### createExternal

创建外部表。

```java
public void createExternal(String projectName, String tableName, TableSchema schema,
                           String location, String storageHandler,
                           Map<String, String> serDeProperties,
                           List<String> jars, String comment) throws OdpsException
```

---

### delete

删除表。

```java
public void delete(String tableName) throws OdpsException
public void delete(String tableName, boolean ifExists) throws OdpsException
public void delete(String projectName, String tableName) throws OdpsException
public void delete(String projectName, String schemaName, String tableName) throws OdpsException
public void delete(String projectName, String schemaName, String tableName, boolean ifExists) throws OdpsException
```

---

### iterator / iterable

遍历项目中的表。

```java
public Iterator<Table> iterator()
public Iterator<Table> iterator(String projectName)
public Iterator<Table> iterator(TableFilter filter)
public Iterator<Table> iterator(String projectName, TableFilter filter)
public Iterator<Table> iterator(String projectName, String schemaName, TableFilter filter)
public Iterable<Table> iterable()
public Iterable<Table> iterable(String projectName)
public Iterable<Table> iterable(TableFilter filter)
public Iterable<Table> iterable(String projectName, TableFilter filter)
public Iterable<Table> iterable(String projectName, String schemaName, TableFilter filter)
```

TableFilter 属性：

| 属性 | 说明 |
|------|------|
| `name` | 表名前缀匹配 |
| `owner` | 表所有者 |
| `type` | 表类型筛选 |

示例：
```java
TableFilter filter = new TableFilter();
filter.setName("user_");
for (Table t : odps.tables().iterable(filter)) {
    System.out.println(t.getName());
}
```

---

### loadTables

批量加载表元数据（一次 API 请求获取多张表的信息）。

```java
public List<Table> loadTables(Collection<String> tableNames) throws OdpsException
public List<Table> loadTables(String projectName, Collection<String> tableNames) throws OdpsException
public List<Table> loadTables(String projectName, String schemaName, Collection<String> tableNames) throws OdpsException
```

---

### reloadTables

批量刷新已有表对象的元数据。

```java
public List<Table> reloadTables(Collection<Table> tables) throws OdpsException
public List<Table> reloadTables(Iterator<Table> tables) throws OdpsException
```

## 基本信息

### getName

```java
public String getName()
```

**返回值**：表名称

---

### getSchema

```java
public TableSchema getSchema()
```

**返回值**：`TableSchema` 对象，包含列定义和分区列定义

```java
tableSchema.getColumns();          // data 列
tableSchema.getPartitionColumns(); // 分区列
```

---

### getJsonSchema

```java
public String getJsonSchema()
```

**返回值**：JSON 格式的表结构描述

---

### getProject

```java
public String getProject()
```

**返回值**：表所在项目名

---

### getSchemaName

```java
public String getSchemaName()
```

**返回值**：表所在 Schema 名（需开启三层模型）

---

### getComment

```java
public String getComment()
```

**返回值**：表注释

---

### getOwner

```java
public String getOwner()
```

**返回值**：表所属用户

---

### getType

```java
public Table.TableType getType()
```

**返回值**：`TableType` 枚举，可选值：`MANAGED_TABLE`、`VIRTUAL_VIEW`、`EXTERNAL_TABLE`、`MATERIALIZED_VIEW`

衍生判断方法：

```java
table.isVirtualView();
table.isMaterializedView();
table.isExternalTable();
```

---

### getCreatedTime

```java
public Date getCreatedTime()
```

---

### getLastMetaModifiedTime

```java
public Date getLastMetaModifiedTime()
```

---

### getSize

```java
public long getSize()
```

**返回值**：表存储大小（bytes），通常为估计值

---

### getRecordNum

```java
public long getRecordNum()
```

**返回值**：表数据行数，无准确数据时返回 -1

---

### getLife

```java
public long getLife()
```

**返回值**：表生命周期（天）

---

### getTableID

获取表的唯一标识符。

```java
public String getTableID()
```

**返回值**：表的唯一 ID

---

### getTableLabel

获取表的安全标签等级。

```java
public String getTableLabel()
```

**返回值**：表的安全标签等级

---

### getTableExtendedLabels

获取表的扩展标签列表。

```java
public List<String> getTableExtendedLabels()
```

**返回值**：扩展标签列表

---

### getMaxLabel

获取表和列的最高安全标签等级。

```java
public String getMaxLabel()
```

**返回值**：最高安全标签等级

---

### getCryptoAlgoName

获取表的加密算法名称。

```java
public String getCryptoAlgoName()
```

**返回值**：加密算法名称

---

### getLastDataModifiedTime

获取数据最后修改时间。

```java
public Date getLastDataModifiedTime()
```

**返回值**：数据最后修改时间

---

### getLastDataAccessTime

获取数据最后访问时间。

```java
public Date getLastDataAccessTime()
```

**返回值**：数据最后访问时间

---

### getHubLifecycle

获取 DataHub 生命周期（天）。

```java
public long getHubLifecycle()
```

**返回值**：DataHub 生命周期天数

---

### isPartitioned

判断是否为分区表。

```java
public boolean isPartitioned()
```

**返回值**：`true` 表示为分区表

## 数据操作

### read

```java
public RecordReader read(int limit) throws OdpsException
public RecordReader read(PartitionSpec partition, List<String> columns, int limit) throws OdpsException
public RecordReader read(PartitionSpec partition, List<String> columns, int limit, String timezone) throws OdpsException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `limit` | int | 最多读取行数，最大 10000 |
| `partition` | PartitionSpec | 分区表达式，null 表示全表 |
| `columns` | List\<String\> | 列名列表，null 表示全部列 |
| `timezone` | String | datetime 时区，如 `"Asia/Shanghai"` |

**注意**：最多返回 1W 行，数据不超过 10MB。大量数据请使用 Tunnel。

---

### truncate

清空表数据。

```java
public void truncate() throws OdpsException
```

## DDL

### setLifeCycle

```java
public void setLifeCycle(int days) throws OdpsException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `days` | int | 生命周期天数，正整数 |

---

### changeOwner

```java
public void changeOwner(String newOwner) throws OdpsException
```

---

### changeComment

```java
public void changeComment(String newComment) throws OdpsException
```

---

### touch

更新表的最后修改时间为当前时间。

```java
public void touch() throws OdpsException
```

---

### rename

```java
public void rename(String newName) throws Exception
```

---

### addColumns

```java
public void addColumns(List<Column> columns, boolean ifNotExists) throws Exception
```

---

### dropColumns

```java
public void dropColumns(List<String> columnNames) throws Exception
```

---

### alterColumnType

```java
public void alterColumnType(String columnName, TypeInfo columnType) throws Exception
```

---

### changeColumnName

```java
public void changeColumnName(String oldColumnName, String newColumnName) throws Exception
```

---

### changeClusterInfo

```java
public void changeClusterInfo(ClusterInfo clusterInfo) throws OdpsException
```

## 分区

### getPartition

```java
public Partition getPartition(PartitionSpec partitionSpec)
```

---

### getPartitions

```java
public List<Partition> getPartitions()
```

---

### getPartitionSpecs

仅返回分区值，不含详细信息，效率更高。

```java
public List<PartitionSpec> getPartitionSpecs()
```

---

### hasPartition

```java
public boolean hasPartition(PartitionSpec partitionSpec)
```

---

### createPartition

```java
public void createPartition(PartitionSpec partitionSpec) throws OdpsException
```

---

### deletePartition

```java
public void deletePartition(PartitionSpec partitionSpec) throws OdpsException
```

---

### createPartition (ifNotExists)

创建分区，可指定 ifNotExists。

```java
public void createPartition(PartitionSpec spec, boolean ifNotExists) throws OdpsException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `spec` | PartitionSpec | 分区表达式 |
| `ifNotExists` | boolean | 为 `true` 时，分区已存在不抛异常 |

---

### deletePartition (ifExists)

删除分区，可指定 ifExists。

```java
public void deletePartition(PartitionSpec spec, boolean ifExists) throws OdpsException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `spec` | PartitionSpec | 分区表达式 |
| `ifExists` | boolean | 为 `true` 时，分区不存在不抛异常 |

---

### getPartitionIterator

获取所有分区的迭代器。

```java
public Iterator<Partition> getPartitionIterator()
```

**返回值**：分区迭代器

---

### getPartitionIterator (指定父分区)

获取指定父分区下的分区迭代器。

```java
public Iterator<Partition> getPartitionIterator(PartitionSpec spec)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `spec` | PartitionSpec | 父分区表达式 |

**返回值**：分区迭代器

---

### getPartitionIterator (高级)

获取分区迭代器，支持反序、分页和数量限制。

```java
public Iterator<Partition> getPartitionIterator(PartitionSpec spec, boolean reverse, Long batchSize, Long limit)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `spec` | PartitionSpec | 父分区表达式，null 表示全部 |
| `reverse` | boolean | 是否反序返回 |
| `batchSize` | Long | 每次请求的分区数量 |
| `limit` | Long | 最多返回的分区总数 |

**返回值**：分区迭代器

## 标签

### getTags

```java
// 表级别
public List<Tag> getTags()
// 字段级别
public List<Tag> getTags(String columnName)
```

---

### addTag

```java
public void addTag(Tag tag)
public void addTag(Tag tag, List<String> columns)
```

---

### removeTag

```java
public void removeTag(Tag tag)
```

---

### getSimpleTags

```java
public Map<String, Map<String, String>> getSimpleTags()
public Map<String, Map<String, String>> getSimpleTags(String columnName)
```

---

### addSimpleTag

```java
public void addSimpleTag(String category, String key, String value)
public void addSimpleTag(String category, String key, String value, List<String> columns)
```

---

### removeSimpleTag

```java
public void removeSimpleTag(String category, String key, String value)
public void removeSimpleTag(String category, String key, String value, List<String> columns)
```

## 扩展信息

这部分信息通过 lazy 加载获取，无法通过 `reload()` 刷新。

### isArchived

```java
public boolean isArchived()
```

---

### isTransactional

```java
public boolean isTransactional()
```

---

### isDeltaTable

判断是否为 Delta Table（带主键的事务表）。

```java
public boolean isDeltaTable()
```

**返回值**：`true` 表示为 Delta Table

---

### hasRowAccessPolicy

判断表是否设置了行级访问策略。

```java
public boolean hasRowAccessPolicy()
```

**返回值**：`true` 表示设置了行级访问策略

---

### getPrimaryKey

获取主键列名列表。

```java
public List<String> getPrimaryKey()
```

**返回值**：主键列名列表，无主键时返回空列表

---

### getAcidDataRetainHours

获取 ACID 数据保留时长（小时）。

```java
public int getAcidDataRetainHours()
```

**返回值**：数据保留时长（小时）

---

### getPhysicalSize

```java
public long getPhysicalSize()
```

**返回值**：表磁盘物理大小（估计值）

---

### getFileNum

```java
public long getFileNum()
```

**返回值**：表占用文件数（估计值）

---

### getClusterInfo

```java
public ClusterInfo getClusterInfo()
```

---

### getReserved

```java
public String getReserved()
```

**返回值**：JSON 格式的保留字段信息

---

### 视图相关

```java
public String getViewText()
public String getViewExpandedText()
public boolean isMaterializedViewRewriteEnabled()
public boolean isMaterializedViewOutdated()
```

---

### isAutoRefreshEnabled

获取物化视图是否启用自动刷新。

```java
public boolean isAutoRefreshEnabled()
```

**返回值**：`true` 表示启用自动刷新

---

### isAutoSubstituteEnabled

获取物化视图是否启用自动替换（可为 null）。

```java
public Boolean isAutoSubstituteEnabled()
```

**返回值**：`true` 表示启用自动替换，`null` 表示未设置

---

### getRefreshInterval

获取物化视图刷新间隔（分钟，可为 null）。

```java
public Integer getRefreshInterval()
```

**返回值**：刷新间隔（分钟），`null` 表示未设置

---

### getRefreshCron

获取物化视图刷新 cron 表达式。

```java
public String getRefreshCron()
```

**返回值**：cron 表达式

---

### getRefreshHistory

获取物化视图刷新历史（最多 10 条）。

```java
public List<Map<String, String>> getRefreshHistory()
```

**返回值**：刷新历史列表，每条记录为 key-value 形式

---

### 外部表相关

```java
public String getLocation()
public String getResources()
public String getStorageHandler()
public Map<String, String> getSerDeProperties()
```

## CDC (Change Data Capture)

### getCdcSize

获取 CDC 数据大小。

```java
public long getCdcSize()
```

**返回值**：CDC 数据大小（bytes）

---

### getCdcRecordNum

获取 CDC 记录数。

```java
public long getCdcRecordNum()
```

**返回值**：CDC 记录数

---

### getCdcLatestVersion

获取最新 CDC 版本号。

```java
public long getCdcLatestVersion()
```

**返回值**：最新 CDC 版本号

---

### getCdcLatestTimestamp

获取最新 CDC 时间戳。

```java
public Date getCdcLatestTimestamp()
```

**返回值**：最新 CDC 时间戳

## 存储分层 / 生命周期

### getStorageTierInfo

获取存储分层信息（仅非分区表）。

```java
public StorageTierInfo getStorageTierInfo()
```

**返回值**：`StorageTierInfo` 对象，包含存储分层详细信息

---

### getTableLifecycleConfig

获取表生命周期配置。

```java
public TableLifecycleConfig getTableLifecycleConfig()
```

**返回值**：`TableLifecycleConfig` 对象，包含生命周期配置

## Stream

### newStream

在事务表上创建新的 Stream。

```java
public Stream newStream(String streamName) throws OdpsException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `streamName` | String | Stream 名称 |

**返回值**：创建的 `Stream` 对象
