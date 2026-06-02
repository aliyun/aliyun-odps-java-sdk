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

### 外部表相关

```java
public String getLocation()
public String getResources()
public String getStorageHandler()
public Map<String, String> getSerDeProperties()
```
