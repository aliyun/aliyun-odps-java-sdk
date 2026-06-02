---
title: 创建表
description: 使用 TableCreator 建表工具创建各种类型的 MaxCompute 表，包括分区表、Delta 表、外部表等
sidebar_position: 2
module: odps-sdk-core
task: manage-tables
apis:
  - TableCreator
  - Column
  - TableSchema
since: "0.48.0"
keywords:
  - TableCreator
  - 建表
  - Column
  - TableSchema
  - 分区表
  - Delta 表
  - 外部表
  - 聚簇表
  - auto-partition
---

# 创建表

`Tables.TableCreator` 是 MaxCompute Java SDK 提供的建表工具，采用 Builder 模式简化表创建过程。本文介绍如何使用 `TableCreator` 创建各种类型的表。

## TableCreator Builder 模式

### 获取 TableCreator 对象

```java
Odps odps = new Odps(...);

// 指定项目名
Tables.TableCreator creator = odps.tables().newTableCreator("project_name", "table_name", schema);

// 使用默认项目
Tables.TableCreator creator = odps.tables().newTableCreator("table_name", schema);
```

### 完整示例

```java
TableSchema schema = TableSchema.builder()
    .withColumn(Column.newBuilder("id", TypeInfoFactory.BIGINT).notNull().build())
    .withColumn(Column.newBuilder("name", TypeInfoFactory.STRING).withComment("用户名").build())
    .withColumn(Column.newBuilder("amount", TypeInfoFactory.DECIMAL).build())
    .withPartitionColumn(Column.newBuilder("dt", TypeInfoFactory.STRING).build())
    .build();

odps.tables().newTableCreator("my_project", "orders", schema)
    .withSchemaName("my_schema")           // 三层模型
    .withComment("订单表")                  // 表注释
    .withLifeCycle(365L)                   // 生命周期（天）
    .ifNotExists()                         // 表已存在时不报错
    .create();                             // 执行创建
```

## 列定义

使用 `Column.newBuilder` 构建列对象（since 0.47.0）：

```java
// 基本列定义
Column idCol = Column.newBuilder("id", TypeInfoFactory.BIGINT).build();
Column nameCol = Column.newBuilder("name", TypeInfoFactory.STRING).build();

// 设置列属性
Column col = Column.newBuilder("price", TypeInfoFactory.DECIMAL)
    .notNull()                                    // 非空约束
    .withComment("商品价格")                       // 列注释
    .withDefaultValue("0.00")                     // 默认值
    .build();
```

`TypeInfoFactory` 支持 MaxCompute 全部数据类型，包括 `BIGINT`、`STRING`、`DOUBLE`、`DECIMAL`、`DATETIME`、`BOOLEAN`、`ARRAY`、`MAP`、`STRUCT` 等。

## 构建表结构

`TableSchema` 包含数据列和分区列：

```java
TableSchema schema = TableSchema.builder()
    .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
    .withColumn(Column.newBuilder("c2", TypeInfoFactory.BIGINT).build())
    .withColumns(moreColumns)                    // 批量添加数据列
    .withPartitionColumn(Column.newBuilder("pt", TypeInfoFactory.STRING).build())
    .build();
```

也提供快捷方法创建简单表结构：

```java
TableSchema schema = TableSchema.builder()
    .withBigintColumn("id")
    .withStringColumn("name")
    .withDoubleColumn("score")
    .withDatetimeColumn("created_at")
    .withBooleanColumn("is_active")
    .build();
```

## 分区列

分区列通过 `withPartitionColumn` 指定，通常使用 `STRING` 类型：

```java
TableSchema schema = TableSchema.builder()
    .withColumn(Column.newBuilder("data", TypeInfoFactory.STRING).build())
    .withPartitionColumn(Column.newBuilder("dt", TypeInfoFactory.STRING).build())
    .withPartitionColumn(Column.newBuilder("region", TypeInfoFactory.STRING).build())
    .build();

odps.tables().newTableCreator("project", "partitioned_table", schema)
    .ifNotExists()
    .create();
```

## 主键 (since 0.48.5)

Delta 表必须配置主键信息，主键列必须设置 `notNull()`：

```java
TableSchema schema = TableSchema.builder()
    .withColumn(Column.newBuilder("pk1", TypeInfoFactory.BIGINT).notNull().build())
    .withColumn(Column.newBuilder("pk2", TypeInfoFactory.STRING).notNull().build())
    .withColumn(Column.newBuilder("value", TypeInfoFactory.STRING).build())
    .build();

odps.tables().newTableCreator("project", "delta_pk_table", schema)
    .deltaTable()
    .withPrimaryKeys(List.of("pk1", "pk2"))
    .withDeltaTableBucketNum(16)
    .create();
```

## 表格式

### Append 表（默认）

默认创建的即为 Append 表。如需 Append 2.0 格式，可通过 `withTblProperties` 指定：

```java
Map<String, String> props = new HashMap<>();
props.put("table.format.version", "2");

odps.tables().newTableCreator("project", "append_v2_table", schema)
    .withTblProperties(props)
    .create();
```

### Transaction 表

```java
odps.tables().newTableCreator("project", "txn_table", schema)
    .transactionTable()
    .create();
```

### Delta 表

Delta 表是一种支持高效 upsert 操作的事务表类型，必须配置主键和分桶数：

```java
TableSchema schema = TableSchema.builder()
    .withColumn(Column.newBuilder("pk", TypeInfoFactory.BIGINT).notNull().build())
    .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
    .withColumn(Column.newBuilder("c2", TypeInfoFactory.STRING).build())
    .build();

odps.tables().newTableCreator("project", "delta_table", schema)
    .deltaTable()
    .withPrimaryKeys(List.of("pk"))
    .withDeltaTableBucketNum(16)
    .ifNotExists()
    .create();
```

## 聚簇表 (Clustering)

### Hash Clustering

```java
Table.ClusterInfo clusterInfo = new Table.ClusterInfo(
    Table.ClusterInfo.ClusterType.HASH,
    List.of("c1", "c2"),                           // clustered by 列
    List.of(                                        // 桶内排序列
        new Table.SortColumn("c1", Table.SortColumn.Order.ASC),
        new Table.SortColumn("c2", Table.SortColumn.Order.DESC)
    ),
    64                                              // 桶数量
);

odps.tables().newTableCreator("project", "hash_cluster_table", schema)
    .withClusterInfo(clusterInfo)
    .create();
```

### Range Clustering

```java
Table.ClusterInfo clusterInfo = new Table.ClusterInfo(
    Table.ClusterInfo.ClusterType.RANGE,
    List.of("c1", "c2"),
    List.of(
        new Table.SortColumn("c1", Table.SortColumn.Order.ASC)
    ),
    0                                               // Range Cluster 桶数量可设为0（自动）
);

odps.tables().newTableCreator("project", "range_cluster_table", schema)
    .withClusterInfo(clusterInfo)
    .create();
```

## Auto-Partition (since 0.51.0)

Auto-partition 表可以根据生成表达式自动创建分区，无需手动管理分区：

```java
Column autoPartCol = Column.newBuilder("p1", TypeInfoFactory.STRING)
    .withGenerateExpression(new TruncTime("event_time", TruncTime.DatePart.DAY))
    .build();

TableSchema schema = TableSchema.builder()
    .withColumn(Column.newBuilder("event_time", TypeInfoFactory.DATETIME).build())
    .withColumn(Column.newBuilder("data", TypeInfoFactory.STRING).build())
    .withPartitionColumn(autoPartCol)
    .build();

odps.tables().newTableCreator("project", "auto_part_table", schema)
    .ifNotExists()
    .create();
```

`TruncTime.DatePart` 支持的粒度：`YEAR`、`MONTH`、`DAY`、`HOUR`。

## 外部表

外部表用于连接外部数据源（如 OSS）：

```java
TableSchema schema = TableSchema.builder()
    .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
    .withColumn(Column.newBuilder("c2", TypeInfoFactory.STRING).build())
    .build();

odps.tables().newTableCreator("project", "external_table", schema)
    .externalTable()
    .withStorageHandler("com.aliyun.odps.udf.example.text.TextStorageHandler")
    .withLocation("oss://bucket/path/to/data/")
    .withResources(List.of("odps-udf-example.jar"))
    .withSerdeProperties(Map.of(
        "odps.text.option.delimiter", "|",
        "odps.text.option.encoding", "UTF-8"
    ))
    .create();
```

## SQL 预览

在执行创建前，可以通过 `getSQL()` 方法预览将要执行的 DDL 语句：

```java
Tables.TableCreator creator = odps.tables().newTableCreator("project", "my_table", schema)
    .transactionTable()
    .withLifeCycle(90L);

// 预览 SQL
String sql = creator.getSQL();
System.out.println(sql);

// 确认后执行
creator.create();
```
