---
title: 修改表
description: 使用 MaxCompute Java SDK 对已有表执行修改操作，包括重命名、修改生命周期、增删列等
sidebar_position: 3
module: odps-sdk-core
task: manage-tables
apis:
  - Table
since: "0.51.0"
keywords:
  - 修改表
  - alter table
  - 重命名
  - 生命周期
  - 增删列
  - 聚簇信息
---

# 修改表

本文介绍如何使用 `Table` 对象对已有表进行修改操作，包括重命名、修改生命周期、增删列、更改列类型等。

## 前置条件

所有修改操作都需要先获取 `Table` 实例：

```java
Odps odps = new Odps(...);
Table table = odps.tables().get("project_name", "table_name");
```

## 重命名表

```java
public void rename(String newName) throws Exception
```

将表重命名为新名称，新名称必须符合 MaxCompute 命名规则。

```java
table.rename("new_table_name");
```

## 修改生命周期

```java
public void setLifeCycle(int days) throws OdpsException
```

修改表的生命周期，单位为天，必须为正整数。

```java
// 设置生命周期为 90 天
table.setLifeCycle(90);
```

## 更改表所有者

```java
public void changeOwner(String newOwner) throws OdpsException
```

更改表的所有者。只有项目所有者或具备超级管理角色的用户可以执行此操作。

```java
table.changeOwner("new_owner_id");
```

## 修改表注释

```java
public void changeComment(String newComment) throws OdpsException
```

修改表的注释内容，可以传入空字符串来清空注释。

```java
table.changeComment("这是一张用户行为日志表");
```

## 更新时间戳

```java
public void touch() throws OdpsException
```

将表的最后修改时间更新为当前时间。可用于延长表在生命周期管理下的存活时间。

```java
table.touch();
```

## 添加列

```java
public void addColumns(List<Column> columns, boolean ifNotExists) throws Exception
```

向表中添加新列。

**参数说明**
- `columns`：要添加的列列表
- `ifNotExists`：为 `true` 时，列已存在不会抛出异常

```java
List<Column> newColumns = List.of(
    Column.newBuilder("email", TypeInfoFactory.STRING).withComment("邮箱地址").build(),
    Column.newBuilder("age", TypeInfoFactory.INT).build()
);

table.addColumns(newColumns, true);
```

## 删除列

```java
public void dropColumns(List<String> columnNames) throws Exception
```

从表中删除指定列。

```java
List<String> columnsToDrop = List.of("temp_col", "deprecated_col");
table.dropColumns(columnsToDrop);
```

## 更改列类型

```java
public void alterColumnType(String columnName, TypeInfo columnType) throws Exception
```

更改表中已有列的数据类型。注意只支持兼容的类型转换。

```java
// 将 age 列从 INT 改为 BIGINT
table.alterColumnType("age", TypeInfoFactory.BIGINT);

// 将 score 列改为 DECIMAL
table.alterColumnType("score", TypeInfoFactory.DECIMAL);
```

## 更改列名

```java
public void changeColumnName(String oldColumnName, String newColumnName) throws Exception
```

修改表中某一列的名称。

```java
table.changeColumnName("old_column_name", "new_column_name");
```

## 更改聚簇信息

```java
public void changeClusterInfo(ClusterInfo clusterInfo) throws OdpsException
```

修改表的聚簇（Cluster）信息，可以变更聚簇列、排序方式或桶数量。

```java
Table.ClusterInfo newClusterInfo = new Table.ClusterInfo(
    Table.ClusterInfo.ClusterType.HASH,
    List.of("user_id"),
    List.of(new Table.SortColumn("user_id", Table.SortColumn.Order.ASC)),
    32
);

table.changeClusterInfo(newClusterInfo);
```

## 完整示例

以下示例展示了对表进行一系列修改操作的典型流程：

```java
public void alterTableExample(Odps odps) throws Exception {
    Table table = odps.tables().get("my_project", "user_events");

    // 1. 修改表注释
    table.changeComment("用户事件表 - 记录用户行为日志");

    // 2. 设置生命周期为 180 天
    table.setLifeCycle(180);

    // 3. 添加新列
    List<Column> newColumns = List.of(
        Column.newBuilder("device_type", TypeInfoFactory.STRING).withComment("设备类型").build(),
        Column.newBuilder("app_version", TypeInfoFactory.STRING).withComment("应用版本").build()
    );
    table.addColumns(newColumns, true);

    // 4. 重命名列
    table.changeColumnName("device_type", "device_category");

    // 5. 更新时间戳
    table.touch();
}
```
