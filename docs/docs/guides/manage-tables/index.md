---
title: 管理表
description: MaxCompute Java SDK 表生命周期管理概览，包括获取表实例、读取表元数据、表类型等
sidebar_position: 1
module: odps-sdk-core
task: manage-tables
apis:
  - Table
  - Tables
since: "0.47.0"
keywords:
  - Table
  - 表管理
  - 表类型
  - lazyload
  - 元数据
---

# 管理表

本章节介绍如何使用 MaxCompute Java SDK 对表进行全生命周期管理，包括创建、修改、分区管理和标签操作。

## 表类型

MaxCompute 中的表分为以下四种类型：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public enum TableType {
    MANAGED_TABLE,       // 常规 MaxCompute 内部表
    VIRTUAL_VIEW,        // 虚拟视图
    EXTERNAL_TABLE,      // 外部表
    MATERIALIZED_VIEW    // 物化视图
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Python SDK 中表类型通过 table.type 属性获取
# 可能的值: Table.Type.MANAGED_TABLE, VIRTUAL_VIEW, EXTERNAL_TABLE, MATERIALIZED_VIEW
```

</TabItem>
<TabItem value="go" label="Go">

```go
// Go SDK 中表类型定义
const (
    ManagedTable      TableType = "MANAGED_TABLE"
    VirtualView       TableType = "VIRTUAL_VIEW"
    ExternalTable     TableType = "EXTERNAL_TABLE"
    MaterializedView  TableType = "MATERIALIZED_VIEW"
)
```

</TabItem>
</Tabs>

可以通过以下方法判断表类型：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Table.TableType tableType = table.getType();

// 或使用便捷方法
table.isVirtualView();
table.isMaterializedView();
table.isExternalTable();
```

</TabItem>
<TabItem value="python" label="Python">

```python
table = odps.get_table('my_table')
table_type = table.type

# 判断表类型
table.is_virtual_view
table.is_materialized_view
```

</TabItem>
<TabItem value="go" label="Go">

```go
table := odpsIns.Table("my_table")
table.Load()
tableType := table.Type()
```

</TabItem>
</Tabs>

## 获取表实例

要操作一张表，首先需要获取 `Table` 实例对象。获取表实例是一个 **lazy** 操作，即此时不会发起网络请求，只有在调用 `Table` 类的其他方法时，才会真正获取表的元数据信息。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 指定项目名获取表实例
Table table = odps.tables().get("project_name", "table_name");

// 使用默认项目
Table table = odps.tables().get("table_name");

// 三层模型：指定 schema
Table table = odps.tables().get("project_name", "schema_name", "table_name");
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 指定项目名获取表实例
table = odps.get_table('table_name', project='project_name')

# 使用默认项目
table = odps.get_table('table_name')

# 三层模型：指定 schema
table = odps.get_table('table_name', project='project_name', schema='schema_name')
```

</TabItem>
<TabItem value="go" label="Go">

```go
// 使用默认项目获取表实例
table := odpsIns.Table("table_name")

// 通过 Tables 集合获取
table = odpsIns.Tables().Get("table_name")
```

</TabItem>
</Tabs>

## 读取表元数据

`Table` 实现了 `lazyload` 机制，首次调用以下方法时会自动发起网络请求加载元数据。也可以手动调用 `reload()` 强制刷新：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 手动加载/刷新元数据
table.reload();
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 手动加载/刷新元数据
table.reload()
```

</TabItem>
<TabItem value="go" label="Go">

```go
// 手动加载元数据
err := table.Load()
```

</TabItem>
</Tabs>

除非再次调用 `reload()`，否则后续访问均使用缓存数据。

### 常用元数据方法

| 方法 | 说明 |
|------|------|
| `table.getName()` | 获取表名 |
| `table.getProject()` | 获取所在项目名 |
| `table.getSchemaName()` | 获取所在 Schema 名（需开启 Schema 功能） |
| `table.getSchema()` | 获取表结构（TableSchema） |
| `table.getJsonSchema()` | 获取 JSON 格式的表结构 |
| `table.getComment()` | 获取表注释 |
| `table.getOwner()` | 获取表所属用户 |
| `table.getType()` | 获取表类型 |
| `table.getSize()` | 获取表存储大小（bytes，估计值） |
| `table.getRecordNum()` | 获取记录数（无准确数据时返回 -1） |
| `table.getLife()` | 获取生命周期（天） |
| `table.getCreatedTime()` | 获取创建时间 |
| `table.getLastMetaModifiedTime()` | 获取最后修改时间 |

### 示例

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Odps odps = new Odps(...);
Table table = odps.tables().get("my_project", "user_events");

// 获取表结构
TableSchema schema = table.getSchema();
List<Column> columns = schema.getColumns();           // 数据列
List<Column> partCols = schema.getPartitionColumns(); // 分区列

// 获取表大小
long sizeInBytes = table.getSize();

// 获取表类型
Table.TableType type = table.getType();
System.out.println("Table type: " + type);
```

</TabItem>
<TabItem value="python" label="Python">

```python
table = odps.get_table('user_events', project='my_project')

# 获取表结构
schema = table.table_schema
columns = schema.columns              # 数据列
part_cols = schema.partitions          # 分区列

# 获取表大小
size_in_bytes = table.size

# 获取表类型
table_type = table.type
print(f"Table type: {table_type}")
```

</TabItem>
<TabItem value="go" label="Go">

```go
table := odpsIns.Table("user_events")
table.Load()

// 获取表结构
schema := table.Schema()
columns := schema.Columns             // 数据列
partCols := schema.PartitionColumns   // 分区列

// 获取表大小
sizeInBytes := table.Size()

// 获取表类型
tableType := table.Type()
fmt.Printf("Table type: %s\n", tableType)
```

</TabItem>
</Tabs>

## 子章节

- [创建表](./create-table.md) - 使用 TableCreator 创建各类表
- [修改表](./alter-table.md) - 重命名、修改生命周期、增删列等操作
- [分区操作](./partitions.md) - 分区的增删查操作
- [标签操作](./tags.md) - 表级别和列级别的标签管理
