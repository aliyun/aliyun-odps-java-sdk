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

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Odps odps = new Odps(...);
Table table = odps.tables().get("project_name", "table_name");
```

</TabItem>
<TabItem value="python" label="Python">

```python
from odps import ODPS

odps = ODPS(...)
table = odps.get_table('table_name', project='project_name')
```

</TabItem>
<TabItem value="go" label="Go">

```go
odpsIns := odps.NewOdps(account, endpoint)
odpsIns.SetDefaultProjectName("project_name")
table := odpsIns.Tables().Get("table_name")
```

</TabItem>
</Tabs>

## 重命名表

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void rename(String newName) throws Exception
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.rename(new_name)
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) Rename(newName string) error
```

</TabItem>
</Tabs>

将表重命名为新名称，新名称必须符合 MaxCompute 命名规则。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
table.rename("new_table_name");
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.rename('new_table_name')
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.Rename("new_table_name")
```

</TabItem>
</Tabs>

## 修改生命周期

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void setLifeCycle(int days) throws OdpsException
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.set_lifecycle(days)
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) SetLifeCycle(days int) error
```

</TabItem>
</Tabs>

修改表的生命周期，单位为天，必须为正整数。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 设置生命周期为 90 天
table.setLifeCycle(90);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 设置生命周期为 90 天
table.set_lifecycle(90)
```

</TabItem>
<TabItem value="go" label="Go">

```go
// 设置生命周期为 90 天
err := table.SetLifeCycle(90)
```

</TabItem>
</Tabs>

## 更改表所有者

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void changeOwner(String newOwner) throws OdpsException
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.set_owner(new_owner)
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) ChangeOwner(newOwner string) error
```

</TabItem>
</Tabs>

更改表的所有者。只有项目所有者或具备超级管理角色的用户可以执行此操作。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
table.changeOwner("new_owner_id");
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.set_owner('new_owner_id')
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.ChangeOwner("new_owner_id")
```

</TabItem>
</Tabs>

## 修改表注释

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void changeComment(String newComment) throws OdpsException
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.set_comment(new_comment)
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) ChangeComment(newComment string) error
```

</TabItem>
</Tabs>

修改表的注释内容，可以传入空字符串来清空注释。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
table.changeComment("这是一张用户行为日志表");
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.set_comment('这是一张用户行为日志表')
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.ChangeComment("这是一张用户行为日志表")
```

</TabItem>
</Tabs>

## 更新时间戳

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void touch() throws OdpsException
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.touch()
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) Touch() error
```

</TabItem>
</Tabs>

将表的最后修改时间更新为当前时间。可用于延长表在生命周期管理下的存活时间。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
table.touch();
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.touch()
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.Touch()
```

</TabItem>
</Tabs>

## 添加列

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void addColumns(List<Column> columns, boolean ifNotExists) throws Exception
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.add_columns(columns, if_not_exists=False)
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) AddColumns(columns []tableschema.Column, ifNotExists bool) error
```

</TabItem>
</Tabs>

向表中添加新列。

**参数说明**
- `columns`：要添加的列列表
- `ifNotExists`：为 `true` 时，列已存在不会抛出异常

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
List<Column> newColumns = List.of(
    Column.newBuilder("email", TypeInfoFactory.STRING).withComment("邮箱地址").build(),
    Column.newBuilder("age", TypeInfoFactory.INT).build()
);

table.addColumns(newColumns, true);
```

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.types import Column

table.add_columns([
    Column('email', 'string', comment='邮箱地址'),
    Column('age', 'int'),
], if_not_exists=True)
```

</TabItem>
<TabItem value="go" label="Go">

```go
columns := []tableschema.Column{
    {Name: "email", Type: datatype.StringType, Comment: "邮箱地址"},
    {Name: "age", Type: datatype.IntType},
}

err := table.AddColumns(columns, true)
```

</TabItem>
</Tabs>

## 删除列

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void dropColumns(List<String> columnNames) throws Exception
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.delete_columns(columns)
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) DropColumns(columnNames []string) error
```

</TabItem>
</Tabs>

从表中删除指定列。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
List<String> columnsToDrop = List.of("temp_col", "deprecated_col");
table.dropColumns(columnsToDrop);
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.delete_columns(['temp_col', 'deprecated_col'])
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.DropColumns([]string{"temp_col", "deprecated_col"})
```

</TabItem>
</Tabs>

## 更改列类型

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void alterColumnType(String columnName, TypeInfo columnType) throws Exception
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Python SDK 暂不提供直接的更改列类型方法，可通过 execute_sql 执行 DDL
odps.execute_sql('ALTER TABLE table_name CHANGE COLUMN col_name col_name new_type;')
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) AlterColumnType(columnName string, columnType datatype.DataType) error
```

</TabItem>
</Tabs>

更改表中已有列的数据类型。注意只支持兼容的类型转换。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 将 age 列从 INT 改为 BIGINT
table.alterColumnType("age", TypeInfoFactory.BIGINT);

// 将 score 列改为 DECIMAL
table.alterColumnType("score", TypeInfoFactory.DECIMAL);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Python SDK 暂不提供直接的更改列类型方法，可通过 execute_sql 执行 DDL
odps.execute_sql('ALTER TABLE my_table CHANGE COLUMN age age BIGINT;')
odps.execute_sql('ALTER TABLE my_table CHANGE COLUMN score score DECIMAL;')
```

</TabItem>
<TabItem value="go" label="Go">

```go
// 将 age 列从 INT 改为 BIGINT
err := table.AlterColumnType("age", datatype.BigIntType)

// 将 score 列改为 DECIMAL
err = table.AlterColumnType("score", datatype.NewDecimalType(38, 18))
```

</TabItem>
</Tabs>

## 更改列名

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void changeColumnName(String oldColumnName, String newColumnName) throws Exception
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.rename_column(old_column_name, new_column_name)
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) ChangeColumnName(oldColumnName string, newColumnName string) error
```

</TabItem>
</Tabs>

修改表中某一列的名称。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
table.changeColumnName("old_column_name", "new_column_name");
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.rename_column('old_column_name', 'new_column_name')
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := table.ChangeColumnName("old_column_name", "new_column_name")
```

</TabItem>
</Tabs>

## 更改聚簇信息

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
public void changeClusterInfo(ClusterInfo clusterInfo) throws OdpsException
```

</TabItem>
<TabItem value="python" label="Python">

```python
table.set_cluster_info(new_cluster_info)
```

</TabItem>
<TabItem value="go" label="Go">

```go
func (t *Table) ChangeClusterInfo(clusterInfo tableschema.ClusterInfo) error
```

</TabItem>
</Tabs>

修改表的聚簇（Cluster）信息，可以变更聚簇列、排序方式或桶数量。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Table.ClusterInfo newClusterInfo = new Table.ClusterInfo(
    Table.ClusterInfo.ClusterType.HASH,
    List.of("user_id"),
    List.of(new Table.SortColumn("user_id", Table.SortColumn.Order.ASC)),
    32
);

table.changeClusterInfo(newClusterInfo);
```

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.models.cluster_info import ClusterInfo, ClusterType, ClusterSortCol, ClusterSortOrder

cluster_info = ClusterInfo(
    cluster_type=ClusterType.HASH,
    cluster_cols=['user_id'],
    sort_cols=[ClusterSortCol(name='user_id', order=ClusterSortOrder.ASC)],
    bucket_num=32,
)

table.set_cluster_info(cluster_info)
```

</TabItem>
<TabItem value="go" label="Go">

```go
clusterInfo := tableschema.ClusterInfo{
    ClusterType: tableschema.CLUSTER_TYPE.Hash,
    ClusterCols: []string{"user_id"},
    SortCols:    []tableschema.SortColumn{{Name: "user_id", Order: tableschema.SORT_ORDER.ASC}},
    BucketNum:   32,
}

err := table.ChangeClusterInfo(clusterInfo)
```

</TabItem>
</Tabs>

## 完整示例

以下示例展示了对表进行一系列修改操作的典型流程：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.types import Column

table = odps.get_table('user_events', project='my_project')

# 1. 修改表注释
table.set_comment('用户事件表 - 记录用户行为日志')

# 2. 设置生命周期为 180 天
table.set_lifecycle(180)

# 3. 添加新列
table.add_columns([
    Column('device_type', 'string', comment='设备类型'),
    Column('app_version', 'string', comment='应用版本'),
], if_not_exists=True)

# 4. 重命名列
table.rename_column('device_type', 'device_category')

# 5. 更新时间戳
table.touch()
```

</TabItem>
<TabItem value="go" label="Go">

```go
func alterTableExample(odpsIns *odps.Odps) error {
    table := odpsIns.Tables().Get("user_events")

    // 1. 修改表注释
    err := table.ChangeComment("用户事件表 - 记录用户行为日志")
    if err != nil {
        return err
    }

    // 2. 设置生命周期为 180 天
    err = table.SetLifeCycle(180)
    if err != nil {
        return err
    }

    // 3. 添加新列
    columns := []tableschema.Column{
        {Name: "device_type", Type: datatype.StringType, Comment: "设备类型"},
        {Name: "app_version", Type: datatype.StringType, Comment: "应用版本"},
    }
    err = table.AddColumns(columns, true)
    if err != nil {
        return err
    }

    // 4. 重命名列
    err = table.ChangeColumnName("device_type", "device_category")
    if err != nil {
        return err
    }

    // 5. 更新时间戳
    return table.Touch()
}
```

</TabItem>
</Tabs>
