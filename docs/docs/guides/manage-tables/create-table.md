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

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Odps odps = new Odps(...);

// 指定项目名
Tables.TableCreator creator = odps.tables().newTableCreator("project_name", "table_name", schema);

// 使用默认项目
Tables.TableCreator creator = odps.tables().newTableCreator("table_name", schema);
```

</TabItem>
<TabItem value="python" label="Python">

```python
from odps import ODPS

odps = ODPS('<access_id>', '<access_key>', project='default_project', endpoint='<endpoint>')

# 使用 schema 对象创建表
table = odps.create_table('table_name', table_schema=schema)

# 使用简易字符串语法创建表
table = odps.create_table('table_name', 'col1 string, col2 bigint')
```

</TabItem>
<TabItem value="go" label="Go">

```go
import (
    "github.com/aliyun/aliyun-odps-go-sdk/odps"
    "github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// 使用默认项目
tables := odps.NewTables(odpsIns, odpsIns.DefaultProjectName(), "")

// 指定项目名
tables := odps.NewTables(odpsIns, "project_name", "")

// 调用 Create 创建表
err := tables.Create(schema, true, nil, nil)
```

</TabItem>
</Tabs>

### 完整示例

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
from odps import ODPS
from odps.models import TableSchema, Column, Partition

schema = TableSchema(
    columns=[
        Column('id', 'bigint', nullable=False),
        Column('name', 'string', comment='用户名'),
        Column('amount', 'decimal'),
    ],
    partitions=[
        Partition('dt', 'string'),
    ]
)

odps.create_table(
    'my_schema.orders',       # 三层模型使用 schema.table 格式
    table_schema=schema,
    comment='订单表',
    lifecycle=365,
    if_not_exists=True,
    project='my_project',
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
import (
    "github.com/aliyun/aliyun-odps-go-sdk/odps"
    "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
    "github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

schema := tableschema.NewSchemaBuilder().
    Name("orders").
    Comment("订单表").
    Column(tableschema.Column{Name: "id", Type: datatype.BigIntType, NotNull: true}).
    Column(tableschema.Column{Name: "name", Type: datatype.StringType, Comment: "用户名"}).
    Column(tableschema.Column{Name: "amount", Type: datatype.NewDecimalType(38, 18)}).
    PartitionColumn(tableschema.Column{Name: "dt", Type: datatype.StringType}).
    Lifecycle(365).
    Build()

// 三层模型通过 schemaName 参数指定
tables := odps.NewTables(odpsIns, "my_project", "my_schema")
err := tables.Create(schema, true, nil, nil) // createIfNotExists=true
```

</TabItem>
</Tabs>

## 列定义

使用 `Column.newBuilder` 构建列对象（since 0.47.0）：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.models import Column

# 基本列定义
id_col = Column('id', 'bigint')
name_col = Column('name', 'string')

# 设置列属性
col = Column(
    'price', 'decimal',
    nullable=False,              # 非空约束
    comment='商品价格',           # 列注释
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
import (
    "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
    "github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// 基本列定义
idCol := tableschema.Column{Name: "id", Type: datatype.BigIntType}
nameCol := tableschema.Column{Name: "name", Type: datatype.StringType}

// 设置列属性
col := tableschema.Column{
    Name:    "price",
    Type:    datatype.NewDecimalType(38, 18),
    NotNull: true,                  // 非空约束
    Comment: "商品价格",             // 列注释
}
```

</TabItem>
</Tabs>

`TypeInfoFactory` 支持 MaxCompute 全部数据类型，包括 `BIGINT`、`STRING`、`DOUBLE`、`DECIMAL`、`DATETIME`、`BOOLEAN`、`ARRAY`、`MAP`、`STRUCT` 等。

## 构建表结构

`TableSchema` 包含数据列和分区列：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
TableSchema schema = TableSchema.builder()
    .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
    .withColumn(Column.newBuilder("c2", TypeInfoFactory.BIGINT).build())
    .withColumns(moreColumns)                    // 批量添加数据列
    .withPartitionColumn(Column.newBuilder("pt", TypeInfoFactory.STRING).build())
    .build();
```

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.models import TableSchema, Column, Partition

schema = TableSchema(
    columns=[
        Column('c1', 'string'),
        Column('c2', 'bigint'),
    ],
    partitions=[
        Partition('pt', 'string'),
    ]
)

# 也可以用 from_lists 快捷创建
schema = TableSchema.from_lists(
    ['c1', 'c2'], ['string', 'bigint'],          # 数据列
    ['pt'], ['string']                            # 分区列
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
schema := tableschema.NewSchemaBuilder().
    Name("table_name").
    Column(tableschema.Column{Name: "c1", Type: datatype.StringType}).
    Column(tableschema.Column{Name: "c2", Type: datatype.BigIntType}).
    PartitionColumn(tableschema.Column{Name: "pt", Type: datatype.StringType}).
    Build()
```

</TabItem>
</Tabs>

也提供快捷方法创建简单表结构：

:::info 此功能仅 Java SDK 支持。:::

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

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.models import TableSchema, Column, Partition

schema = TableSchema(
    columns=[
        Column('data', 'string'),
    ],
    partitions=[
        Partition('dt', 'string'),
        Partition('region', 'string'),
    ]
)

odps.create_table('partitioned_table', schema, if_not_exists=True, project='project')
```

</TabItem>
<TabItem value="go" label="Go">

```go
schema := tableschema.NewSchemaBuilder().
    Name("partitioned_table").
    Column(tableschema.Column{Name: "data", Type: datatype.StringType}).
    PartitionColumn(tableschema.Column{Name: "dt", Type: datatype.StringType}).
    PartitionColumn(tableschema.Column{Name: "region", Type: datatype.StringType}).
    Build()

tables := odps.NewTables(odpsIns, "project", "")
err := tables.Create(schema, true, nil, nil)
```

</TabItem>
</Tabs>

## 主键 (since 0.48.5)

Delta 表必须配置主键信息，主键列必须设置 `notNull()`：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.models import TableSchema, Column

schema = TableSchema.from_lists(
    ['pk1', 'pk2', 'value'],
    ['bigint', 'string', 'string']
)

odps.create_table(
    'delta_pk_table', schema,
    primary_key=['pk1', 'pk2'],
    transactional=True,
    table_properties={'write.bucket.num': '16'},
    project='project',
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
schema := tableschema.NewSchemaBuilder().
    Name("delta_pk_table").
    Column(tableschema.Column{Name: "pk1", Type: datatype.BigIntType, NotNull: true}).
    Column(tableschema.Column{Name: "pk2", Type: datatype.StringType, NotNull: true}).
    Column(tableschema.Column{Name: "value", Type: datatype.StringType}).
    PrimaryKeys([]string{"pk1", "pk2"}).
    TblProperties(map[string]string{
        "transactional":    "true",
        "write.bucket.num": "16",
    }).
    Build()

tables := odps.NewTables(odpsIns, "project", "")
err := tables.Create(schema, false, nil, nil)
```

</TabItem>
</Tabs>

## 表格式

### Append 表（默认）

默认创建的即为 Append 表。如需 Append 2.0 格式，可通过 `withTblProperties` 指定：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Map<String, String> props = new HashMap<>();
props.put("table.format.version", "2");

odps.tables().newTableCreator("project", "append_v2_table", schema)
    .withTblProperties(props)
    .create();
```

</TabItem>
<TabItem value="python" label="Python">

```python
odps.create_table(
    'append_v2_table', schema,
    table_properties={'table.format.version': '2'},
    project='project',
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
schema := tableschema.NewSchemaBuilder().
    Name("append_v2_table").
    Columns(columns...).
    TblProperties(map[string]string{"table.format.version": "2"}).
    Build()

tables := odps.NewTables(odpsIns, "project", "")
err := tables.Create(schema, false, nil, nil)
```

</TabItem>
</Tabs>

### Transaction 表

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
odps.tables().newTableCreator("project", "txn_table", schema)
    .transactionTable()
    .create();
```

</TabItem>
<TabItem value="python" label="Python">

```python
odps.create_table(
    'txn_table', schema,
    transactional=True,
    project='project',
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
schema := tableschema.NewSchemaBuilder().
    Name("txn_table").
    Columns(columns...).
    TblProperties(map[string]string{"transactional": "true"}).
    Build()

tables := odps.NewTables(odpsIns, "project", "")
err := tables.Create(schema, false, nil, nil)
```

</TabItem>
</Tabs>

### Delta 表

Delta 表是一种支持高效 upsert 操作的事务表类型，必须配置主键和分桶数：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.models import TableSchema

schema = TableSchema.from_lists(
    ['pk', 'c1', 'c2'],
    ['bigint', 'string', 'string']
)

odps.create_table(
    'delta_table', schema,
    primary_key=['pk'],
    transactional=True,
    table_properties={'write.bucket.num': '16'},
    if_not_exists=True,
    project='project',
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
schema := tableschema.NewSchemaBuilder().
    Name("delta_table").
    Column(tableschema.Column{Name: "pk", Type: datatype.BigIntType, NotNull: true}).
    Column(tableschema.Column{Name: "c1", Type: datatype.StringType}).
    Column(tableschema.Column{Name: "c2", Type: datatype.StringType}).
    PrimaryKeys([]string{"pk"}).
    TblProperties(map[string]string{
        "transactional":    "true",
        "write.bucket.num": "16",
    }).
    Build()

tables := odps.NewTables(odpsIns, "project", "")
err := tables.Create(schema, true, nil, nil)
```

</TabItem>
</Tabs>

## 聚簇表 (Clustering)

### Hash Clustering

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
odps.execute_sql(
    'CREATE TABLE project.hash_cluster_table (c1 STRING, c2 STRING, c3 STRING) '
    'CLUSTERED BY (c1, c2) '
    'SORTED BY (c1 ASC, c2 DESC) '
    'INTO 64 BUCKETS'
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
schema := tableschema.NewSchemaBuilder().
    Name("hash_cluster_table").
    Column(tableschema.Column{Name: "c1", Type: datatype.StringType}).
    Column(tableschema.Column{Name: "c2", Type: datatype.StringType}).
    Column(tableschema.Column{Name: "c3", Type: datatype.StringType}).
    ClusterType(tableschema.CLUSTER_TYPE.Hash).
    ClusterColumns([]string{"c1", "c2"}).
    ClusterSortColumns([]tableschema.SortColumn{
        {Name: "c1", Order: "asc"},
        {Name: "c2", Order: "desc"},
    }).
    ClusterBucketNum(64).
    Build()

tables := odps.NewTables(odpsIns, "project", "")
err := tables.Create(schema, false, nil, nil)
```

</TabItem>
</Tabs>

### Range Clustering

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
odps.execute_sql(
    'CREATE TABLE project.range_cluster_table (c1 STRING, c2 STRING, c3 STRING) '
    'RANGE CLUSTERED BY (c1, c2) '
    'SORTED BY (c1 ASC)'
    # Range Cluster 桶数量可省略（自动）
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
schema := tableschema.NewSchemaBuilder().
    Name("range_cluster_table").
    Column(tableschema.Column{Name: "c1", Type: datatype.StringType}).
    Column(tableschema.Column{Name: "c2", Type: datatype.StringType}).
    Column(tableschema.Column{Name: "c3", Type: datatype.StringType}).
    ClusterType(tableschema.CLUSTER_TYPE.Range).
    ClusterColumns([]string{"c1", "c2"}).
    ClusterSortColumns([]tableschema.SortColumn{
        {Name: "c1", Order: "asc"},
    }).
    ClusterBucketNum(0). // Range Cluster 桶数量可设为0（自动）
    Build()

tables := odps.NewTables(odpsIns, "project", "")
err := tables.Create(schema, false, nil, nil)
```

</TabItem>
</Tabs>

## Auto-Partition (since 0.51.0)

Auto-partition 表可以根据生成表达式自动创建分区，无需手动管理分区：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.models import TableSchema, Column, Partition

schema = TableSchema(
    columns=[
        Column('event_time', 'datetime'),
        Column('data', 'string'),
    ],
    partitions=[
        Partition('p1', 'string', generate_expression="trunc_time(`event_time`, 'DAY')"),
    ]
)

odps.create_table('auto_part_table', schema, if_not_exists=True, project='project')
```

</TabItem>
<TabItem value="go" label="Go">

```go
autoPartCol := tableschema.Column{
    Name:               "p1",
    Type:               datatype.StringType,
    GenerateExpression: tableschema.NewTruncTime("event_time", tableschema.DAY),
}

schema := tableschema.NewSchemaBuilder().
    Name("auto_part_table").
    Column(tableschema.Column{Name: "event_time", Type: datatype.DateTimeType}).
    Column(tableschema.Column{Name: "data", Type: datatype.StringType}).
    PartitionColumn(autoPartCol).
    Build()

tables := odps.NewTables(odpsIns, "project", "")
err := tables.Create(schema, true, nil, nil)
```

</TabItem>
</Tabs>

`TruncTime.DatePart` 支持的粒度：`YEAR`、`MONTH`、`DAY`、`HOUR`。

## 外部表

外部表用于连接外部数据源（如 OSS）：

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

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

</TabItem>
<TabItem value="python" label="Python">

```python
from odps.models import TableSchema

schema = TableSchema.from_lists(['c1', 'c2'], ['string', 'string'])

odps.create_table(
    'external_table', schema,
    storage_handler='com.aliyun.odps.udf.example.text.TextStorageHandler',
    location='oss://bucket/path/to/data/',
    resources='odps-udf-example.jar',
    serde_properties={
        'odps.text.option.delimiter': '|',
        'odps.text.option.encoding': 'UTF-8',
    },
    project='project',
)
```

</TabItem>
<TabItem value="go" label="Go">

```go
schema := tableschema.NewSchemaBuilder().
    Name("external_table").
    Column(tableschema.Column{Name: "c1", Type: datatype.StringType}).
    Column(tableschema.Column{Name: "c2", Type: datatype.StringType}).
    StorageHandler("com.aliyun.odps.udf.example.text.TextStorageHandler").
    Location("oss://bucket/path/to/data/").
    Build()

serdeProperties := map[string]string{
    "odps.text.option.delimiter": "|",
    "odps.text.option.encoding":  "UTF-8",
}
jars := []string{"odps-udf-example.jar"}

tables := odps.NewTables(odpsIns, "project", "")
err := tables.CreateExternal(schema, false, serdeProperties, jars, nil, nil)
```

</TabItem>
</Tabs>

## SQL 预览

在执行创建前，可以通过 `getSQL()` 方法预览将要执行的 DDL 语句：

:::info 此功能仅 Java SDK 支持。:::

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
