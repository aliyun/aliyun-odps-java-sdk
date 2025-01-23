---
title: 执行建表操作
sidebar_label:  执行建表操作
sidebar_position: 3
---

# 执行建表操作

`Tables.TableCreator` 是一个便捷的建表工具，旨在简化和标准化表创建过程中所需的各项步骤。
在使用 `odps-sdk-java` 的过程中，用户可以通过 `TableCreator` 类来更高效地执行建表操作。

下面我们将依次详细介绍如何使用 `TableCreator` 类，包括如何获取对象、构建表结构、指定参数等。

## 获取 TableCreator 对象

使用 `TableCreator` 前，您需要先获取到 `Odps` 实例，获取`Odps`示例的方法参考[构建 ODPS 客户端](./init-odps-client.md)。
同时你必须提供希望创建表所在的表名，以及表的模式（即 `TableSchema`），如何构建`TableSchema`可以参考本文[构建表结构](#构建表结构)。

以下是获取 `TableCreator` 对象的示例：

```java
Odps odps = new Odps(...); // 创建 Odps 实例
String projectName = "your_project"; // 项目名称
String tableName = "your_table"; // 表名称
TableSchema tableSchema = new TableSchema(...); // 表结构定义

Tables.TableCreator tableCreator =
    odps.tables().newTableCreator(projectName, tableName, tableSchema);
```
也可以不指定项目名，将在`Odps.defaultProject`中创建表。
```java
Tables.TableCreator tableCreator = odps.tables().newTableCreator(tableName, tableSchema);
```

## 构建表结构

`TableSchema` 是描述表结构的核心对象，包括列名、数据类型及其其他特性。创建 `TableSchema` 对象通常需要定义多个 `Column` 对象，如何构建`Column`可以参考本文[构造列对象](#构造列对象)。

`TableSchema` 主要包含表的数据列和分区列信息，以下是构建 `TableSchema` 示例：
```java
Column dataColumn1 = new Column(...);
Column dataColumn2 = new Column(...);
List<Column> moreDataColumns = List.of(...);

Column partitionColumn1 = new Column(...);
Column partitionColumn2 = new Column(...);

TableSchema tableSchema = TableSchema.builder()
    .withColumn(dataColumn1)
    .withColumn(dataColumn2)
    .withColumns(moreDataColumns)
    .withPartitionColumn(partitionColumn1)
    .withPartitionColumn(partitionColumn2)
    .build();
```

同时`TableSchema`还拥有几个简单的方法，来加速简单表结构的构建，如：
```java
TableSchema tableSchema = TableSchema.builder()
    .withBigintColumn("bigint")
    .withStringColumn("string")
    .withDoubleColumn("double")
    .withDecimalColumn("decimal")
    .withDatetimeColumn("datetime")
    .withBooleanColumn("boolean")
    .build();
```

## 构造列对象

`Column` 对象用于定义每个列的属性，包括列名和数据类型和其他特性。可以使用 `Column.newBuilder` 方法来构建列对象，示例如下：

```java
Column idColumn = Column.newBuilder("id", TypeInfoFactory.BIGINT).build();
Column nameColumn = Column.newBuilder("name", TypeInfoFactory.STRING).build();
```
`TypeInfoFactory` 是一个工厂类，用于创建不同类型的 `TypeInfo` 对象，如 `BIGINT`、`STRING`、`DOUBLE` 等。
支持 MaxCompute 全部支持的列类型，详情参考[数据类型](https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-v2-0-data-type-edition)。

`ColumnBuilder` 额外支持配置列的属性，如：
- **设置列是否为非空**：`notNull` 方法
- **设置列的注释**：`withComment` 方法
- **设置列的默认值**：`withDefaultValue` 方法
- **设置列的生成表达式**：`withGenerateExpression` 方法（仅分区列生效）

## 指定建表参数
`TableCreator` 提供了多种方法来灵活配置建表参数。以下是 `TableCreator` 类中可用的主要配置选项：

### 表的基本信息配置
- **withSchemaName(String schemaName)**：设置表所在 Schema 的名称（三层模型）。
- **withComment(String comment)**：为表添加描述信息。
- **ifNotExists()**：设置在表已存在的情况下不抛出异常，而是忽略创建请求。
- **withHints(Map hints)**：所执行的 SQL 任务的 hints。
- **withAliases(Map aliases)**：指定 SQL 语句中的别名。

### 表类型配置
- **virtualView()**：将表设置为虚拟视图。仅支持 `create view as` 语句。
- **transactionTable()**：将表设置为事务表。
- **deltaTable()**：将表设置为 Delta 表。Delta 表必须配置主键信息，主键列必须`NotNull`。
- **externalTable()**：将表设置为外部表。

### 表属性和选项配置
- **withLifeCycle(Long lifeCycle)**：设置表的生命周期。
- **withTblProperties(Map tblProperties)**：设置用户自定义的表属性。
- **autoPartitionBy(GenerateExpression expression, String aliasName)**：创建 auto-partition 表，需要指定表的生成表达式和分区列名。该方法将使用指定分区信息，覆盖当前 TableSchema 中的分区信息。

### 聚簇表相关配置
- **withClusterInfo(Table.ClusterInfo clusterInfo)**：为表配置聚簇信息，用来创建 Hash/Range Cluster 表。

### Delta 表相关配置
- **withPrimaryKeys(List primaryKeys)**：指定表的主键列，仅`Delta Table`具有限定 distinct 作用。
- **withDeltaTableBucketNum(Integer bucketNum)**：为 Delta 表设置桶的数量。

### 外表相关配置
- **withStorageHandler(String storageHandler)**：按照外部表数据格式指定StorageHandler。
- **withResources(List usingResources)**：指定使用的外部资源。
- **withLocation(String location)**：外部表数据存储位置。
- **withSerdeProperties(Map serdeProperties)**：外部表的授权、压缩、字符解析等相关参数。

### 视图相关配置
- **withSelectStatement(String selectStatement)**：`create view as`/`create table as` 语句的 select cause。

### 其他配置
- **withDataHubInfo(DataHubInfo dataHubInfo)**：创建 DataHub 表的相关信息。
- **debug()**：启用调试模式，输出执行日志到控制台。

### SQL 生成和执行
- **getSQL()**：生成用于创建表的 SQL 语句。
- **create()**：执行创建表操作，实际在 ODPS 上创建表。

这些方法可以组合使用，以便根据具体的用例构建复杂的表定义。 通过灵活使用这些方法，您可以根据业务需求定制表的创建。

## 示例

完成所有配置后，可以通过 `getSQL` 方法获取创建表的 SQL 语句，或者直接调用 `create` 方法来执行建表操作：

### 创建分区表
分区表是用于优化大数据的存储和查询性能的一种表结构，适合存储海量数据。通过为表设置分区列，可以有效地提高查询效率和管理数据。
以下示例演示如何创建一个分区表，定义 p_date 列作为分区列，并在表中添加其他数据列。
```java
public void createPartitionTable() throws OdpsException {
    TableSchema schema = TableSchema.builder()
        .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
        .withColumn(Column.newBuilder("c2", TypeInfoFactory.STRING).build())
        .withPartitionColumn(Column.newBuilder("p_date", TypeInfoFactory.STRING).build())
        .build();
    odps.tables().newTableCreator("testProject", "testCreatePartitionTable", schema)
        .ifNotExists()
        .create();
}
```

### 创建 Delta 表
Delta 表是一种用于处理事务性数据的表类型，具有高性能和一致性保证。
下列示例将创建一个 Delta 表，以 BIGINT 类型 'pk' 作为主键，其他列类型为 STRING，分桶数量设置为 16。
```java
public void createDeltaTable() throws OdpsException {
    TableSchema schema = TableSchema.builder()
        .withColumn(Column.newBuilder("pk", TypeInfoFactory.BIGINT).notNull().build())
        .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
        .withColumn(Column.newBuilder("c2", TypeInfoFactory.STRING).build())
        .withColumn(Column.newBuilder("c3", TypeInfoFactory.STRING).build())
        .build();

    odps.tables().newTableCreator("testProject", "testCreateDeltaTable", schema)
        .deltaTable()
        .withDeltaTableBucketNum(16)
        .withPrimaryKeys(ImmutableList.of("pk"))
        .ifNotExists()
        .create();
}
```

### 创建 Transaction 表
Transaction 表是一种用于处理事务性数据的表类型。
下列示例将创建一个 Transaction 表，使用 struct 类型作为列类型
```java
public void createStructTransactionalTable() throws OdpsException {
    StructTypeInfo structTypeInfo = TypeInfoFactory.getStructTypeInfo(
        ImmutableList.of("end"), ImmutableList.of(TypeInfoFactory.STRING));
    
    TableSchema schema = TableSchema.builder()
        .withColumn(Column.newBuilder("c1", structTypeInfo).build()).build();

    odps.tables().newTableCreator("testProject", "testCreateStructTransactionalTable", schema)
            .transactionTable()
            .ifNotExists()
            .create();
}
```

### 创建 auto-partition 表
auto-partition 表是一种用于自动生成分区的表类型，当用户不指定分区信息时，系统会根据指定的生成表达式自动生成分区。
下列示例将创建一个 auto-partition 表，其中 `p1` 列使用 `TruncTime` 生成表达式，根据 `c1` 列生成，`c1` 列存储日期时间数据。
```java
public void createAutoPartitionTable() throws OdpsException {
    Column autoParitionCol = Column.newBuilder("p1", TypeInfoFactory.STRING)
        .withGenerateExpression(new TruncTime("c1", TruncTime.DatePart.DAY))
        .build();
    TableSchema schema = TableSchema.builder()
        .withColumn(Column.newBuilder("c1", TypeInfoFactory.DATETIME).build())
        .withPartitionColumn(autoPar)
        .build();
    odps.tables().newTableCreator("testProject", "testCreateAutoPartitionTable", schema)
        .ifNotExists()
        .create();
}
```
### 创建 Cluster 表
Cluster 表是用于分布式处理的表，这种表可以通过指定的列进行哈希分区/Row-Range分区，从而实现负载均衡。
通过在创建表时设置 `CLUSTERED BY` 语句，可以增强数据的存取效率。下面的示例代码演示如何创建一个 Hash Cluster 表，选择 `c1` 和 `c2` 两个列作为分区依据，并定义排序方式。

```java
public void createClusterTable() throws OdpsException {
    Table.ClusterInfo clusterInfo =
        new Table.ClusterInfo(Table.ClusterInfo.ClusterType.HASH, 
                              ImmutableList.of("c1", "c2"), // clustered by 列
                              ImmutableList.of( // 桶内排序列
                                  new Table.SortColumn("c1", Table.SortColumn.Order.DESC),
                                  new Table.SortColumn("c2", Table.SortColumn.Order.ASC)), 
                              10); // 桶数量

    odps.tables().newTableCreator("testProject", "testCreateClusterTable",
                                  TableSchema.builder()
                                      .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
                                      .withColumn(Column.newBuilder("c2", TypeInfoFactory.STRING).build())
                                      .build())
        .withClusterInfo(clusterInfo).create();
}
```

### 创建外表
外表用于连接数据仓库外部的数据源，比如OSS存储或其他数据源。
以下示例展示了如何创建一个外表，指定它的数据存储处理器以及其它相关参数，如位置和序列化属性。

```java
public void testCreateExternalTable() throws OdpsException {
    odps.tables().newTableCreator("testProject", "testCreateExternalTable",
            TableSchema.builder()
                .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
                .withColumn(Column.newBuilder("c2", TypeInfoFactory.STRING).build())
                .build())
        .externalTable()
        .withStorageHandler("com.aliyun.odps.udf.example.text.TextStorageHandler")
        .withResources(ImmutableList.of("odps-udf-example.jar", "another.jar"))
        .withLocation("oss://path/to/your/external/data/location/")
        .withSerdeProperties(
            ImmutableMap.of("odps.text.option.delimiter", "|", "my.own.option", "value"))
        .create();
}
```

### 创建虚拟视图
虚拟视图是一种用于查询数据源的视图，可以简化数据查询。
以下示例展示了如何创建一个虚拟视图，指定它的查询语句。
```java
public void testCreateView() throws OdpsException {
    odps.tables().newTableCreator("testProject", "testCreateView",
            TableSchema.builder()
                .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
                .withColumn(Column.newBuilder("c2", TypeInfoFactory.STRING).build())
                .build())
        .view()
        .withSelectStatement("select c1, c2 from testProject.testCreateView")
        .create();
}
```

### 创建自定义 TblProperties 表（如 Append 2.0 表）
除了 SDK 预定义的表类型外，用户也可以自行指定建表时的 Tblproperties。如下以 Append 2.0 表为例（指定 `transactional=true` 和 `table.format.version=2`)

```java
public void testCreateTableWithTblProperties() throws OdpsException {
    // some table properties
    Map<String, String> tblProperties = new HashMap<>();
    tblProperties.put("transactional", "true");
    tblProperties.put("table.format.version", "2");
    
    odps.tables().newTableCreator("testProject", "testCreateTableWithTblProperties",
            TableSchema.builder()
                .withColumn(Column.newBuilder("c1", TypeInfoFactory.STRING).build())
                .withColumn(Column.newBuilder("c2", TypeInfoFactory.STRING).build())
                .build())
        .withTblProperties(tblProperties)
        .create();
}
```
