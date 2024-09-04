---
title: Table
sidebar_position: 2
---

# Table

`Table` 类代表ODPS中的表，可以用于进行表操作，比如表的查询、分区操作等。下面是`Table` 类的使用说明。

## 概览

- [创建表实例对象](#创建表实例对象)
- [表基本信息](#表基本信息)
- [表数据操作](#表数据操作)
- [分区操作](#分区操作)
- [标签操作](#标签操作)
- [表扩展信息](#表扩展信息)
- [物化视图操作](#物化视图操作)
- [视图操作](#视图操作)
- [外部表操作](#外部表操作)

## 创建表实例对象

要操作表，首先需要创建一个表的实例对象。

注意，获取表实例是一个lazy操作，即只有当调用`Table` 类的其他方法时，才会真正获取表的元数据信息。同时，只有表真实存在，才能获取到表实例。
```java
Table table = odps.tables().get("table_project", "table_name");
```
当开启[Schema功能](https://help.aliyun.com/zh/maxcompute/user-guide/schemas)后，可以传入schema参数，来获取指定Table实例。
```java
Table table = odps.tables().get("table_project", "schema_name", "table_name");
```

## 表基本信息
`Table`作为一个实现`lazyload`功能的对象，只有在调用以下方法时，才会进行网络通信，获取真正的元数据信息。
同时，我们也可以手动调用`reload()`方法，来达到同样的效果。
```java
table.reload();
```
除非手动触发`reload()`方法，否则Table在`reload`一次之后默认使用缓存。


### 获取表名

```java
String tableName = table.getName();
```

### 获取表结构
```java
// 结构化数据
TableSchema tableSchema = table.getSchema();
// json类型数据
String jsonSchema = table.getJsonSchema();

tableSchema.getColumns(); // 获取data列（不包含分区列）
tableSchema.getPartitionColumns(); // 获取分区列
```
### 获取表所在项目名

```java
String projectName = table.getProject();
```

### 获取表所在Schema名
需要开启[Schema功能](https://help.aliyun.com/zh/maxcompute/user-guide/schemas)，才能正确获取该字段。

```java
String schemaName = table.getSchemaName();
```


### 获取表注释

```java
String comment = table.getComment();
```

### 获取表所属用户

```java
String owner = table.getOwner();
```

### 获取表类型

```java
Table.TableType tableType = table.getType();
```
目前表类型分被为以下四类
```java
public enum TableType {
    /**
     * 常规maxcompute(odps)表
     */
    MANAGED_TABLE,
    /**
     * 视图
     * https://help.aliyun.com/zh/maxcompute/user-guide/view-related-operations
     */
    VIRTUAL_VIEW,
    /**
     * 外表
     * https://help.aliyun.com/zh/maxcompute/user-guide/external-data-sources
     */
    EXTERNAL_TABLE,
    /**
     * 物化视图 
     * https://help.aliyun.com/zh/maxcompute/user-guide/materialized-views
     */
    MATERIALIZED_VIEW
}
```
衍生表类型判断方法
```java
table.isVirtualView();
table.isMaterializedView();
table.isExternalTable();
```

### 获取创建时间

```java
Date createdTime = table.getCreatedTime();
```

### 获取最后修改时间

```java
Date lastMetaModifiedTime = table.getLastMetaModifiedTime();
```

### 获取表存储大小
注意：此方法通常不保证和实际占用存储大小相同。单位为`bytes`
```java
long size = table.getSize();
```

### 获取表数据数量
注意：此方法当无准确数据时，返回-1
```java
long recordNum = table.getRecordNum();
```

### 获取表生命周期
单位为天
```java
long lifeCycle = table.getLife();
```
### 获取Shard
```java
Shard shard = table.getShard();
```

## 表数据操作

### 读取表数据
当仅需要对数据进行预览操作（读取少量数据）时，可以使用`Table.read()`方法来进行操作。
对于大量数据的操作，应当使用`Tunnel`。


Table.read 方法用于读取表内的数据。用户可以通过不同的重载版本来读取全表数据、指定分区的数据、指定列的数据，以及设置数据读取的行数限制和时区。

方法签名
```java
public RecordReader read(int limit) throws OdpsException
public RecordReader read(PartitionSpec partition, List<String> columns, int limit) throws OdpsException
public RecordReader read(PartitionSpec partition, List<String> columns, int limit, String timezone) throws OdpsException
```

参数说明
- `limit`: 最多读取的记录行数。如果小于0，将抛出异常。
- `partition`: 表的分区（PartitionSpec 对象）。如果不指定分区，则传入 null。
- `columns`: 所要读取的列名列表。如果读取全表，则传入 null。
- `timezone`: 设置 datetime 类型数据的时区字符串，如 "Asia/Shanghai"。如果不设置，则使用默认时区。

注意
- 读取数据时，最多返回 1W 条记录，若超过，数据将被截断。
- 读取的数据大小不能超过 10MB，否则将抛出异常。

使用示例
```java
Table table = ...; // 获取Table对象的代码
try {
    RecordReader reader = table.read(partition, columns, 1000, "Asia/Shanghai");
    Record record;
    while ((record = reader.read()) != null) {
        // 处理每一条记录
    }
} catch (OdpsException e) {
    // 异常处理
} catch (IOException e) {
    // IO异常处理
}
```
### 删除表数据

```java
table.truncate();
```

## 分区操作

### 获取分区

```java
Partition partition = table.getPartition(partitionSpec);
```

### 获取所有分区

```java
List<Partition> partitions = table.getPartitions();
```

### 判断分区是否存在

```java
boolean exists = table.hasPartition(partitionSpec);
```

### 创建分区

```java
table.createPartition(partitionSpec);
```

### 删除分区

```java
table.deletePartition(partitionSpec);
```

## 标签操作
MaxCompute(ODPS) 支持对表、字段进行打标操作。

通过标签，可以基于不同的业务场景对数据做标识(Tagging), 使业务能够基于Tagging合理使用数据（如数据访问控制，数据血缘追踪）。

### 获取标签

```java
// 表级别
List<Tag> tags = table.getTags();
// 字段级别
List<Tag> tags = table.getTags("columnName");
```

### 添加标签
注意：表和标签应当属于同一个`project`
```java
// 表级别
table.addTag(tag);
// 字段级别
table.addTag(tag, List.of("column1", "column2"));
```

### 删除标签

```java
table.removeTag(tag);
```

### 获取简单标签

```java
// 表级别
Map<String, Map<String, String>> simpleTags = table.getSimpleTags();
// 字段级别
Map<String, Map<String, String>> simpleTags = table.getSimpleTags("columnName");
```

### 添加简单标签

```java
// 表级别
table.addSimpleTag(category, key, value);
// 字段级别
table.addSimpleTag(category, key, value, List.of("column1", "column2"));
```

### 删除简单标签

```java
// 表级别
table.removeSimpleTag(category, key, value);
// 字段级别
table.removeSimpleTag(category, key, value, List.of("column1", "column2"));
```

## 表扩展信息
这部分信息也属于`lazyload`操作，会在第一次调用此类方法时获取数据，然而，无法通过手动调用`reload()`方法刷新。
因为这部分信息通常随着table的创建而确定，或变化较少。

### 查看扩展信息的保留字段
返回值为Json字符串
```java
String reserved = table.getReserved();
```

### 查看表是否进行过归档（archive）操作
返回true表示进行过归档（archive）操作，false表示未进行过
```java
table.isArchived();
```

### 查看表是否事务化（transactional）
返回true表示事务化（transactional），false反之
```java
table.isTransactional();
```

### 查看表所占磁盘的物理大小
注意：此类方法通常为估计值，不保证准确性
```java
long physicalSize = table.getPhysicalSize();
```

### 查看表占用文件数
注意：此类方法通常为估计值，不保证准确性
```java
long fileNum = table.getFileNum();
```

### 查看cluster range 表的 cluster 信息
```java
ClusterInfo clusterInfo = table.getClusterInfo();
```

## 物化视图操作
### 查看是否能够应用物化视图重写
```java
boolean flag = table.isMaterializedViewRewriteEnabled();
```
### 查看物化视图是否过期
```java
table.isMaterializedViewOutdated();
```


## 视图操作
### 获取视图的文本内容
```java
table.getViewText();
table.getViewExpandedText();
```

## 外部表操作

### 外部表数据存储位置
```java
String location = table.getLocation();
```

### 外部表使用的资源
```java
String resources = table.getResources();
```

### 外部表数据处理句柄
```java
String storageHandler = table.getStorageHandler();
```

### 外部表序列化配置（SerDe）
```java
Map<String, String> serDeProperties = table.getSerDeProperties();
```

