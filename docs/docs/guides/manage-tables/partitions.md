---
title: 分区操作
description: 使用 MaxCompute Java SDK 管理表分区，包括分区的创建、查询、删除和 PartitionSpec 的构造
sidebar_position: 4
module: odps-sdk-core
task: manage-tables
apis:
  - Table
  - Partition
  - PartitionSpec
since: "0.47.0"
keywords:
  - 分区
  - Partition
  - PartitionSpec
  - 创建分区
  - 删除分区
  - 分区列表
---

# 分区操作

本文介绍如何使用 MaxCompute Java SDK 对分区表进行分区管理，包括分区的查询、创建、删除以及 `PartitionSpec` 的构造方法。

## 前置条件

所有分区操作都基于 `Table` 实例：

```java
Odps odps = new Odps(...);
Table table = odps.tables().get("project_name", "partitioned_table");
```

## PartitionSpec 构造

`PartitionSpec` 用于描述一个具体的分区值，是分区操作的核心参数。

### 通过字符串解析构造

```java
// 单级分区
PartitionSpec spec = new PartitionSpec("dt='20240101'");

// 多级分区
PartitionSpec spec = new PartitionSpec("dt='20240101',region='cn'");
```

### 通过 key-value 逐个添加

```java
PartitionSpec spec = new PartitionSpec();
spec.set("dt", "20240101");
spec.set("region", "cn");
```

### trim 参数 (since 0.50.4)

构造 `PartitionSpec` 时，可以指定 `trim` 参数来自动去除分区值两端的空白字符：

```java
// trim=true 会去除分区值前后的空格
PartitionSpec spec = new PartitionSpec("dt=' 20240101 '", true);
// 等价于 dt='20240101'
```

## 获取单个分区

```java
Partition partition = table.getPartition(new PartitionSpec("dt='20240101'"));
```

获取到 `Partition` 对象后，可以查询该分区的详细信息：

```java
long size = partition.getSize();              // 分区存储大小（bytes）
long recordNum = partition.getRecordNum();    // 分区记录数
Date createdTime = partition.getCreatedTime(); // 分区创建时间
```

## 获取所有分区

### getPartitions - 获取完整分区信息

返回所有分区的详细信息列表，包括每个分区的大小、记录数等元数据：

```java
List<Partition> partitions = table.getPartitions();

for (Partition p : partitions) {
    System.out.println("Partition: " + p.getPartitionSpec());
    System.out.println("  Size: " + p.getSize());
    System.out.println("  Records: " + p.getRecordNum());
    System.out.println("  Created: " + p.getCreatedTime());
}
```

### getPartitionSpecs - 仅获取分区值（高性能）

与 `getPartitions()` 不同，此方法仅返回分区值，不包含分区的详细元数据信息，因此效率更高。当只需要知道有哪些分区存在时，推荐使用此方法。

```java
List<PartitionSpec> specs = table.getPartitionSpecs();

for (PartitionSpec spec : specs) {
    System.out.println("Partition spec: " + spec.toString());
}
```

> **性能提示**：当表分区数量较多时，`getPartitionSpecs()` 比 `getPartitions()` 性能显著更优，因为它不需要加载每个分区的元数据。

## 判断分区是否存在

```java
PartitionSpec spec = new PartitionSpec("dt='20240101'");
boolean exists = table.hasPartition(spec);

if (exists) {
    System.out.println("分区已存在");
}
```

## 创建分区

```java
PartitionSpec spec = new PartitionSpec("dt='20240115',region='cn'");
table.createPartition(spec);
```

带 `ifNotExists` 参数，分区已存在时不抛出异常：

```java
table.createPartition(spec, true);
```

## 删除分区

```java
PartitionSpec spec = new PartitionSpec("dt='20240101'");
table.deletePartition(spec);
```

带 `ifExists` 参数，分区不存在时不抛出异常：

```java
table.deletePartition(spec, true);
```

## 完整示例

### 批量创建分区

```java
public void batchCreatePartitions(Odps odps) throws OdpsException {
    Table table = odps.tables().get("my_project", "daily_events");

    // 批量创建最近 7 天的分区
    LocalDate today = LocalDate.now();
    for (int i = 0; i < 7; i++) {
        String dt = today.minusDays(i).format(DateTimeFormatter.BASIC_ISO_DATE);
        PartitionSpec spec = new PartitionSpec("dt='" + dt + "'");

        if (!table.hasPartition(spec)) {
            table.createPartition(spec);
            System.out.println("Created partition: " + spec);
        }
    }
}
```

### 清理过期分区

```java
public void cleanExpiredPartitions(Odps odps, int retentionDays) throws OdpsException {
    Table table = odps.tables().get("my_project", "daily_events");

    LocalDate threshold = LocalDate.now().minusDays(retentionDays);
    List<PartitionSpec> specs = table.getPartitionSpecs();

    for (PartitionSpec spec : specs) {
        String dtValue = spec.get("dt");
        LocalDate partDate = LocalDate.parse(dtValue, DateTimeFormatter.BASIC_ISO_DATE);

        if (partDate.isBefore(threshold)) {
            table.deletePartition(spec, true);
            System.out.println("Deleted expired partition: " + spec);
        }
    }
}
```

### 遍历分区统计信息

```java
public void printPartitionStats(Odps odps) throws OdpsException {
    Table table = odps.tables().get("my_project", "user_logs");

    List<Partition> partitions = table.getPartitions();
    long totalSize = 0;
    long totalRecords = 0;

    for (Partition p : partitions) {
        totalSize += p.getSize();
        totalRecords += p.getRecordNum();
    }

    System.out.println("Total partitions: " + partitions.size());
    System.out.println("Total size: " + totalSize + " bytes");
    System.out.println("Total records: " + totalRecords);
}
```
