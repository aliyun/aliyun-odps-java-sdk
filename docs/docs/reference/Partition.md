---
title: Partition
description: Partition 类代表 MaxCompute 表的分区，提供分区元数据查询、存储信息和扩展属性访问。
sidebar_position: 17
module: odps-sdk-core
apis:
  - Partition.getPartitionSpec
  - Partition.getCreatedTime
  - Partition.getLastMetaModifiedTime
  - Partition.getSize
  - Partition.getRecordNum
  - Partition.getLifeCycle
  - Partition.getPhysicalSize
  - Partition.getFileNum
  - Partition.isArchived
keywords:
  - Partition
  - 分区
  - PartitionSpec
  - 分区元数据
---

# Partition

`Partition` 类代表 MaxCompute 中的表分区，用于查询分区基本信息和扩展信息。

## 获取实例

通过 Table 对象获取 Partition 实例：

```java
Partition partition = table.getPartition(new PartitionSpec("dt='20250101'"));
```

> 获取 Partition 实例是 lazy 操作，只有调用属性方法时才会加载元数据。分区必须真实存在。

## 数据加载

`Partition` 实现 `LazyLoad`，首次调用属性方法时自动加载元数据。也可手动触发：

```java
partition.reload();
```

`reload()` 后默认使用缓存，不会重复加载。

## 基本信息方法

### getPartitionSpec

获取分区规格。

```java
public PartitionSpec getPartitionSpec()
```

**返回值**：`PartitionSpec` 对象

---

### getCreatedTime

获取分区创建时间。

```java
public Date getCreatedTime()
```

---

### getLastMetaModifiedTime

获取分区元数据最后修改时间。

```java
public Date getLastMetaModifiedTime()
```

---

### getSize

获取分区存储大小。

```java
public long getSize()
```

**返回值**：存储大小（字节）

> 此值通常不保证与实际占用存储完全一致。

---

### getRecordNum

获取分区数据行数。

```java
public long getRecordNum()
```

**返回值**：数据行数；无准确数据时返回 `-1`

---

### getLifeCycle

获取分区生命周期。

```java
public long getLifeCycle()
```

**返回值**：生命周期（天）

---

## 扩展信息方法

扩展信息在首次调用时加载，无法通过 `reload()` 刷新（因为这些信息通常随分区创建而确定）。

### isArchived

查看分区是否进行过归档（archive）操作。

```java
public boolean isArchived()
```

**返回值**：`true` 已归档；`false` 未归档

---

### getPhysicalSize

获取分区所占磁盘的物理大小。

```java
public long getPhysicalSize()
```

**返回值**：物理大小（字节），为估计值

---

### getFileNum

获取分区占用文件数。

```java
public long getFileNum()
```

**返回值**：文件数量，为估计值

---

### getReserved

获取扩展信息的保留字段。

```java
public String getReserved()
```

**返回值**：JSON 字符串

---

### getClusterInfo

获取 Cluster 信息。

```java
public ClusterInfo getClusterInfo()
```

---

### getCdcSize

获取 CDC 数据大小。

```java
public long getCdcSize()
```

---

### getCdcRecordNum

获取 CDC 数据行数。

```java
public long getCdcRecordNum()
```

---

## 使用示例

```java
Table table = odps.tables().get("my_table");
Partition partition = table.getPartition(new PartitionSpec("dt='20250101'"));

// 基本信息
System.out.println("创建时间: " + partition.getCreatedTime());
System.out.println("存储大小: " + partition.getSize() + " bytes");
System.out.println("数据行数: " + partition.getRecordNum());

// 扩展信息
System.out.println("物理大小: " + partition.getPhysicalSize());
System.out.println("文件数: " + partition.getFileNum());
System.out.println("已归档: " + partition.isArchived());
```
