---
title: Partition
sidebar_position: 4
---

# Partition

`Partition` 类代表 ODPS 中的分区，可以用于进行分区基本信息的查询操作。

## 概览

- [获取分区实例对象](#获取分区实例对象)
- [分区基本信息](#分区基本信息)
- [分区扩展信息](#分区扩展信息)

## 获取分区实例对象

要操作分区，首先需要创获取一个分区的实例对象。

> 注意，获取分区实例是一个lazy操作，即只有当调用 `Partition` 类的其他方法时，才会真正获取元数据信息。同时，只有分区真实存在，才能获取到分区实例。

```java
Partition partition = table.get(new PartitionSpec("partition_spec"));
```
## 分区基本信息
`Partition` 作为一个实现 `lazyload`功能的对象，只有在调用以下方法时，才会进行网络通信，获取真正的元数据信息。
同时，我们也可以手动调用`reload()`方法，来达到同样的效果。

```java
partitoin.reload();
```
除非手动触发`reload()`方法，否则 Partition 在`reload`一次之后默认使用缓存。


### 获取分区

```java
PartitionSpec = partition.getPartitionSpec();
```

### 获取创建时间

```java
Date createdTime = partition.getCreatedTime();
```

### 获取最后修改时间

```java
Date lastMetaModifiedTime = partition.getLastMetaModifiedTime();
```

### 获取存储大小
注意：此方法通常不保证和实际占用存储大小相同。单位为`bytes`
```java
long size = partition.getSize();
```

### 获取数据数量
注意：此方法当无准确数据时，返回-1
```java
long recordNum = partition.getRecordNum();
```

### 获取生命周期
单位为天
```java
long lifeCycle = partition.getLifeCycle();
```
## 分区扩展信息
这部分信息也属于`lazyload`操作，会在第一次调用此类方法时获取数据，然而，无法通过手动调用`reload()`方法刷新。
因为这部分信息通常随着分区的创建而确定，或变化较少。

### 查看扩展信息的保留字段
返回值为Json字符串
```java
String reserved = partition.getReserved();
```

### 查看分区是否进行过归档（archive）操作
返回true表示进行过归档（archive）操作，false表示未进行过
```java
partition.isArchived();
```

### 查看分区所占磁盘的物理大小
注意：此类方法通常为估计值，不保证准确性
```java
long physicalSize = partition.getPhysicalSize();
```

### 查看分区占用文件数
注意：此类方法通常为估计值，不保证准确性
```java
long fileNum = partition.getFileNum();
```

### 查看 Cluster 信息
```java
ClusterInfo clusterInfo = partition.getClusterInfo();
```

### 查看 CDC 信息

```java
long cdcSize = partition.getCdcSize();
long cdcRecordNum = partition.getCdcRecordNum();
```

