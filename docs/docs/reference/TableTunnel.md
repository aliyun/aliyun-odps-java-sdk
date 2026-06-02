---
title: TableTunnel
description: Tunnel 服务入口类，提供表数据上传、下载和流式写入的会话创建
sidebar_position: 3
module: odps-sdk-core
apis: [TableTunnel]
keywords: [TableTunnel, Tunnel, 上传, 下载, Stream, Upsert]
---

# TableTunnel

`TableTunnel` 是 MaxCompute Tunnel 服务的入口类，用于创建数据上传、下载和流式写入会话。

## 获取实例

```java
// 方式一：通过 Odps 对象创建
TableTunnel tunnel = odps.tableTunnel();

// 方式二：直接构造
TableTunnel tunnel = new TableTunnel(odps);
tunnel.setEndpoint("http://dt.odps.aliyun.com");

// 方式三：自定义 Configuration
TableTunnel tunnel = new TableTunnel(odps, configuration);
```

## 方法列表

### setEndpoint

设置 Tunnel 服务地址。未设置时自动路由。

```java
public void setEndpoint(String endpoint)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `endpoint` | String | Tunnel 服务地址 |

---

### getConfig

获取 Tunnel 配置对象。

```java
public Configuration getConfig()
```

**返回值**：`Configuration` 对象

---

### createUploadSession

在表上创建上传会话。

```java
// 非分区表
public UploadSession createUploadSession(String projectName, String tableName) throws TunnelException
public UploadSession createUploadSession(String projectName, String tableName, boolean overwrite) throws TunnelException
public UploadSession createUploadSession(String projectName, String schemaName, String tableName, boolean overwrite) throws TunnelException

// 分区表
public UploadSession createUploadSession(String projectName, String tableName, PartitionSpec partitionSpec) throws TunnelException
public UploadSession createUploadSession(String projectName, String tableName, PartitionSpec partitionSpec, boolean overwrite) throws TunnelException
public UploadSession createUploadSession(String projectName, String schemaName, String tableName, PartitionSpec partitionSpec, boolean overwrite) throws TunnelException
```

| 参数 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `projectName` | String | 是 | 项目名称 |
| `schemaName` | String | 否 | Schema 名称（三层模型） |
| `tableName` | String | 是 | 表名称 |
| `partitionSpec` | PartitionSpec | 否 | 分区表达式（分区表必填） |
| `overwrite` | boolean | 否 | 是否覆盖已有数据，默认 false |

**示例**：

```java
UploadSession session = tunnel.createUploadSession("my_project", "my_table");
```

---

### getUploadSession

获取已有的上传会话。

```java
public UploadSession getUploadSession(String projectName, String tableName, String id) throws TunnelException
public UploadSession getUploadSession(String projectName, String tableName, PartitionSpec partitionSpec, String id) throws TunnelException
public UploadSession getUploadSession(String projectName, String tableName, String id, long shares, long shareId) throws TunnelException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `id` | String | 上传会话 ID |
| `shares` | long | 共享会话的实例总数 |
| `shareId` | long | 当前实例的唯一标识（从 0 开始） |

---

### buildDownloadSession

创建下载会话构建器（推荐方式）。

```java
public DownloadSessionBuilder buildDownloadSession(String projectName, String tableName)
```

**返回值**：`DownloadSessionBuilder` 实例

**示例**：

```java
DownloadSession session = tunnel.buildDownloadSession("my_project", "my_table")
    .setPartitionSpec(new PartitionSpec("dt=20231001"))
    .setAsyncMode(true)
    .build();
```

---

### createDownloadSession

在表上创建下载会话（已废弃，推荐使用 `buildDownloadSession`）。

```java
@Deprecated
public DownloadSession createDownloadSession(String projectName, String tableName) throws TunnelException
@Deprecated
public DownloadSession createDownloadSession(String projectName, String tableName, PartitionSpec partitionSpec) throws TunnelException
```

---

### buildStreamUploadSession

创建流式上传会话构建器。

```java
public StreamUploadSession.Builder buildStreamUploadSession(String projectName, String tableName)
```

**返回值**：`StreamUploadSession.Builder` 实例

**示例**：

```java
StreamUploadSession session = tunnel.buildStreamUploadSession("my_project", "my_table")
    .setPartitionSpec("dt=20231001")
    .build();
```

---

### buildUpsertSession

创建 Upsert 会话构建器。

```java
public UpsertSession.Builder buildUpsertSession(String projectName, String tableName)
```

**返回值**：`UpsertSession.Builder` 实例

---

### preview

数据预览，最多返回 5000 行，返回 Arrow 格式数据流。

```java
public ArrowStreamReader preview(String projectName, String schemaName, String tableName) throws TunnelException
public ArrowStreamReader preview(String projectName, String schemaName, String tableName, String partitionSpec) throws TunnelException
public ArrowStreamReader preview(String projectName, String schemaName, String tableName, String partitionSpec, Long limit) throws TunnelException
public ArrowStreamReader preview(String projectName, String schemaName, String tableName, String partitionSpec, Long limit, List<String> requiredColumns) throws TunnelException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `projectName` | String | 项目名称 |
| `schemaName` | String | Schema 名称，可为 null |
| `tableName` | String | 表名称 |
| `partitionSpec` | String | 分区表达式，可为 null |
| `limit` | Long | 最多读取行数，最大 5000 |
| `requiredColumns` | List\<String\> | 指定列名，可为 null |

**返回值**：`ArrowStreamReader`，可使用 `ArrowStreamRecordReader` 转换为 RecordReader

**示例**：

```java
ArrowStreamReader reader = tunnel.preview("my_project", null, "my_table", null, 100L);
```
