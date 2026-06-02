---
title: MaxStorageClient
description: Storage API 客户端入口类，提供表读写 Session 创建和数据预览
sidebar_position: 5
module: odps-sdk-core
apis: [MaxStorageClient]
keywords: [MaxStorageClient, Storage API, Arrow, 读取, 写入]
---

# MaxStorageClient

`MaxStorageClient` 是 Storage API 的客户端入口类，提供创建表读取/写入 Session、预览表数据和 Blob 数据管理功能。客户端是**线程安全**的，建议全局持有单一实例。

## 获取实例

使用 Builder 模式构建：

```java
MaxStorageClient client = MaxStorageClient.builder()
    .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
    .credentialsProvider(credentialsProvider)
    .project("my_project")
    .build();
```

### Builder 配置

| 方法 | 参数类型 | 必需 | 说明 |
|------|----------|------|------|
| `endpoint(String)` | String | 是 | MaxCompute 服务 Endpoint |
| `credentialsProvider(ICredentialsProvider)` | ICredentialsProvider | 推荐 | 认证凭证提供者 |
| `project(String)` | String | 否 | 默认项目名称 |
| `tunnelEndpoint(String)` | String | 否 | Tunnel 端点（不设置自动获取） |
| `region(String)` | String | 否 | 地域标识，如 `cn-hangzhou` |
| `quota(String)` | String | 否 | 资源配额名称 |
| `httpSettings(HttpSettings)` | HttpSettings | 否 | HTTP 连接配置 |
| `bufferAllocator(BufferAllocator)` | BufferAllocator | 否 | Arrow 内存分配器 |
| `userAgent(String)` | String | 否 | 自定义 User-Agent 标识 |

## 方法列表

### createTableReadSessionBuilder

创建表读取 Session 的 Builder。

```java
public TableReadSessionBuilder createTableReadSessionBuilder(TableIdentifier table)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | TableIdentifier | 目标表标识符 |

**返回值**：`TableReadSessionBuilder` 实例

**示例**：

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "name"))
    .build();
```

---

### createTableWriteSessionBuilder

创建表写入 Session 的 Builder。

```java
public TableWriteSessionBuilder createTableWriteSessionBuilder(TableIdentifier table)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | TableIdentifier | 目标表标识符 |

**返回值**：`TableWriteSessionBuilder` 实例

**示例**：

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
    .withPartition(new PartitionSpec("dt='20250101'"))
    .build();
```

---

### createInstanceReadSessionBuilder

创建 Instance 读取 Session 的 Builder，用于读取 SQL 查询结果。

```java
public InstanceReadSessionBuilder createInstanceReadSessionBuilder(InstanceIdentifier instance)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `instance` | InstanceIdentifier | Instance 标识符 |

**示例**：

```java
InstanceIdentifier instanceId = InstanceIdentifier.of("my_project", "instance_id_xxx");
InstanceReadSession session = client.createInstanceReadSessionBuilder(instanceId).build();
```

---

### previewTable

快速预览表数据。

```java
public ArrowReader previewTable(TableIdentifier table, PartitionSpec partition,
                                List<String> columns, Integer limit)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | TableIdentifier | 目标表标识符 |
| `partition` | PartitionSpec | 分区规格，null 读取全部分区 |
| `columns` | List\<String\> | 列名列表，null 读取全部列 |
| `limit` | Integer | 最大返回行数，null 不限制 |

**返回值**：`ArrowReader`（调用方负责关闭）

**示例**：

```java
try (ArrowReader reader = client.previewTable(tableId, null, null, 100)) {
    while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // 处理数据
    }
}
```

---

### openBlobManager

打开 Blob 管理器，用于读写非结构化大对象数据。

```java
public BlobManager openBlobManager()
```

**返回值**：`BlobManager` 实例

---

### close

关闭客户端，释放网络连接、线程池和内存资源。

```java
public void close()
```

**示例**：

```java
try (MaxStorageClient client = MaxStorageClient.builder()
        .endpoint("...")
        .credentialsProvider(credentialsProvider)
        .build()) {
    // 使用 client
}
```

## TableIdentifier

用于唯一标识一张 MaxCompute 表。

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
// 三层模型
TableIdentifier tableId = TableIdentifier.of("my_project", "my_schema", "my_table");
```
