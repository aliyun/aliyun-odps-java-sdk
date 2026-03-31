---
title: MaxStorageClient
sidebar_position: 2
toc_max_heading_level: 4
---

## 概述

`MaxStorageClient` 是 Storage API 的客户端入口类，提供与 MaxCompute Storage 服务交互的核心方法，包括创建表读取/写入 Session、预览表数据以及 Blob 数据管理。

客户端是**线程安全**的，建议在应用中全局持有单一实例，避免频繁创建。

## 构建客户端

使用 `MaxStorageClient.builder()` 创建 Builder 并配置参数后调用 `build()` 方法构建客户端。

```java
MaxStorageClient client = MaxStorageClient.builder()
    .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
    .credentialsProvider(credentialsProvider)
    .build();
```

### 配置选项

#### `endpoint`（必选）

设置 MaxCompute 服务的 Endpoint 地址。

```java
public Builder endpoint(String endpoint)
```

**参数**：
- `endpoint`：MaxCompute 服务的 Endpoint URL，例如 `https://service.cn-hangzhou.maxcompute.aliyun.com/api`

---

#### `credentialsProvider`（推荐）

设置认证凭证提供者，用于请求签名和鉴权。

```java
public Builder credentialsProvider(ICredentialsProvider credentialsProvider)
```

**参数**：
- `credentialsProvider`：阿里云凭证提供者，支持 AccessKey、STS Token 等多种认证方式

**示例**：

```java
import com.aliyun.credentials.Client;
import com.aliyun.credentials.models.Config;

Config config = new Config();
config.setType("access_key");
config.setAccessKeyId("your_access_key_id");
config.setAccessKeySecret("your_access_key_secret");
ICredentialsProvider credentialsProvider = new Client(config);

MaxStorageClient client = MaxStorageClient.builder()
    .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
    .credentialsProvider(credentialsProvider)
    .build();
```

---

#### `project`

设置默认项目名称。

```java
public Builder project(String project)
```

**参数**：
- `project`：MaxCompute 项目名称

---

#### `tunnelEndpoint`

设置 Tunnel 服务端点，不设置时会自动从服务端获取。

```java
public Builder tunnelEndpoint(String tunnelEndpoint)
```

**参数**：
- `tunnelEndpoint`：Tunnel 服务端点 URL

---

#### `region`

设置服务所在地域。

```java
public Builder region(String region)
```

**参数**：
- `region`：地域标识，例如 `cn-hangzhou`

---

#### `quota`

设置资源配额名称，用于指定使用特定的计算配额。

```java
public Builder quota(String quota)
```

**参数**：
- `quota`：配额名称

---

#### `httpSettings`

设置 HTTP 连接配置，包括超时时间、连接池大小等。

```java
public Builder httpSettings(HttpSettings httpSettings)
```

**参数**：
- `httpSettings`：HTTP 连接配置对象

**示例**：

```java
HttpSettings httpSettings = HttpSettings.newBuilder()
    .withConnectTimeout(30)
    .withReadTimeout(120)
    .build();

MaxStorageClient client = MaxStorageClient.builder()
    .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
    .credentialsProvider(credentialsProvider)
    .httpSettings(httpSettings)
    .build();
```

---

#### `bufferAllocator`

设置 Apache Arrow 内存分配器。不设置时使用默认的 `RootAllocator`。

```java
public Builder bufferAllocator(BufferAllocator bufferAllocator)
```

**参数**：
- `bufferAllocator`：Arrow 内存分配器，用于管理列式数据的堆外内存

---

#### `userAgent`

在请求头 `User-Agent` 中追加自定义标识字符串，便于流量分析和问题排查。

```java
public Builder userAgent(String userAgent)
```

**参数**：
- `userAgent`：自定义标识，会追加在标准 SDK 标识的末尾：
  `JavaSDK/x.x.x (id:xxx; revision:xxx; java-version:x; region:xx) [userAgent]`

---

## 主要方法

### `createTableReadSessionBuilder`

创建表读取 Session 的 Builder。

```java
public TableReadSessionBuilder createTableReadSessionBuilder(TableIdentifier table)
```

**参数**：
- `table`：目标表的标识符，使用 `TableIdentifier.of(project, tableName)` 创建

**返回值**：[`TableReadSessionBuilder`](./read.md#tablereadsessionbuilder) 实例，可进一步配置后调用 `build()` 创建读取 Session

**示例**：

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "name"))
    .build();
```

---

### `createTableWriteSessionBuilder`

创建表写入 Session 的 Builder。

```java
public TableWriteSessionBuilder createTableWriteSessionBuilder(TableIdentifier table)
```

**参数**：
- `table`：目标表的标识符

**返回值**：[`TableWriteSessionBuilder`](./write.md#tablewritesessionbuilder) 实例，可进一步配置后调用 `build()` 创建写入 Session

**示例**：

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
    .withPartition(new PartitionSpec("dt='20250101'"))
    .build();
```

---

### `createInstanceReadSessionBuilder`

创建 Instance 读取 Session 的 Builder，用于读取 SQL 查询实例的结果数据。

```java
public InstanceReadSessionBuilder createInstanceReadSessionBuilder(InstanceIdentifier instance)
```

**参数**：
- `instance`：Instance 标识符，使用 `InstanceIdentifier.of(project, instanceId)` 创建

**返回值**：[`InstanceReadSessionBuilder`](./read.md#instancereadsession读取查询结果) 实例

**示例**：

```java
InstanceIdentifier instanceId = InstanceIdentifier.of("my_project", "instance_id_xxx");
InstanceReadSession session = client.createInstanceReadSessionBuilder(instanceId).build();
```

---

### `previewTable`

快速预览表中的数据，适合少量数据查看场景。

```java
public ArrowReader previewTable(TableIdentifier table, PartitionSpec partition,
                                List<String> columns, Integer limit)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | `TableIdentifier` | 目标表标识符 |
| `partition` | `PartitionSpec` | 分区规格，`null` 表示读取所有分区 |
| `columns` | `List<String>` | 列名列表，`null` 表示读取所有列 |
| `limit` | `Integer` | 最大返回行数，`null` 表示不限制 |

**返回值**：`ArrowReader`，调用方负责关闭

**示例**：

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
PartitionSpec partition = new PartitionSpec("dt='20250101'");
List<String> columns = Arrays.asList("id", "name", "score");

try (ArrowReader reader = client.previewTable(tableId, partition, columns, 100)) {
    while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        System.out.println("预览行数: " + root.getRowCount());
        // 处理数据...
    }
}
```

---

### `openBlobManager`

打开 Blob 管理器，用于读写非结构化的大对象（Blob）数据。

```java
public BlobManager openBlobManager()
```

**返回值**：[`BlobManager`](./blob.md#blobmanager-接口) 实例，详见 [Blob 数据读写](./blob.md)

---

### `close`

关闭客户端，释放底层网络连接、线程池和内存等资源。客户端不再使用时应显式关闭。

```java
public void close()
```

**示例**：

```java
// 推荐使用 try-with-resources
try (MaxStorageClient client = MaxStorageClient.builder()
        .endpoint("...")
        .credentialsProvider(credentialsProvider)
        .build()) {
    // 使用 client ...
}
```

## TableIdentifier

`TableIdentifier` 用于唯一标识一张 MaxCompute 表。

```java
// 指定 project 和表名
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");

// 指定 project、schema 和表名（支持 Schema 模型）
TableIdentifier tableId = TableIdentifier.of("my_project", "my_schema", "my_table");
```
