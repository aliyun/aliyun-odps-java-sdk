---
title: UpsertSession
description: UpsertSession 用于对 Transactional 表执行数据插入或更新操作，支持高并发的 Upsert/Delete 流式写入。
sidebar_position: 10
module: odps-sdk-tunnel
apis:
  - UpsertSession.Builder
  - UpsertSession.getId
  - UpsertSession.getSchema
  - UpsertSession.getStatus
  - UpsertSession.commit
  - UpsertSession.abort
  - UpsertSession.newRecord
  - UpsertSession.buildUpsertStream
  - UpsertStream.upsert
  - UpsertStream.delete
  - UpsertStream.flush
  - UpsertStream.close
keywords:
  - UpsertSession
  - UpsertStream
  - Transactional表
  - 主键表
  - upsert
  - delete
---

# UpsertSession

`UpsertSession` 用于管理对 Transactional 表（主键表）的数据插入或更新会话。通过该接口可以获取会话信息、提交或中止会话、构建 `UpsertStream` 执行写入操作。

## 获取实例

通过 `TableTunnel.buildUpsertSession()` 创建 Builder 并构建：

```java
TableTunnel tunnel = odps.tableTunnel();
UpsertSession session = tunnel.buildUpsertSession(projectName, tableName)
    .setSchemaName(schemaName)
    .setPartitionSpec(partitionSpec)
    .build();
```

## UpsertSession 方法

### getId

```java
String getId()
```

**返回值**：当前会话 ID，可用于重建会话

---

### getQuotaName

```java
public String getQuotaName()
```

**返回值**：当前使用的 Quota 名称

---

### getStatus

```java
String getStatus() throws TunnelException
```

**返回值**：会话状态码

| 状态 | 说明 |
|------|------|
| `normal` | 正常 |
| `committing` | 提交中 |
| `committed` | 已提交 |
| `expired` | 过期 |
| `critical` | 错误 |
| `aborted` | 已中止 |

---

### getSchema

```java
TableSchema getSchema()
```

**返回值**：当前会话写入的表的表结构

---

### commit

提交当前会话，使数据可见。

```java
void commit(boolean async) throws TunnelException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `async` | `boolean` | `true` 异步提交（立即返回，数据不立即可见）；`false` 同步提交 |

---

### abort

```java
void abort() throws TunnelException
```

中止当前会话，丢弃未提交数据。

---

### close

```java
void close()
```

清理客户端本地资源。

---

### newRecord

```java
Record newRecord()
```

**返回值**：`UpsertRecord` 实例，包含 upsert 操作所需的元数据隐藏列

> 执行 Upsert 操作时必须使用此方法获取 Record，不要手动创建 `ArrayRecord`。

---

### buildUpsertStream

```java
UpsertStream.Builder buildUpsertStream()
```

**返回值**：`UpsertStream.Builder` 对象

---

## Builder 接口

`UpsertSession.Builder` 用于配置和构建 `UpsertSession`。通过 `TableTunnel.buildUpsertSession()` 获取。

### 配置方法

| 方法 | 说明 |
|------|------|
| `setUpsertId(String)` | 设置已有会话 ID（用于重建） |
| `setSchemaName(String)` | 设置 Schema 名称 |
| `setPartitionSpec(PartitionSpec)` | 设置分区规格 |
| `setPartitionSpec(String)` | 设置分区规格（字符串形式） |
| `setSlotNum(long)` | 设置 Slot 数量 |
| `setCommitTimeout(long)` | 设置提交超时时间（毫秒） |
| `setNetworkThreadNum(int)` | 设置 Netty 网络 IO 线程数，默认 1 |
| `setConcurrentNum(int)` | 设置最大并发 Channel 数，默认 20，负值表示无限制 |
| `setConnectTimeout(long)` | 设置连接超时（毫秒），默认 180000 |
| `setReadTimeout(long)` | 设置响应超时（毫秒），默认 300000 |
| `setLifecycle(long)` | 设置 Session 生命周期（小时），有效范围 1-24 |

### build

```java
UpsertSession build() throws TunnelException, IOException
```

**返回值**：`UpsertSession` 实例

---

## UpsertStream

`UpsertStream` 是执行数据插入、更新、删除操作的核心接口，支持数据缓冲和批量提交。

### 获取实例

```java
UpsertStream stream = session.buildUpsertStream().build();
```

### upsert

插入或更新一条记录。

```java
void upsert(Record record) throws IOException, TunnelException
void upsert(Record record, List<String> upsertCols) throws IOException, TunnelException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `record` | `Record` | `UpsertRecord` 实例（通过 `session.newRecord()` 创建） |
| `upsertCols` | `List<String>` | 需要更新的列名列表（部分列更新） |

---

### delete

删除一条记录。

```java
void delete(Record record) throws IOException, TunnelException
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `record` | `Record` | `UpsertRecord` 实例，设置主键值即可 |

---

### flush

刷新缓冲区，将缓冲区中的数据提交到服务器。

```java
void flush() throws IOException, TunnelException
```

---

### close

关闭流，提交所有未提交的数据。

```java
void close() throws IOException, TunnelException
```

---

### reset

重置缓冲区，清空所有未提交的数据。

```java
void reset() throws IOException
```

---

## UpsertStream.Builder

通过 `session.buildUpsertStream()` 获取。

| 方法 | 说明 |
|------|------|
| `setMaxBufferSize(long)` | 设置数据缓冲最大容量（字节） |
| `setSlotBufferSize(long)` | 设置每个桶的缓冲区大小（字节） |
| `setCompressOption(CompressOption)` | 设置压缩策略 |
| `setListener(Listener)` | 设置事件监听器（用于重试逻辑） |
| `build()` | 构建 UpsertStream 实例 |

---

## 使用示例

```java
TableTunnel tunnel = odps.tableTunnel();

try (UpsertSession session = tunnel.buildUpsertSession(projectName, tableName)
        .setSchemaName(schemaName)
        .setPartitionSpec("dt='20250101'")
        .build()) {

    UpsertStream stream = session.buildUpsertStream().build();
    Record record = session.newRecord();

    // 插入/更新
    record.setString("key", "k1");
    record.setString("value", "v1");
    stream.upsert(record);

    // 删除
    record.setString("key", "k2");
    stream.delete(record);

    stream.flush();
    session.commit(false);
}
```
