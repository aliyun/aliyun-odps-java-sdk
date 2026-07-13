---
title: TableWriteSession
description: Storage API 写入会话，支持批量和流式两种写入模式，使用 Arrow 格式写入数据并支持事务提交。
sidebar_position: 12
module: odps-sdk-storage-api
apis:
  - TableWriteSessionBuilder.withWriteMode
  - TableWriteSessionBuilder.withPartition
  - TableWriteSessionBuilder.withOverwrite
  - TableWriteSessionBuilder.build
  - TableWriteSession.createWriterBuilder
  - TableWriteSession.commit
  - TableWriteSession.abort
  - TableWriteSession.getId
  - TableWriteSession.getWriteMode
keywords:
  - TableWriteSession
  - Storage API
  - Arrow
  - 批量写入
  - 流式写入
  - WriteMode
  - commit
---

# TableWriteSession

`TableWriteSession` 是 Storage API 的写入会话，支持以 Arrow 格式向 MaxCompute 表写入数据。提供批量（Batch）和流式（Streaming）两种写入模式。

## 获取实例

通过 `MaxStorageClient.createTableWriteSessionBuilder()` 创建 Builder 并构建：

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
    .withWriteMode(WriteMode.BATCH)
    .build();
```

## WriteMode 枚举

| 枚举值 | 说明 |
|--------|------|
| `WriteMode.BATCH` | 批量模式（默认），`commit()` 后数据可见，支持事务回滚 |
| `WriteMode.BATCH_COMPATIBLE` | 批量兼容模式，语义与 `BATCH` 一致，但使用兼容型存储布局 |
| `WriteMode.STREAMING` | 流式模式，`flush()` 后数据立即可见，不支持回滚 |
| `WriteMode.STREAMING_REALTIME` | 实时流式模式，客户端语义与 `STREAMING` 一致，但服务端采用更低延迟的实时链路 |

## TableWriteSessionBuilder

通过 `client.createTableWriteSessionBuilder(tableId)` 获取。

### withWriteMode

设置写入模式。

```java
public TableWriteSessionBuilder withWriteMode(WriteMode writeMode)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `writeMode` | `WriteMode` | `BATCH`（默认）、`BATCH_COMPATIBLE`、`STREAMING` 或 `STREAMING_REALTIME` |

---

### withPartition

设置写入目标分区。分区表必须指定。

```java
public TableWriteSessionBuilder withPartition(PartitionSpec partitionSpec)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `partitionSpec` | `PartitionSpec` | 分区规格，如 `new PartitionSpec("dt='20250101'")` |

---

### withOverwrite

是否覆盖目标表或分区中的现有数据。

```java
public TableWriteSessionBuilder withOverwrite(boolean overwrite)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `overwrite` | `boolean` | `true` 覆盖写入；`false`（默认）追加写入 |

---

### withSessionId

复用已有 Session。

```java
public TableWriteSessionBuilder withSessionId(String sessionId)
```

---

### build

```java
public TableWriteSession build()
```

**返回值**：`TableWriteSession` 实例

---

## TableWriteSession 方法

### createWriterBuilder

创建写入 Builder。

```java
public TableWriterBuilder createWriterBuilder(String streamId, long streamVersion)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `streamId` | `String` | Stream 唯一标识，同一 Session 下不同 Writer 使用不同 ID |
| `streamVersion` | `long` | Stream 版本号，必须 >= 1，用于幂等重试 |

**返回值**：`TableWriterBuilder` 实例

> `streamId` + `streamVersion` 组合用于幂等重试：相同组合重新写入时服务端只保留一份结果。

---

### commit

提交写入事务，使所有已写入数据对外可见。

```java
public void commit()
```

- **Batch 模式**：必须调用才能使数据可见
- **Streaming 模式**：空操作，数据已在 `flush()` 时可见

---

### abort

中止写入事务，丢弃所有已写入数据。

```java
public void abort()
```

- **Batch 模式**：放弃所有未提交数据
- **Streaming 模式**：无效操作

---

### close

关闭 Session。若未显式 `commit()`，自动执行 `abort()`。

```java
public void close()
```

---

### getId

```java
public String getId()
```

**返回值**：Session 唯一标识符

---

### getWriteMode

```java
public WriteMode getWriteMode()
```

**返回值**：当前 Session 的写入模式

---

## TableWriterBuilder

通过 `session.createWriterBuilder(streamId, version)` 获取。

| 方法 | 说明 |
|------|------|
| `withBufferSize(long)` | 缓冲区大小（字节），默认 64MB |
| `withAutoFlushEnabled(boolean)` | 是否启用自动 Flush，默认 `true` |
| `withExecutorService(ExecutorService)` | 异步 Flush 线程池 |
| `withBatchBlobUploadEnabled(boolean)` | 启用批量 Blob 上传 |
| `withBlobMimeType(String)` | 为批量 Blob 上传设置默认 MIME Type |
| `withExactlyOnceMode(boolean)` | 启用 Exactly-once 写入模式 |
| `withResume(boolean)` | 基于已有 stream 元数据恢复写入 |
| `withMaxPendingBuffers(int)` | 控制异步发送时允许挂起的批次数 |
| `build()` | 构建 `ArrowWriter`（实际为 `TableArrowWriter`） |

---

## TableArrowWriter

### createVectorSchemaRoot

创建与表 Schema 匹配的空 `VectorSchemaRoot`。

```java
public VectorSchemaRoot createVectorSchemaRoot()
```

---

### writeBatch

写入一个 Arrow 数据批次到缓冲区。调用返回后 `root` 可安全复用。

```java
public void writeBatch(VectorSchemaRoot root)
```

---

### flush

将缓冲区数据发送到服务端。

```java
public void flush()
```

- **Batch 模式**：`flush()` 仅把数据推送到服务端暂存区，仍需 `commit()` 才会对外可见
- **Streaming / StreamingRealtime 模式**：`flush()` 返回后数据即可见

---

### bytesWritten

返回已写入的总字节数。

```java
public long bytesWritten()
```

---

## 使用示例

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");

try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
        .withPartition(new PartitionSpec("dt='20250101'"))
        .build()) {

    try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
        TableArrowWriter arrowWriter = (TableArrowWriter) writer;
        try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot()) {
            root.allocateNew();
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            for (int i = 0; i < 1000; i++) {
                nameVec.setSafe(i, ("user_" + i).getBytes());
            }
            root.setRowCount(1000);
            writer.writeBatch(root);
        }
        writer.flush();
    }
    session.commit();
}
```
