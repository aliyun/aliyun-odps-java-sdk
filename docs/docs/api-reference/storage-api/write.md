---
title: 写入数据
sidebar_position: 4
toc_max_heading_level: 4
---

## 概述

Storage API 支持两种写入模式：

- **Batch（批量）模式**（默认）：数据写入后需要显式调用 `commit()` 才对外可见，支持事务回滚，适合需要原子性保证的场景
- **Streaming（流式）模式**：数据调用 `flush()` 后立即可见，无需 `commit()`，适合实时写入场景

写入流程分为三个阶段：

1. 通过 `TableWriteSessionBuilder` 配置写入参数，调用 `build()` 创建 `TableWriteSession`
2. 通过 `session.createWriterBuilder(streamId, streamVersion)` 创建 `TableWriterBuilder`，调用 `build()` 获取 `ArrowWriter`
3. 使用 `ArrowWriter` 写入 Arrow 格式的数据批次，完成后调用 `flush()`；批量模式还需调用 `session.commit()`

客户端通过 [`MaxStorageClient.createTableWriteSessionBuilder()`](./client.md#createtablewritesessionbuilder) 进入写入流程。如果表包含 Blob 列，请参考 [Blob 数据读写](./blob.md)。

```
MaxStorageClient
    └─ createTableWriteSessionBuilder(tableId)
           └─ TableWriteSessionBuilder.build()
                  └─ TableWriteSession
                         └─ createWriterBuilder(streamId, streamVersion)
                                └─ TableWriterBuilder.build()
                                       └─ TableArrowWriter
                                              ├─ writeBatch(VectorSchemaRoot)
                                              └─ flush()
                         └─ commit()  // Batch 模式必须
```

## TableWriteSessionBuilder

`TableWriteSessionBuilder` 用于配置写入参数，通过 `client.createTableWriteSessionBuilder(tableId)` 获取。

### 配置选项

#### `withPartition` — 写入指定分区

设置写入目标分区。对于分区表，必须指定分区才能写入。

```java
public TableWriteSessionBuilder withPartition(PartitionSpec partitionSpec)
```

**参数**：
- `partitionSpec`：分区规格，例如 `new PartitionSpec("dt='20250101'")`

**示例**：

```java
TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
    .withPartition(new PartitionSpec("dt='20250101'"))
    .build();
```

---

#### `withOverwrite` — 覆盖写入

是否覆盖目标表或分区中的现有数据。

```java
public TableWriteSessionBuilder withOverwrite(boolean overwrite)
```

**参数**：
- `overwrite`：`true` 表示覆盖写入；`false`（默认）表示追加写入

**示例**：

```java
// 覆盖写入指定分区
TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
    .withPartition(new PartitionSpec("dt='20250101'"))
    .withOverwrite(true)
    .build();
```

---

#### `withWriteMode` — 写入模式

设置写入模式（批量或流式）。

```java
public TableWriteSessionBuilder withWriteMode(WriteMode writeMode)
```

**参数**：
- `writeMode`：写入模式枚举值：
  - `WriteMode.BATCH`（默认）：批量模式，提交后数据可见
  - `WriteMode.STREAMING`：流式模式，flush 后立即可见

**示例**：

```java
// 流式写入模式
TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
    .withWriteMode(WriteMode.STREAMING)
    .build();
```

---

#### `withSessionId` — 复用已有 Session

通过 Session ID 复用之前已创建的写入 Session。

```java
public TableWriteSessionBuilder withSessionId(String sessionId)
```

---

#### `build` — 构建 Session

```java
public TableWriteSession build()
```

**返回值**：`TableWriteSession` 实例

---

## TableWriteSession

`TableWriteSession` 代表一个写入会话，管理写入的完整生命周期（创建、写入、提交/回滚）。

### `createWriterBuilder`

创建写入 Builder，用于构建实际的 Arrow Writer。

```java
public TableWriterBuilder createWriterBuilder(String streamId, long streamVersion)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `streamId` | `String` | Stream 唯一标识符，同一 Session 下不同 Writer 应使用不同的 ID |
| `streamVersion` | `long` | Stream 版本号，必须 ≥ 1，用于幂等重试 |

**返回值**：`TableWriterBuilder` 实例

:::tip 幂等重试
`streamId` + `streamVersion` 的组合用于标识一次写入操作的唯一性，可利用此机制实现写入的幂等重试：对同一批数据，使用相同的 `streamId` 和 `streamVersion` 重新创建 Writer 并写入，服务端会保证只保留一份结果。
:::

---

### `commit`

提交写入事务，使所有已写入的数据对外可见。

```java
public void commit()
```

:::info
- **Batch 模式**：必须调用 `commit()` 才能使数据可见
- **Streaming 模式**：`commit()` 是空操作，数据已在 `flush()` 时可见
:::

**异常**：
- `ClientException`：Session 已关闭时抛出

---

### `abort`

中止写入事务，丢弃所有已写入的数据。

```java
public void abort()
```

:::info
- **Batch 模式**：调用后放弃所有未提交的数据
- **Streaming 模式**：无效操作，数据已对外可见无法回滚
:::

---

### `close`

关闭 Session，若未显式 `commit()`，则自动执行 `abort()` 丢弃数据。

```java
public void close()
```

推荐使用 `try-with-resources` 保证资源释放：

```java
try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId).build()) {
    // 写入数据 ...
    session.commit(); // 必须在 close() 前调用
}
// close() 自动调用，若未 commit() 则 abort()
```

---

### `getId`

获取 Session 的唯一标识符。

```java
public String getId()
```

---

### `getWriteMode`

获取当前 Session 的写入模式。

```java
public WriteMode getWriteMode()
```

---

## TableWriterBuilder

`TableWriterBuilder` 用于配置 Arrow Writer 的参数，通过 `session.createWriterBuilder(streamId, version)` 获取。

### 配置选项

#### `withBufferSize` — 缓冲区大小

设置写入缓冲区大小（字节），缓冲区满时触发自动 flush。

```java
public TableWriterBuilder withBufferSize(long bufferSize)
```

**参数**：
- `bufferSize`：缓冲区大小，默认为 64MB（`64 * 1024 * 1024`）

---

#### `withAutoFlushEnabled` — 自动 Flush

是否启用自动 Flush（缓冲区满时自动发送数据）。

```java
public TableWriterBuilder withAutoFlushEnabled(boolean enabled)
```

**参数**：
- `enabled`：默认为 `true`

---

#### `withExecutorService` — 异步 Flush 线程池

设置用于异步 Flush 的线程池，实现写入与网络发送的并行化。

```java
public TableWriterBuilder withExecutorService(ExecutorService executorService)
```

**参数**：
- `executorService`：线程池，建议使用 `Executors.newSingleThreadExecutor()`

**示例**：

```java
ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
ArrowWriter writer = session.createWriterBuilder("stream-1", 1)
    .withExecutorService(flushExecutor)
    .build();
```

---

#### `withBatchBlobUploadEnabled` — 批量 Blob 上传

启用批量 Blob 上传模式（即 [Blob 写入模式 A](./blob.md#写入方式二—arrow-接口tablearrowwriter)），适用于包含 Blob 字段的表。开启后，`writeBatch()` 时自动批量上传 Blob 并回填引用。

```java
public TableWriterBuilder withBatchBlobUploadEnabled(boolean enabled)
```

---

#### `build` — 构建 ArrowWriter

```java
public ArrowWriter build()
```

**返回值**：`ArrowWriter`（实际类型为 `TableArrowWriter`）

---

## TableArrowWriter

`TableArrowWriter` 是 Arrow 格式的写入器实现，继承 `ArrowWriter` 接口。

### `createVectorSchemaRoot`

创建与该 Writer 匹配的 `VectorSchemaRoot` 对象，用于填充数据。

```java
public VectorSchemaRoot createVectorSchemaRoot()
```

**返回值**：空的 `VectorSchemaRoot`，Schema 与目标表一致

:::caution
调用方负责管理 `VectorSchemaRoot` 的生命周期，必须使用 `try-with-resources` 或手动调用 `close()` 以防止内存泄漏。
:::

---

### `writeBatch`

写入一个 Arrow 数据批次到缓冲区。

```java
public void writeBatch(VectorSchemaRoot root)
```

**参数**：
- `root`：包含一批数据的 `VectorSchemaRoot`，方法返回后可安全复用

:::info 设计特性
`writeBatch()` 内部会立即将 `VectorSchemaRoot` 序列化为字节数组缓存到内存，因此调用返回后 `root` 对象可以安全地修改或重用，不会影响已缓存的数据。
:::

---

### `flush`

将缓冲区中的所有数据发送到服务端。

```java
public void flush()
```

- **Batch 模式**：数据发送到服务端，但需要 `session.commit()` 后才对外可见
- **Streaming 模式**：数据发送后立即对外可见

---

### `close`

关闭写入器，释放资源。关闭前会自动执行最后一次 `flush()`。

---

### `bytesWritten`

返回已写入的总字节数（序列化后）。

```java
public long bytesWritten()
```

---

## 写入示例

### 基础批量写入

```java
MaxStorageClient client = MaxStorageClient.builder()
    .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
    .credentialsProvider(credentialsProvider)
    .build();

TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");

try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId).build()) {
    try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
        TableArrowWriter arrowWriter = (TableArrowWriter) writer;

        // 创建 VectorSchemaRoot（与表 Schema 对应）
        try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot()) {
            // 清空并设置行数
            root.allocateNew();

            // 填充数据（以 VarCharVector 为例）
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            BigIntVector ageVector = (BigIntVector) root.getVector("age");

            for (int i = 0; i < 1000; i++) {
                nameVector.setSafe(i, ("user_" + i).getBytes());
                ageVector.setSafe(i, 20 + i % 50);
            }
            root.setRowCount(1000);

            // 写入数据
            writer.writeBatch(root);
        }
        writer.flush();
    }
    // 提交事务，数据变为可见
    session.commit();
}
```

### 分区表写入

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_partitioned_table");

// 写入指定分区（如果分区不存在，会自动创建）
try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
        .withPartition(new PartitionSpec("dt='20250101'"))
        .build()) {
    try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
        // 写入数据（分区列无需包含在写入数据中）...
        writer.flush();
    }
    session.commit();
}
```

### 覆盖写入（分区替换）

```java
try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
        .withPartition(new PartitionSpec("dt='20250101'"))
        .withOverwrite(true)   // 覆盖该分区的所有数据
        .build()) {
    // 写入数据...
    session.commit();
}
```

### 多 Writer 并行写入

同一个 Session 可以创建多个独立的 Writer，支持并行写入以提升吞吐量：

```java
try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId).build()) {
    int numWriters = 4;
    ExecutorService executor = Executors.newFixedThreadPool(numWriters);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < numWriters; i++) {
        final int writerIndex = i;
        futures.add(executor.submit(() -> {
            // 每个 Writer 使用独立的 streamId
            try (ArrowWriter writer = session.createWriterBuilder("stream-" + writerIndex, 1).build()) {
                TableArrowWriter arrowWriter = (TableArrowWriter) writer;
                // 写入分配给该 Writer 的数据分片...
                writer.flush();
            }
        }));
    }

    for (Future<?> f : futures) {
        f.get();
    }
    executor.shutdown();

    // 所有 Writer 完成后统一提交
    session.commit();
}
```

### 流式写入（Streaming 模式）

Streaming 模式适合需要数据实时可见的场景，flush 后数据立即对外可见：

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");

try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
        .withWriteMode(WriteMode.STREAMING)
        .build()) {
    try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
        TableArrowWriter arrowWriter = (TableArrowWriter) writer;

        // 循环写入，每批数据 flush 后立即可见
        while (hasMoreData()) {
            try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot()) {
                // 填充数据...
                writer.writeBatch(root);
            }
            writer.flush(); // 立即可见
        }
    }
    // Streaming 模式无需 commit
}
```

### 异常处理与重试

批量模式下，如果写入过程发生异常，Session 关闭时会自动 `abort()`，保证数据一致性：

```java
try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId).build()) {
    try {
        try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
            // 写入数据...
            writer.flush();
        }
        session.commit(); // 正常提交
    } catch (MaxStorageException e) {
        // 服务端异常，Session.close() 会自动 abort()
        throw e;
    }
    // try-with-resources 自动调用 session.close()
    // 若未 commit，则自动 abort 丢弃数据
}
```

---

## WriteMode

写入模式枚举，控制数据的可见时机。

| 枚举值 | 说明 |
|--------|------|
| `WriteMode.BATCH` | 批量模式（默认），`commit()` 后数据可见，支持事务回滚 |
| `WriteMode.STREAMING` | 流式模式，`flush()` 后数据立即可见，不支持回滚 |

```java
// 批量模式（显式指定）
.withWriteMode(WriteMode.BATCH)

// 流式模式
.withWriteMode(WriteMode.STREAMING)
```
