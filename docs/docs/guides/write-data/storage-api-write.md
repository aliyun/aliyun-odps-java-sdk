---
title: Storage API 高性能写入
description: 使用 Storage API 基于 Apache Arrow 格式进行高性能数据写入，支持批量和流式两种模式
sidebar_position: 5
module: odps-sdk-storage-api
task: write-data
apis: [MaxStorageClient, TableWriteSession, TableArrowWriter]
since: "0.52.0"
keywords: [storage-api, arrow, 高性能, batch, streaming, 列式, VectorSchemaRoot]
---

# Storage API 高性能写入

Storage API 基于 Apache Arrow 列式格式提供最高性能的数据写入能力，支持 Batch（批量）和 Streaming（流式）两种模式，适合大规模数据导入和高频实时写入场景。

## 前置条件

- 已添加 `odps-sdk-storage-api` 依赖
- 已初始化 `MaxStorageClient` 实例
- 目标表已存在
- 项目中已引入 Apache Arrow 相关依赖

```xml
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-storage-api</artifactId>
    <version>0.52.0</version>
</dependency>
```

## 完整示例

### Batch 模式（批量写入）

Batch 模式需要显式调用 `commit()` 后数据才对外可见，支持事务回滚，适合需要原子性保证的场景。

```java
import com.aliyun.odps.storage.api.MaxStorageClient;
import com.aliyun.odps.storage.api.TableIdentifier;
import com.aliyun.odps.storage.api.write.ArrowWriter;
import com.aliyun.odps.storage.api.write.TableArrowWriter;
import com.aliyun.odps.storage.api.write.TableWriteSession;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class StorageApiBatchWriteExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建 MaxStorageClient
        MaxStorageClient client = MaxStorageClient.builder()
            .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
            .credentialsProvider(credentialsProvider)
            .build();

        // 2. 指定目标表
        TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");

        // 3. 创建 TableWriteSession（Batch 模式，默认）
        try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
                .build()) {

            // 4. 创建 ArrowWriter
            try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
                TableArrowWriter arrowWriter = (TableArrowWriter) writer;

                // 5. 创建 VectorSchemaRoot 并填充数据
                try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot()) {
                    root.allocateNew();

                    VarCharVector nameVector = (VarCharVector) root.getVector("name");
                    BigIntVector ageVector = (BigIntVector) root.getVector("age");

                    for (int i = 0; i < 10000; i++) {
                        nameVector.setSafe(i, ("user_" + i).getBytes());
                        ageVector.setSafe(i, 20 + i % 50);
                    }
                    root.setRowCount(10000);

                    // 6. 写入数据批次
                    writer.writeBatch(root);
                }

                // 7. flush 数据到服务端
                writer.flush();
            }

            // 8. 提交事务，数据变为可见
            session.commit();
            System.out.println("Batch write committed successfully.");
        }
    }
}
```

### Streaming 模式（流式写入）

Streaming 模式下数据 flush 后立即可见，无需调用 `commit()`，适合实时写入场景。

```java
import com.aliyun.odps.storage.api.MaxStorageClient;
import com.aliyun.odps.storage.api.TableIdentifier;
import com.aliyun.odps.storage.api.write.ArrowWriter;
import com.aliyun.odps.storage.api.write.TableArrowWriter;
import com.aliyun.odps.storage.api.write.TableWriteSession;
import com.aliyun.odps.storage.api.write.WriteMode;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class StorageApiStreamingWriteExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建 MaxStorageClient
        MaxStorageClient client = MaxStorageClient.builder()
            .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
            .credentialsProvider(credentialsProvider)
            .build();

        // 2. 指定目标表
        TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");

        // 3. 创建 TableWriteSession（Streaming 模式）
        try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId)
                .withWriteMode(WriteMode.STREAMING)
                .build()) {

            // 4. 创建 ArrowWriter
            try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
                TableArrowWriter arrowWriter = (TableArrowWriter) writer;

                // 5. 循环写入，每批 flush 后立即可见
                for (int batch = 0; batch < 10; batch++) {
                    try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot()) {
                        root.allocateNew();

                        VarCharVector nameVector = (VarCharVector) root.getVector("name");
                        BigIntVector ageVector = (BigIntVector) root.getVector("age");

                        for (int i = 0; i < 1000; i++) {
                            nameVector.setSafe(i, ("batch_" + batch + "_user_" + i).getBytes());
                            ageVector.setSafe(i, 20 + i % 50);
                        }
                        root.setRowCount(1000);

                        writer.writeBatch(root);
                    }

                    // flush 后数据立即可见
                    writer.flush();
                    System.out.println("Batch " + batch + " flushed and visible.");
                }
            }
            // Streaming 模式无需 commit
        }
    }
}
```

## 代码说明

### 两种写入模式

| 特性 | Batch 模式 | Streaming 模式 |
|------|-----------|---------------|
| 数据可见时机 | `commit()` 后 | `flush()` 后 |
| 事务支持 | 支持（可 abort 回滚） | 不支持（flush 后不可回滚） |
| 是否需要 commit | 必须 | 无需（空操作） |
| 适用场景 | ETL、批量导入 | 实时写入、流处理 |

### Arrow VectorSchemaRoot 的创建和填充

`VectorSchemaRoot` 是 Arrow 格式的核心数据容器，代表一批列式数据：

```java
TableArrowWriter arrowWriter = (TableArrowWriter) writer;

// 创建与表 Schema 匹配的 VectorSchemaRoot
try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot()) {
    // 分配内存
    root.allocateNew();

    // 获取列向量（按列名）
    VarCharVector nameCol = (VarCharVector) root.getVector("name");
    BigIntVector ageCol = (BigIntVector) root.getVector("age");

    // 逐行填充数据
    int rowCount = 5000;
    for (int i = 0; i < rowCount; i++) {
        nameCol.setSafe(i, ("value_" + i).getBytes());
        ageCol.setSafe(i, (long) i);
    }

    // 设置实际行数
    root.setRowCount(rowCount);

    // 写入批次
    writer.writeBatch(root);
}
```

常用向量类型对应关系：

| MaxCompute 类型 | Arrow Vector 类型 |
|----------------|-------------------|
| STRING/VARCHAR | `VarCharVector` |
| BIGINT | `BigIntVector` |
| INT | `IntVector` |
| DOUBLE | `Float8Vector` |
| FLOAT | `Float4Vector` |
| BOOLEAN | `BitVector` |
| DATETIME | `TimeStampMilliVector` |
| DECIMAL | `DecimalVector` |

### 多 Writer 并行写入

同一 Session 下可创建多个独立的 Writer，通过不同的 `streamId` 标识：

```java
try (TableWriteSession session = client.createTableWriteSessionBuilder(tableId).build()) {
    int numWriters = 4;
    ExecutorService executor = Executors.newFixedThreadPool(numWriters);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < numWriters; i++) {
        final int writerIndex = i;
        futures.add(executor.submit(() -> {
            try (ArrowWriter writer = session.createWriterBuilder(
                    "stream-" + writerIndex, 1).build()) {
                TableArrowWriter arrowWriter = (TableArrowWriter) writer;
                try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot()) {
                    root.allocateNew();
                    // 填充该 Writer 负责的数据分片...
                    root.setRowCount(batchSize);
                    writer.writeBatch(root);
                }
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

### 幂等重试

`streamId` + `streamVersion` 的组合用于标识一次写入操作的唯一性。对同一批数据使用相同的 `streamId` 和 `streamVersion` 重新写入，服务端会保证只保留一份结果：

```java
// 初次写入
try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
    writer.writeBatch(root);
    writer.flush();
}

// 如果写入失败，使用相同的 streamId 和 streamVersion 重试
try (ArrowWriter writer = session.createWriterBuilder("stream-1", 1).build()) {
    writer.writeBatch(root); // 服务端保证幂等
    writer.flush();
}
```

## 配置选项

### Session 构建参数

| 方法 | 说明 |
|------|------|
| `withPartition(PartitionSpec)` | 写入指定分区 |
| `withOverwrite(boolean)` | 是否覆盖已有数据（默认 false） |
| `withWriteMode(WriteMode)` | 写入模式：BATCH（默认）/ STREAMING |
| `withSessionId(String)` | 复用已有 Session |

### Writer 构建参数

| 方法 | 默认值 | 说明 |
|------|--------|------|
| `withBufferSize(long)` | 64MB | 写入缓冲区大小（字节） |
| `withAutoFlushEnabled(boolean)` | true | 缓冲区满时是否自动 flush |
| `withExecutorService(ExecutorService)` | - | 异步 flush 线程池 |

### 异步 Flush

通过设置线程池，可以实现写入和网络发送的并行化，提升吞吐：

```java
ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
ArrowWriter writer = session.createWriterBuilder("stream-1", 1)
    .withExecutorService(flushExecutor)
    .build();
```

## 注意事项

1. **VectorSchemaRoot 生命周期**：调用方负责管理 `VectorSchemaRoot` 的关闭，必须使用 try-with-resources 或手动 `close()` 防止内存泄漏。
2. **writeBatch 后可复用**：`writeBatch()` 方法内部会立即序列化数据，调用返回后 `VectorSchemaRoot` 可安全修改或重用。
3. **Batch 模式必须 commit**：Batch 模式下如果 Session 关闭前未调用 `commit()`，会自动执行 `abort()` 丢弃所有数据。
4. **Streaming 模式不可回滚**：Streaming 模式下 flush 后的数据无法撤回。
5. **streamId 唯一性**：同一 Session 下不同 Writer 应使用不同的 `streamId`。
6. **分区列不需写入**：写入分区表时，分区列不包含在写入数据中，通过 `withPartition()` 指定。

## 相关文档

- [Storage API 写入 API 参考](/docs/api-reference/storage-api/write)
- [Storage API Client 参考](/docs/api-reference/storage-api/client)
- [Tunnel Upload 批量写入](./tunnel-upload.md)
- [Tunnel Stream 流式写入](./tunnel-stream.md)
- [Tunnel Upsert 更新插入](./tunnel-upsert.md)
