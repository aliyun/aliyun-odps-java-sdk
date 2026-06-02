---
title: Storage API 读取
description: 使用 Storage API 高性能并行读取表数据，支持列裁剪、谓词下推、分区过滤和 Arrow 列式格式。
sidebar_position: 4
module: odps-sdk-storage-api
task: read-data
apis: [MaxStorageClient, TableReadSession, ArrowReader]
since: "0.52.0"
keywords: [Storage API, Arrow, 并行读取, 列裁剪, 谓词下推, Split]
---

# Storage API 读取

Storage API 是 MaxCompute 提供的高性能数据读取接口，基于 Apache Arrow 列式内存格式，支持列裁剪、谓词下推、分区过滤和并行分片读取，适用于大规模数据处理场景。

## 前置条件

- 添加 `odps-sdk-storage-api` 模块依赖
- 拥有目标表的读取权限
- SDK 版本 >= 0.52.0

## 完整示例

```java
import com.aliyun.odps.storage.api.MaxStorageClient;
import com.aliyun.odps.storage.api.TableIdentifier;
import com.aliyun.odps.storage.api.read.TableReadSession;
import com.aliyun.odps.storage.api.read.InputSplit;
import com.aliyun.odps.storage.api.read.SplitOptions;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.Arrays;
import java.util.List;

public class StorageApiReadExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建 MaxStorageClient
        MaxStorageClient client = MaxStorageClient.builder()
            .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
            .credentialsProvider(credentialsProvider)
            .build();

        // 2. 指定表标识
        TableIdentifier tableId = TableIdentifier.of("my_project", "user_behavior");

        // 3. 创建 TableReadSession，配置列裁剪和过滤条件
        TableReadSession session = client.createTableReadSessionBuilder(tableId)
            .withColumns(Arrays.asList("user_id", "action", "timestamp"))
            .withFilter("action = 'purchase' AND timestamp > '2025-01-01'")
            .withSplitOptions(SplitOptions.newBuilder()
                .withSplitSize(256 * 1024 * 1024L)  // 每个 Split 约 256MB
                .build())
            .build();

        // 4. 获取 Split 列表
        List<InputSplit> splits = session.getSplits();
        System.out.println("分片数量: " + splits.size());

        // 5. 逐个 Split 读取数据
        long totalRows = 0;
        for (InputSplit split : splits) {
            try (ArrowReader reader = session.createReaderBuilder(split).build()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    int rowCount = root.getRowCount();
                    totalRows += rowCount;

                    // 按列获取 Vector 进行处理
                    BigIntVector userIdVector = (BigIntVector) root.getVector("user_id");
                    VarCharVector actionVector = (VarCharVector) root.getVector("action");

                    for (int i = 0; i < rowCount; i++) {
                        long userId = userIdVector.get(i);
                        String action = new String(actionVector.get(i));
                        // 处理业务逻辑...
                    }
                }
            }
        }
        System.out.println("总读取行数: " + totalRows);
    }
}
```

## 代码说明

1. **创建客户端**：通过 `MaxStorageClient.builder()` 配置 Endpoint 和认证信息，构建客户端实例。
2. **指定表标识**：使用 `TableIdentifier.of(project, table)` 标识目标表。
3. **构建 ReadSession**：通过 Builder 配置读取参数（列裁剪、过滤条件、分片策略等），调用 `build()` 创建 Session。
4. **获取 Split 列表**：`session.getSplits()` 返回所有可并行读取的数据分片。
5. **读取 Arrow 数据**：对每个 Split 创建 `ArrowReader`，循环调用 `loadNextBatch()` 读取数据批次，通过 `VectorSchemaRoot` 访问列式数据。

## 配置选项

### TableReadSessionBuilder 参数

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `withColumns(List<String>)` | 列裁剪，指定需要读取的数据列 | 全部列 |
| `withPartitionColumns(List<String>)` | 指定需要返回的分区列 | - |
| `withPartitions(List<PartitionSpec>)` | 分区过滤，仅读取指定分区 | 全部分区 |
| `withFilter(String)` | 谓词下推，服务端过滤数据 | - |
| `enableFilterFallback(boolean)` | 不支持的过滤条件是否回退为全表扫描 | false |
| `withSplitOptions(SplitOptions)` | 分片策略配置 | 256MB/Split |
| `withBucketIds(List<Integer>)` | 按 Bucket ID 过滤（分桶表） | - |
| `withSessionReadyTimeout(long)` | Session 就绪超时（秒） | 3600 |
| `withSessionId(String)` | 复用已有 Session | - |
| `withIncrementalReadOptions(...)` | 增量读取配置 | - |
| `withIncrementalReadEnabled(boolean)` | 启用增量读取 | false |

### SplitOptions 分片策略

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `withSplitSize(long bytes)` | 按字节大小分片 | 256MB |
| `withSplitRowCount(long)` | 按行数分片 | - |
| `withCrossPartition(boolean)` | 是否允许跨分区的 Split | true |

### TableReaderBuilder 参数

| 方法 | 说明 |
|------|------|
| `withMaxBatchRows(long)` | 每批最大行数 |
| `withMaxBatchRawSize(long)` | 每批最大原始数据大小（字节） |
| `withSkipRowNum(long)` | 跳过的行数 |
| `withDataColumns(List<String>)` | Split 级别的列选择 |

## 并行读取

利用线程池对多个 Split 并行处理，充分发挥 Storage API 的吞吐优势：

```java
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class ParallelStorageApiRead {

    public static void main(String[] args) throws Exception {
        MaxStorageClient client = MaxStorageClient.builder()
            .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
            .credentialsProvider(credentialsProvider)
            .build();

        TableIdentifier tableId = TableIdentifier.of("my_project", "large_table");

        TableReadSession session = client.createTableReadSessionBuilder(tableId)
            .withColumns(Arrays.asList("id", "name", "score"))
            .withSplitOptions(SplitOptions.newBuilder()
                .withSplitSize(256 * 1024 * 1024L)
                .build())
            .build();

        List<InputSplit> splits = session.getSplits();
        System.out.println("分片数量: " + splits.size());

        ExecutorService executor = Executors.newFixedThreadPool(splits.size());
        AtomicLong totalRows = new AtomicLong(0);
        List<Future<?>> futures = new ArrayList<>();

        for (InputSplit split : splits) {
            futures.add(executor.submit(() -> {
                try (ArrowReader reader = session.createReaderBuilder(split).build()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        totalRows.addAndGet(root.getRowCount());
                        // 处理数据...
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();
        System.out.println("总读取行数: " + totalRows.get());
    }
}
```

## 读取分区表

```java
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withPartitions(Arrays.asList(
        new PartitionSpec("dt='20250101'"),
        new PartitionSpec("dt='20250102'")
    ))
    .withPartitionColumns(Arrays.asList("dt"))  // 同时返回分区列值
    .withColumns(Arrays.asList("id", "name"))
    .build();
```

## 增量读取

读取指定时间范围内的变化数据：

```java
IncrementalReadOptions options = IncrementalReadOptions.newBuilder()
    .withMode("timestamp")
    .withStartTimeStamp("2025-01-01 00:00:00")
    .withEndTimeStamp("2025-01-02 00:00:00")
    .build();

TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withIncrementalReadOptions(options)
    .withIncrementalReadEnabled(true)
    .build();
```

## 注意事项

- **Arrow 内存管理**：`VectorSchemaRoot` 中的数据在下一次 `loadNextBatch()` 调用后会被覆盖，如需保留数据请在当前批次内完成处理或拷贝。
- **资源释放**：务必使用 try-with-resources 关闭 `ArrowReader`，避免内存泄漏。
- **谓词下推限制**：并非所有表达式都支持下推，不支持时默认抛出异常；如需降级为全表扫描，请设置 `enableFilterFallback(true)`。
- **Session 复用**：对于重复读取相同数据的场景，可通过 `session.getId()` 保存 Session ID，后续用 `withSessionId()` 复用，避免重复创建。
- **Split 并行度**：Split 数量由服务端根据数据量和 SplitOptions 决定，建议线程池大小不超过 Split 数量。
- **支持 Transactional 表**：Storage API 支持读取 Transactional 表，这是相比 Tunnel 的重要优势。

## 相关文档

- [Tunnel 下载](./tunnel-download.md) - 适用于简单批量导出
- [Blob 数据下载](./blob.md) - 读取非结构化二进制数据
- [读取数据概览](./index.md) - 三种方式对比与选型
