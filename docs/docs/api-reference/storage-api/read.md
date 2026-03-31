---
title: 读取数据
sidebar_position: 3
toc_max_heading_level: 4
---

## 概述

Storage API 的读取流程分为三个阶段：

1. 通过 `TableReadSessionBuilder` 配置读取参数，调用 `build()` 创建 `TableReadSession`
2. 从 `TableReadSession` 获取 `InputSplit` 列表，每个 Split 代表一个可独立并行读取的数据分片
3. 对每个 Split 使用 `TableReaderBuilder` 创建 `ArrowReader`，循环读取 Arrow 格式的数据批次

客户端通过 [`MaxStorageClient.createTableReadSessionBuilder()`](./client.md#createtablereadsessionbuilder) 进入读取流程。如果表包含 Blob 列，请参考 [Blob 数据读写](./blob.md)。

```
MaxStorageClient
    └─ createTableReadSessionBuilder(tableId)
           └─ TableReadSessionBuilder.build()
                  └─ TableReadSession
                         ├─ getSplits() → List<InputSplit>
                         └─ createReaderBuilder(split)
                                └─ TableReaderBuilder.build()
                                       └─ ArrowReader
```

## TableReadSessionBuilder

`TableReadSessionBuilder` 用于配置读取参数，通过 `client.createTableReadSessionBuilder(tableId)` 获取。

### 配置选项

#### `withColumns` — 列裁剪

指定需要读取的数据列，减少不必要的数据传输。

```java
public TableReadSessionBuilder withColumns(List<String> requiredDataColumns)
```

**参数**：
- `requiredDataColumns`：需要读取的列名列表；不设置则读取所有列

**示例**：

```java
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "name", "score"))
    .build();
```

---

#### `withPartitionColumns` — 读取分区列

指定需要返回的分区列。

```java
public TableReadSessionBuilder withPartitionColumns(List<String> requiredPartitionColumns)
```

**参数**：
- `requiredPartitionColumns`：需要读取的分区列名列表

---

#### `withPartitions` — 分区过滤

指定仅读取特定分区，减少扫描的数据量。

```java
public TableReadSessionBuilder withPartitions(List<PartitionSpec> requiredPartitions)
```

**参数**：
- `requiredPartitions`：分区规格列表，例如 `new PartitionSpec("dt='20250101'")`

**示例**：

```java
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withPartitions(Arrays.asList(
        new PartitionSpec("dt='20250101'"),
        new PartitionSpec("dt='20250102'")
    ))
    .build();
```

---

#### `withBucketIds` — 按 Bucket 过滤

对于分桶表，指定仅读取特定 Bucket 的数据。

```java
public TableReadSessionBuilder withBucketIds(List<Integer> requiredBucketIds)
```

**参数**：
- `requiredBucketIds`：需要读取的 Bucket ID 列表

---

#### `withSplitOptions` — Split 分片配置

配置数据切分策略，控制并行度和每个 Split 的数据量。

```java
public TableReadSessionBuilder withSplitOptions(SplitOptions splitOptions)
```

**参数**：
- `splitOptions`：Split 配置对象，详见 [SplitOptions](#splitoptions)

**示例**：

```java
// 按大小分片，每个 Split 约 256MB
SplitOptions splitOptions = SplitOptions.newBuilder()
    .withSplitSize(256 * 1024 * 1024L)
    .build();

// 按行数分片，每个 Split 100万行
SplitOptions splitOptions = SplitOptions.newBuilder()
    .withSplitRowCount(1_000_000L)
    .build();

TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withSplitOptions(splitOptions)
    .build();
```

---

#### `withFilter` — 服务端谓词下推过滤

在服务端过滤数据，减少网络传输量。

```java
public TableReadSessionBuilder withFilter(String filterPredicate)
```

**参数**：
- `filterPredicate`：过滤谓词表达式字符串，例如 `"age > 18 AND city = 'Beijing'"`

**示例**：

```java
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withFilter("score >= 60 AND status = 'active'")
    .build();
```

---

#### `enableFilterFallback` — 过滤回退

当服务端不支持指定的过滤谓词时，是否回退到不过滤（读取全量数据）。

```java
public TableReadSessionBuilder enableFilterFallback(boolean enable)
```

**参数**：
- `enable`：`true` 表示允许回退；`false` 表示不支持过滤时抛出异常（默认）

---

#### `withMaxFilesPerSplit` — 每个 Split 最大文件数

控制每个 Split 包含的最大文件数量。

```java
public TableReadSessionBuilder withMaxFilesPerSplit(int maxFilesPerSplit)
```

---

#### `withIncrementalReadOptions` — 增量读取选项

配置增量读取参数，用于读取指定时间范围或版本范围内的变化数据。

```java
public TableReadSessionBuilder withIncrementalReadOptions(IncrementalReadOptions incrementalReadOptions)
```

**参数**：
- `incrementalReadOptions`：增量读取配置，详见 [IncrementalReadOptions](#incrementalreadoptions)

**示例**：

```java
IncrementalReadOptions options = IncrementalReadOptions.newBuilder()
    .withMode("timestamp")
    .withStartTimeStamp("2025-01-01 00:00:00")
    .withEndTimeStamp("2025-01-02 00:00:00")
    .build();

TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withIncrementalReadOptions(options)
    .build();
```

---

#### `withIncrementalReadEnabled` — 启用增量读取模式

```java
public TableReadSessionBuilder withIncrementalReadEnabled(boolean incrementalRead)
```

---

#### `withSessionReadyTimeout` — Session 就绪超时

设置等待 Session 进入就绪状态（INIT → NORMAL）的最大超时时间。

```java
public TableReadSessionBuilder withSessionReadyTimeout(long timeoutSeconds)
```

**参数**：
- `timeoutSeconds`：超时秒数，默认为 3600 秒（1小时），必须为正整数

---

#### `withSessionId` — 复用已有 Session

通过 Session ID 复用之前已创建的 Session，避免重复建立 Session。

```java
public TableReadSessionBuilder withSessionId(String sessionId)
```

---

#### `build` — 构建 Session

```java
public TableReadSession build()
```

**返回值**：`TableReadSession` 实例

**异常**：
- `ClientException`：Session 创建失败或超时
- `MaxStorageException`：服务端错误

---

## TableReadSession

`TableReadSession` 代表一个表读取会话，包含数据 Schema 信息和 Split 分片列表。

### 方法

#### `getSplits`

获取当前 Session 的所有 InputSplit，每个 Split 可独立并行读取。

```java
public List<InputSplit> getSplits()
```

**返回值**：`List<InputSplit>`，Split 数量由服务端根据 Split 配置决定

---

#### `getArrowSchema`

获取该 Session 数据的 Arrow Schema。

```java
public Schema getArrowSchema()
```

**返回值**：Apache Arrow `Schema` 对象

---

#### `getTableSchema`

获取该 Session 数据的 MaxCompute 表 Schema。

```java
public TableSchema getTableSchema()
```

---

#### `getId`

获取该 Session 的唯一标识符，可用于后续复用（`withSessionId`）。

```java
public String getId()
```

---

#### `createReaderBuilder`

针对指定的 InputSplit 创建 Reader Builder。

```java
public TableReaderBuilder createReaderBuilder(InputSplit split)
```

**参数**：
- `split`：要读取的数据分片，来自 `getSplits()` 的返回值

**返回值**：`TableReaderBuilder` 实例

---

## TableReaderBuilder

`TableReaderBuilder` 用于配置 Arrow Reader 的读取参数，通过 `session.createReaderBuilder(split)` 获取。

### 配置选项

#### `withMaxBatchRows` — 每批最大行数

```java
public TableReaderBuilder withMaxBatchRows(long maxBatchRows)
```

---

#### `withMaxBatchRawSize` — 每批最大原始数据大小（字节）

```java
public TableReaderBuilder withMaxBatchRawSize(long maxBatchRawSize)
```

---

#### `withSkipRowNum` — 跳过行数

```java
public TableReaderBuilder withSkipRowNum(long skipRowNum)
```

---

#### `withDataColumns` — 列选择（Read 级别）

在 Split 级别进一步指定需要读取的列。

```java
public TableReaderBuilder withDataColumns(List<String> dataColumns)
```

---

#### `build` — 构建 ArrowReader

```java
public ArrowReader build()
```

**返回值**：`ArrowReader` 实例，需要调用方关闭

---

## 并行读取示例

### 多线程并行读取

```java
MaxStorageClient client = MaxStorageClient.builder()
    .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
    .credentialsProvider(credentialsProvider)
    .build();

TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");

// 创建读取 Session，按 256MB 分片
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "name", "score"))
    .withSplitOptions(SplitOptions.newBuilder().withSplitSize(256 * 1024 * 1024L).build())
    .build();

List<InputSplit> splits = session.getSplits();
System.out.println("分片数量: " + splits.size());

// 使用线程池并行处理每个 Split
ExecutorService executor = Executors.newFixedThreadPool(splits.size());
List<Future<?>> futures = new ArrayList<>();

for (InputSplit split : splits) {
    futures.add(executor.submit(() -> {
        try (ArrowReader reader = session.createReaderBuilder(split).build()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                // 处理当前批次数据
                int rowCount = root.getRowCount();
                // ... 业务处理逻辑 ...
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }));
}

// 等待所有任务完成
for (Future<?> f : futures) {
    f.get();
}
executor.shutdown();
```

### 读取分区表

```java
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withPartitions(Arrays.asList(
        new PartitionSpec("dt='20250101'"),
        new PartitionSpec("dt='20250102'")
    ))
    .withPartitionColumns(Arrays.asList("dt"))   // 同时返回分区列
    .withColumns(Arrays.asList("id", "name"))
    .build();
```

### 服务端过滤 + 列裁剪

```java
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "name", "age"))
    .withFilter("age >= 18 AND country = 'CN'")
    .enableFilterFallback(true)   // 不支持时降级为全表扫描
    .build();
```

### 增量读取

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

---

## InstanceReadSession（读取查询结果）

除了读取表数据外，Storage API 还支持读取 SQL 查询 Instance 的结果数据。

### 使用流程

```java
// 1. 获取 Instance 标识符（通常来自 SQLTask 的结果）
String instanceId = "your_instance_id";
InstanceIdentifier instanceIdentifier = InstanceIdentifier.of("my_project", instanceId);

// 2. 创建 InstanceReadSession
InstanceReadSession session = client.createInstanceReadSessionBuilder(instanceIdentifier)
    .build();

System.out.println("结果总行数: " + session.getRecordCount());
System.out.println("结果 Schema: " + session.getArrowSchema());

// 3. 创建 Reader 并读取数据
try (ArrowReader reader = session.createReaderBuilder().build()) {
    while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // 处理结果数据...
    }
}
```

### InstanceReadSessionBuilder 配置

#### `withEnableLimit`

启用行数限制（服务端限制返回行数）。

```java
public InstanceReadSessionBuilder withEnableLimit(boolean enableLimit)
```

---

## SplitOptions

`SplitOptions` 用于控制数据分片策略。

### Builder 方法

| 方法 | 默认值 | 说明 |
|------|--------|------|
| `withSplitSize(long bytes)` | 256MB | 按字节大小分片（SIZE 模式） |
| `withSplitRowCount(long rowCount)` | — | 按行数分片（ROW_OFFSET 模式） |
| `withCrossPartition(boolean)` | `true` | 是否允许跨分区的 Split |

**示例**：

```java
// 按大小分片，每个 Split 约 128MB
SplitOptions options = SplitOptions.newBuilder()
    .withSplitSize(128 * 1024 * 1024L)
    .withCrossPartition(false)  // 不跨分区
    .build();
```

---

## IncrementalReadOptions

`IncrementalReadOptions` 用于配置增量读取范围。

### Builder 方法

| 方法 | 说明 |
|------|------|
| `withMode(String mode)` | 增量模式：`"timestamp"` 或 `"version"` |
| `withStartTimeStamp(String)` | 起始时间戳（timestamp 模式），格式：`"yyyy-MM-dd HH:mm:ss"` |
| `withEndTimeStamp(String)` | 结束时间戳（timestamp 模式） |
| `withStartVersion(long)` | 起始版本号（version 模式） |
| `withEndVersion(long)` | 结束版本号（version 模式） |

**示例**：

```java
// 按时间戳增量读取
IncrementalReadOptions options = IncrementalReadOptions.newBuilder()
    .withMode("timestamp")
    .withStartTimeStamp("2025-01-01 00:00:00")
    .withEndTimeStamp("2025-01-02 00:00:00")
    .build();

// 按版本号增量读取
IncrementalReadOptions options = IncrementalReadOptions.newBuilder()
    .withMode("version")
    .withStartVersion(100L)
    .withEndVersion(200L)
    .build();
```
