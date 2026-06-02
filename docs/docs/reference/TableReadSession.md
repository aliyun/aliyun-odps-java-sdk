---
title: TableReadSession
description: Storage API 读取会话，支持列裁剪、谓词下推、分区过滤、并行分片读取 Arrow 格式数据。
sidebar_position: 11
module: odps-sdk-storage-api
apis:
  - TableReadSessionBuilder.withColumns
  - TableReadSessionBuilder.withPartitionFilter
  - TableReadSessionBuilder.withFilter
  - TableReadSessionBuilder.withSplitOptions
  - TableReadSessionBuilder.build
  - TableReadSession.getSplits
  - TableReadSession.createReaderBuilder
  - TableReadSession.getArrowSchema
  - TableReadSession.getTableSchema
keywords:
  - TableReadSession
  - Storage API
  - Arrow
  - 列裁剪
  - 谓词下推
  - 并行读取
  - InputSplit
---

# TableReadSession

`TableReadSession` 是 Storage API 的读取会话，用于从 MaxCompute 表中读取 Arrow 格式数据。支持列裁剪、谓词下推、分区过滤和并行分片读取。

## 获取实例

通过 `MaxStorageClient.createTableReadSessionBuilder()` 创建 Builder 并构建：

```java
MaxStorageClient client = MaxStorageClient.builder()
    .endpoint(endpoint)
    .credentialsProvider(credentialsProvider)
    .build();

TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "name"))
    .build();
```

## TableReadSessionBuilder

通过 `client.createTableReadSessionBuilder(tableId)` 获取。

### withColumns

指定需要读取的数据列（列裁剪）。

```java
public TableReadSessionBuilder withColumns(List<String> requiredDataColumns)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `requiredDataColumns` | `List<String>` | 列名列表；不设置则读取所有列 |

---

### withPartitionColumns

指定需要返回的分区列。

```java
public TableReadSessionBuilder withPartitionColumns(List<String> requiredPartitionColumns)
```

---

### withPartitions

指定仅读取特定分区。

```java
public TableReadSessionBuilder withPartitions(List<PartitionSpec> requiredPartitions)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `requiredPartitions` | `List<PartitionSpec>` | 分区规格列表 |

---

### withFilter

服务端谓词下推过滤，减少网络传输量。

```java
public TableReadSessionBuilder withFilter(String filterPredicate)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `filterPredicate` | `String` | 过滤表达式，如 `"age > 18 AND city = 'Beijing'"` |

---

### enableFilterFallback

当服务端不支持过滤谓词时是否回退为全量读取。

```java
public TableReadSessionBuilder enableFilterFallback(boolean enable)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `enable` | `boolean` | `true` 允许回退；`false`（默认）不支持时抛异常 |

---

### withSplitOptions

配置数据切分策略，控制并行度。

```java
public TableReadSessionBuilder withSplitOptions(SplitOptions splitOptions)
```

**SplitOptions Builder 方法**：

| 方法 | 默认值 | 说明 |
|------|--------|------|
| `withSplitSize(long bytes)` | 256MB | 按字节大小分片 |
| `withSplitRowCount(long rowCount)` | - | 按行数分片 |
| `withCrossPartition(boolean)` | `true` | 是否允许跨分区 Split |

---

### withBucketIds

对分桶表指定读取特定 Bucket。

```java
public TableReadSessionBuilder withBucketIds(List<Integer> requiredBucketIds)
```

---

### withIncrementalReadOptions

配置增量读取参数。

```java
public TableReadSessionBuilder withIncrementalReadOptions(IncrementalReadOptions options)
```

**IncrementalReadOptions Builder 方法**：

| 方法 | 说明 |
|------|------|
| `withMode(String)` | `"timestamp"` 或 `"version"` |
| `withStartTimeStamp(String)` | 起始时间，格式 `"yyyy-MM-dd HH:mm:ss"` |
| `withEndTimeStamp(String)` | 结束时间 |
| `withStartVersion(long)` | 起始版本号 |
| `withEndVersion(long)` | 结束版本号 |

---

### withSessionId

通过已有 Session ID 复用 Session。

```java
public TableReadSessionBuilder withSessionId(String sessionId)
```

---

### withSessionReadyTimeout

设置等待 Session 就绪的超时时间。

```java
public TableReadSessionBuilder withSessionReadyTimeout(long timeoutSeconds)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `timeoutSeconds` | `long` | 超时秒数，默认 3600（1小时） |

---

### withArrowOptions

设置 Arrow 序列化选项。

```java
public TableReadSessionBuilder withArrowOptions(ArrowOptions arrowOptions)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `arrowOptions` | `ArrowOptions` | Arrow 序列化配置 |

---

### withMaxFilesPerSplit

设置每个分片的最大文件数。

```java
public TableReadSessionBuilder withMaxFilesPerSplit(int maxFilesPerSplit)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `maxFilesPerSplit` | `int` | 每个 Split 包含的最大文件数 |

---

### withIncrementalReadEnabled

设置是否启用增量读取。

```java
public TableReadSessionBuilder withIncrementalReadEnabled(boolean incrementalRead)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `incrementalRead` | `boolean` | `true` 启用增量读取；`false` 禁用 |

---

### build

```java
public TableReadSession build()
```

**返回值**：`TableReadSession` 实例

**异常**：`ClientException`（创建失败或超时）、`MaxStorageException`（服务端错误）

---

## TableReadSession 方法

### getSplits

获取所有 InputSplit，每个 Split 可独立并行读取。

```java
public List<InputSplit> getSplits()
```

**返回值**：`List<InputSplit>`

---

### getArrowSchema

获取 Session 数据的 Arrow Schema。

```java
public Schema getArrowSchema()
```

**返回值**：Apache Arrow `Schema` 对象

---

### getTableSchema

获取 Session 数据的 MaxCompute 表 Schema。

```java
public TableSchema getTableSchema()
```

---

### getId

获取 Session 唯一标识符。

```java
public String getId()
```

---

### createReaderBuilder

针对指定 InputSplit 创建 Reader Builder。

```java
public TableReaderBuilder createReaderBuilder(InputSplit split)
```

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `split` | `InputSplit` | 来自 `getSplits()` 的数据分片 |

**返回值**：`TableReaderBuilder` 实例

---

## TableReaderBuilder

通过 `session.createReaderBuilder(split)` 获取。

| 方法 | 说明 |
|------|------|
| `withMaxBatchRows(long)` | 每批最大行数 |
| `withMaxBatchRawSize(long)` | 每批最大原始数据大小（字节） |
| `withSkipRowNum(long)` | 跳过的行数 |
| `withDataColumns(List<String>)` | Split 级别列选择 |
| `build()` | 构建 `ArrowReader` 实例 |

---

## 使用示例

```java
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");

TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "name", "score"))
    .withFilter("score >= 60")
    .withSplitOptions(SplitOptions.newBuilder().withSplitSize(256 * 1024 * 1024L).build())
    .build();

List<InputSplit> splits = session.getSplits();

for (InputSplit split : splits) {
    try (ArrowReader reader = session.createReaderBuilder(split).build()) {
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            int rowCount = root.getRowCount();
            // 处理数据...
        }
    }
}
```
