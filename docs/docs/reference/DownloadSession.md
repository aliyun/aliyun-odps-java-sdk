---
title: DownloadSession
description: Tunnel 下载会话，提供表数据的分块并行下载能力
sidebar_position: 6
module: odps-sdk-core
apis: [DownloadSession, DownloadSessionBuilder]
keywords: [DownloadSession, Tunnel, 下载, RecordReader, Arrow]
---

# DownloadSession

`DownloadSession` 用于从 MaxCompute 表下载数据，支持分块并行读取、列投影和 Arrow 格式。

## 获取实例

通过 `TableTunnel.buildDownloadSession` 构建（推荐）：

```java
DownloadSession session = tunnel.buildDownloadSession("my_project", "my_table")
    .setPartitionSpec(new PartitionSpec("dt=20231001"))
    .build();
```

### DownloadSessionBuilder 配置

| 方法 | 参数类型 | 必需 | 默认值 | 说明 |
|------|----------|------|--------|------|
| `setProjectName(String)` | String | 是 | - | 项目名称 |
| `setTableName(String)` | String | 是 | - | 表名称 |
| `setSchemaName(String)` | String | 否 | null | Schema 名称（三层模型） |
| `setPartitionSpec(PartitionSpec)` | PartitionSpec | 否 | null | 分区表达式 |
| `setShardId(Long)` | Long | 否 | null | 指定分片 ID |
| `setAsyncMode(boolean)` | boolean | 否 | false | 启用异步初始化 |
| `setWaitAsyncBuild(boolean)` | boolean | 否 | false | 阻塞等待异步会话就绪 |
| `setDownloadId(String)` | String | 否 | null | 复用已有会话 ID |

**异步模式示例**：

```java
DownloadSessionBuilder builder = tunnel.buildDownloadSession("my_project", "my_table")
    .setAsyncMode(true);
DownloadSession session = builder.build();
boolean ready = builder.wait(session, 5, 300); // 每5秒检查，最长5分钟
```

## 方法列表

### getSchema

获取表结构信息。

```java
public TableSchema getSchema()
```

**返回值**：`TableSchema` 对象

---

### getRecordCount

获取可下载的总记录数。

```java
public long getRecordCount()
```

---

### getId

获取会话 ID。

```java
public String getId()
```

---

### getStatus

获取会话状态。

```java
public DownloadStatus getStatus()
```

---

### getSplitCount

获取数据分片数量（用于 Arrow 读取）。

```java
public long getSplitCount()
```

---

### getQuotaName

获取本次下载使用的 Quota 名称。

```java
public String getQuotaName()
```

---

### getArrowSchema

获取 Arrow 格式的表结构。

```java
public Schema getArrowSchema()
```

**返回值**：Arrow `Schema` 对象

---

### openRecordReader

打开数据读取器。

```java
// 基础形式
public TunnelRecordReader openRecordReader(long start, long count) throws TunnelException, IOException

// 布尔压缩开关
public TunnelRecordReader openRecordReader(long start, long count, boolean compress) throws TunnelException, IOException

// 带压缩选项
public TunnelRecordReader openRecordReader(long start, long count, CompressOption option) throws TunnelException, IOException

// 指定列裁剪
public TunnelRecordReader openRecordReader(long start, long count, CompressOption compress, List<Column> columns) throws TunnelException, IOException

// 列投影 + 压缩 + 版本检查
public TunnelRecordReader openRecordReader(long start, long count, CompressOption option, List<Column> columns, boolean disableModifiedCheck) throws TunnelException, IOException
```

| 参数 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `start` | long | >= 0 | 读取起始位置（行号） |
| `count` | long | >= 1 | 读取记录数量 |
| `compress` | boolean | - | 是否启用压缩 |
| `option` | CompressOption | - | 压缩配置 |
| `columns` | List\<Column\> | 非空 | 需下载的列集合 |
| `disableModifiedCheck` | boolean | - | 禁用数据版本校验 |

**返回值**：`TunnelRecordReader` 对象，建议使用 try-with-resources

**示例**：

```java
long total = session.getRecordCount();
try (TunnelRecordReader reader = session.openRecordReader(0, total)) {
    Record record;
    while ((record = reader.read()) != null) {
        System.out.println(record.get("user_id"));
    }
}
```

---

### openBufferedRecordReader

打开带缓冲区的记录读取器。

```java
public RecordReader openBufferedRecordReader(long start, long count, long batchSize, long bufferSize, CompressOption compress, List<Column> columns, boolean disableModifiedCheck) throws TunnelException, IOException
```

| 参数 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `start` | long | >= 0 | 读取起始位置（行号） |
| `count` | long | >= 1 | 读取记录数量 |
| `batchSize` | long | >= 1 | 每批次读取的记录数 |
| `bufferSize` | long | >= 1 | 缓冲区大小 |
| `compress` | CompressOption | - | 压缩配置 |
| `columns` | List\<Column\> | 非空 | 需下载的列集合 |
| `disableModifiedCheck` | boolean | - | 禁用数据版本校验 |

**返回值**：`RecordReader` 对象，建议使用 try-with-resources

---

### openArrowRecordReader

打开 Arrow 格式数据读取器，适合高性能列式数据处理。

```java
// 基础形式
public ArrowRecordReader openArrowRecordReader(long start, long count) throws TunnelException, IOException

// 按分片索引读取
public ArrowRecordReader openArrowRecordReader(long splitIndex) throws TunnelException, IOException

// 指定压缩
public ArrowRecordReader openArrowRecordReader(long start, long count, CompressOption compress) throws TunnelException, IOException

// 指定列
public ArrowRecordReader openArrowRecordReader(long start, long count, List<Column> columns) throws TunnelException, IOException

// 指定列和内存分配器
public ArrowRecordReader openArrowRecordReader(long start, long count, List<Column> columns, BufferAllocator allocator) throws TunnelException, IOException

// 完整参数
public ArrowRecordReader openArrowRecordReader(long start, long count, List<Column> columns, BufferAllocator allocator, CompressOption compress) throws TunnelException, IOException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `start` | long | 起始位置 |
| `count` | long | 读取数量 |
| `splitIndex` | long | 分片索引（配合 `getSplitCount()` 使用） |
| `compress` | CompressOption | 压缩配置 |
| `columns` | List\<Column\> | 需下载的列集合 |
| `allocator` | BufferAllocator | Arrow 内存分配器 |

**示例**：

```java
try (ArrowRecordReader reader = session.openArrowRecordReader(0, 50000)) {
    VectorSchemaRoot root = reader.read();
    FieldVector idVector = root.getVector("user_id");
    for (int i = 0; i < root.getRowCount(); i++) {
        System.out.println(idVector.getObject(i));
    }
}
```

---

### 分块并行下载示例

```java
long totalRecords = session.getRecordCount();
int parallelism = 8;
ExecutorService pool = Executors.newFixedThreadPool(parallelism);

for (int i = 0; i < parallelism; i++) {
    long start = i * (totalRecords / parallelism);
    long count = (i == parallelism - 1)
        ? totalRecords - start
        : totalRecords / parallelism;

    pool.submit(() -> {
        try (TunnelRecordReader reader = session.openRecordReader(start, count)) {
            Record record;
            while ((record = reader.read()) != null) {
                processRecord(record);
            }
        }
    });
}
```
