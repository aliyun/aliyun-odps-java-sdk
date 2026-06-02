---
title: StreamUploadSession
description: Tunnel 流式上传会话，支持写入即可见的实时数据摄入
sidebar_position: 8
module: odps-sdk-core
apis: [StreamUploadSession]
keywords: [StreamUploadSession, Tunnel, 流式上传, flush, StreamRecordPack]
---

# StreamUploadSession

`StreamUploadSession` 专为流式数据上传设计，支持写入即可见（auto-commit）特性，无需手动提交会话。

## 获取实例

通过 `TableTunnel.buildStreamUploadSession` 构建：

```java
StreamUploadSession session = tunnel.buildStreamUploadSession("my_project", "my_table")
    .setPartitionSpec("dt=20231001/city=shanghai")
    .build();
```

### Builder 配置

| 方法 | 参数类型 | 默认值 | 说明 |
|------|----------|--------|------|
| `setProjectName(String)` | String | - | 项目名称（必填） |
| `setTableName(String)` | String | - | 表名称（必填） |
| `setSchemaName(String)` | String | null | Schema 名称（三层模型） |
| `setPartitionSpec(String)` | String | null | 分区表达式 |
| `setSlotNum(long)` | long | 0 | 并行槽位数（0 自动分配） |
| `setCreatePartition(boolean)` | boolean | false | 自动创建不存在的分区 |
| `setSchemaVersion(String)` | String | 最新版本 | 指定 Schema 版本号 |
| `allowSchemaMismatch(boolean)` | boolean | true | 是否允许字段类型不匹配 |
| `setDynamicPartition(boolean)` | boolean | false | 设置是否启用动态分区写入 |

## 方法列表

### getId

获取会话 ID。

```java
public String getId()
```

---

### getSchema

获取表结构信息。

```java
public TableSchema getSchema()
```

---

### getSchemaVersion

获取表 Schema 版本号。

```java
public String getSchemaVersion()
```

---

### getQuotaName

获取本次流式上传使用的 Quota 名称。

```java
public String getQuotaName()
```

---

### getLastBatchId

获取最后一个已提交的 batch ID。

```java
public long getLastBatchId() throws TunnelException
```

---

### getLastBatchCommitTime

获取最后一次 batch 提交时间（毫秒时间戳）。

```java
public long getLastBatchCommitTime() throws TunnelException
```

---

### newRecordPack

创建流式数据包。

```java
public StreamRecordPack newRecordPack() throws IOException
public StreamRecordPack newRecordPack(CompressOption option) throws IOException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `option` | CompressOption | 压缩选项，默认 ODPS_ZLIB |

**压缩算法可选值**：`ODPS_RAW`、`ODPS_ZLIB`、`ODPS_SNAPPY`、`ODPS_LZ4_FRAME`

---

### newRecord

创建空记录对象。

```java
public Record newRecord()
```

---

### StreamRecordPack 接口

#### append

向数据包中追加一条记录。

```java
public void append(Record record) throws IOException
```

---

#### getRecordCount

获取当前包中的记录数。

```java
public long getRecordCount()
```

---

#### getDataSize

获取当前数据大小（由于多层缓冲区，值变化可能不连续）。

```java
public long getDataSize()
```

---

#### flush

将数据发送到服务端。flush 成功后 pack 可复用。

```java
// 基础提交
public String flush() throws IOException

// 带选项提交
public FlushResult flush(FlushOption flushOption) throws IOException
```

**FlushOption 配置**：

```java
FlushOption option = new FlushOption().timeout(3000); // 3秒超时
```

**FlushResult 结构**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `traceId` | String | 请求追踪 ID |
| `recordCount` | long | 本次 flush 的记录数 |
| `flushSize` | long | 本次 flush 的数据大小 |
| `batchId` | long | 批次 ID |

---

#### reset

重置缓冲区（flush 成功后自动调用）。

```java
public void reset() throws IOException
```

---

### 使用示例

```java
StreamUploadSession session = tunnel.buildStreamUploadSession("my_project", "my_table")
    .setCreatePartition(true)
    .setPartitionSpec("dt=20231001")
    .build();

StreamRecordPack pack = session.newRecordPack();

while (hasMoreData()) {
    Record record = session.newRecord();
    record.set("id", 1001);
    record.set("action", "purchase");
    pack.append(record);

    if (pack.getDataSize() > 64 * 1024 * 1024) { // 64MB 触发提交
        pack.flush();
    }
}
// 最后一批
if (pack.getRecordCount() > 0) {
    pack.flush();
}
```

**注意事项**：
- 单个 StreamUploadSession 支持创建多个 RecordPack，但需避免跨线程并发操作同一个 pack
- 长时间未 flush 的数据包会持续占用内存
- 修改表结构后需重新创建会话以获取最新 Schema
