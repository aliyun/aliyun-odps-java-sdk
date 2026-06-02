---
title: UploadSession
description: Tunnel 批量上传会话，提供基于 blockId 的数据写入和提交
sidebar_position: 7
module: odps-sdk-core
apis: [UploadSession]
keywords: [UploadSession, Tunnel, 上传, RecordWriter, commit]
---

# UploadSession

`UploadSession` 用于向 MaxCompute 表批量上传数据，支持基于 blockId 的多线程写入和事务性提交。会话服务端生命周期为 24 小时。

## 获取实例

```java
// 创建新会话
UploadSession session = tunnel.createUploadSession("my_project", "my_table");

// 分区表 + 覆盖写
UploadSession session = tunnel.createUploadSession("my_project", "my_table",
    new PartitionSpec("dt=20231001"), true);

// 复用已有会话
UploadSession session = tunnel.getUploadSession("my_project", "my_table", sessionId);
```

## 方法列表

### getSchema

获取表结构信息。

```java
public TableSchema getSchema()
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
public UploadStatus getStatus() throws TunnelException, IOException
```

**返回值**：`UploadStatus` 枚举，可选值：`UNKNOWN`、`NORMAL`、`CLOSING`、`CLOSED`、`EXPIRED`、`CRITICAL`

---

### newRecord

创建空记录对象。

```java
public Record newRecord()
```

---

### openRecordWriter

打开基于 blockId 的记录写入器。每个 blockId 应仅有一个写入者。

```java
public RecordWriter openRecordWriter(long blockId) throws TunnelException, IOException
public RecordWriter openRecordWriter(long blockId, boolean compress) throws TunnelException, IOException
public RecordWriter openRecordWriter(long blockId, CompressOption option) throws TunnelException, IOException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `blockId` | long | 数据块 ID（0 ~ 19999） |
| `compress` | boolean | 是否启用压缩 |
| `option` | CompressOption | 压缩选项 |

**注意**：
- 单个 Block 上限 100GB，建议大于 64MB
- Writer 120 秒无网络活动将被服务端关闭

**示例**：

```java
RecordWriter writer = session.openRecordWriter(0);
Record record = session.newRecord();
record.set("id", 1L);
record.set("name", "test");
writer.write(record);
writer.close();
```

---

### openBufferedWriter

打开带缓冲区的写入器，自动管理 blockId。

```java
public RecordWriter openBufferedWriter() throws TunnelException, IOException
public RecordWriter openBufferedWriter(boolean compress) throws TunnelException, IOException
public RecordWriter openBufferedWriter(CompressOption option) throws TunnelException, IOException
public RecordWriter openBufferedWriter(CompressOption option, long timeout) throws TunnelException, IOException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `compress` | boolean | 是否压缩 |
| `option` | CompressOption | 压缩选项 |
| `timeout` | long | 超时时间（毫秒），≤0 无超时 |

---

### openArrowRecordWriter

打开 Arrow 格式写入器。

```java
public ArrowRecordWriter openArrowRecordWriter(long blockId) throws TunnelException, IOException
public ArrowRecordWriter openArrowRecordWriter(long blockId, CompressOption option) throws TunnelException, IOException
public ArrowRecordWriter openArrowRecordWriter(long blockId, CompressOption option, long blockVersion) throws TunnelException, IOException
```

---

### writeBlock

通过 RecordPack 写入数据块。

```java
public void writeBlock(long blockId, RecordPack pack) throws IOException
public void writeBlock(long blockId, RecordPack pack, long timeout) throws IOException
public void writeBlock(long blockId, RecordPack pack, long timeout, long blockVersion) throws IOException, TunnelException
```

---

### getBlockList

获取已上传的 block 列表。

```java
public Long[] getBlockList()
```

**返回值**：已上传成功的 blockId 数组

---

### commit

提交上传，完成数据持久化。

```java
// 无校验提交
public void commit() throws TunnelException, IOException

// 带 block 列表校验提交
public void commit(Long[] blocks) throws TunnelException, IOException
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `blocks` | Long[] | 已上传的 blockId 列表，用于完整性校验 |

**示例**：

```java
writer.close();
session.commit(new Long[]{0L, 1L, 2L});
```

---

### getArrowSchema

获取 Arrow 格式的表结构。

```java
public Schema getArrowSchema()
```

---

### getQuotaName

获取本次上传使用的 quota 名称。

```java
public String getQuotaName()
```
