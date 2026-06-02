---
title: Tunnel Upload 批量写入
description: 使用 Tunnel UploadSession 进行批量数据写入，支持原子提交和多 Block 并行上传
sidebar_position: 2
module: odps-sdk-tunnel
task: write-data
apis: [TableTunnel, UploadSession, RecordWriter]
since: "0.48.0"
keywords: [tunnel, upload, 批量写入, block, commit, 原子提交, 并行上传]
---

# Tunnel Upload 批量写入

Tunnel Upload 是 MaxCompute 最经典的批量数据写入方式，通过 UploadSession 管理写入生命周期，支持原子提交确保数据一致性。

## 前置条件

- 已初始化 `Odps` 客户端实例
- 已创建 `TableTunnel` 实例（参考 [TableTunnel 文档](../../reference/TableTunnel)）
- 目标表已存在；若为分区表，分区需已创建或指定已有分区

## 完整示例

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;

public class TunnelUploadExample {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 ODPS 客户端
        AliyunAccount account = new AliyunAccount("<accessId>", "<accessKey>");
        Odps odps = new Odps(account);
        odps.setDefaultProject("<projectName>");
        odps.setEndpoint("<endpoint>");

        // 2. 获取 TableTunnel 实例
        TableTunnel tunnel = odps.tableTunnel();

        // 3. 创建 UploadSession
        String projectName = "<projectName>";
        String tableName = "<tableName>";
        TableTunnel.UploadSession session = tunnel.createUploadSession(projectName, tableName);
        System.out.println("Session ID: " + session.getId());

        // 4. 打开 RecordWriter（指定 blockId）
        long blockId = 0;
        RecordWriter writer = session.openRecordWriter(blockId, true); // 启用压缩

        // 5. 写入数据
        for (int i = 0; i < 1000; i++) {
            Record record = session.newRecord();
            record.setString("name", "user_" + i);
            record.setBigint("age", (long) (20 + i % 50));
            writer.write(record);
        }

        // 6. 关闭 Writer
        writer.close();

        // 7. 提交（带 Block 校验）
        session.commit(new Long[]{blockId});
        System.out.println("Upload committed successfully.");
    }
}
```

## 代码说明

### 写入流程

整个 Tunnel Upload 写入流程分为以下步骤：

1. **创建 Session**：通过 `TableTunnel.createUploadSession()` 创建上传会话，获取会话 ID
2. **打开 Writer**：通过 `session.openRecordWriter(blockId)` 获取写入器，需指定 Block ID
3. **写入记录**：构建 Record 对象并逐条写入
4. **关闭 Writer**：调用 `writer.close()` 完成当前 Block 的数据传输
5. **提交 Session**：调用 `session.commit()` 使数据对外可见

### Block 管理

Block 是 Tunnel Upload 的核心概念：

- **Block ID 范围**：0 ~ 19999，同一 Session 内唯一标识一个数据块
- **唯一写入者**：同一时刻每个 Block ID 只能有一个活跃的 Writer
- **覆盖语义**：对同一个 Block ID 重新写入会覆盖之前的数据（未 commit 前）
- **大小建议**：每个 Block 建议不小于 64MB，上限为 100GB

### 多 Block 并行上传

利用不同的 Block ID，可以实现多线程并行写入：

```java
TableTunnel.UploadSession session = tunnel.createUploadSession(projectName, tableName);

int threadCount = 4;
ExecutorService executor = Executors.newFixedThreadPool(threadCount);
List<Future<?>> futures = new ArrayList<>();

for (int t = 0; t < threadCount; t++) {
    final long blockId = t;
    futures.add(executor.submit(() -> {
        try {
            RecordWriter writer = session.openRecordWriter(blockId, true);
            for (int i = 0; i < 10000; i++) {
                Record record = session.newRecord();
                record.setString("name", "thread_" + blockId + "_row_" + i);
                record.setBigint("value", (long) i);
                writer.write(record);
            }
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }));
}

// 等待所有线程完成
for (Future<?> f : futures) {
    f.get();
}
executor.shutdown();

// 提交所有 Block
Long[] blocks = new Long[]{0L, 1L, 2L, 3L};
session.commit(blocks);
```

### 使用 TunnelBufferedWriter

如果不想手动管理 Block ID，可以使用 `TunnelBufferedWriter`，它内部自动管理缓冲区和 Block 分配：

```java
TableTunnel.UploadSession session = tunnel.createUploadSession(projectName, tableName);
RecordWriter writer = session.openBufferedWriter(true); // 启用压缩

for (int i = 0; i < 100000; i++) {
    Record record = session.newRecord();
    record.setString("name", "user_" + i);
    record.setBigint("age", (long) (20 + i % 50));
    writer.write(record);
}

writer.close();
session.commit();
```

## 配置选项

### 压缩算法

| 算法 | 说明 |
|------|------|
| `CompressOption.CompressAlgorithm.ODPS_RAW` | 不压缩 |
| `CompressOption.CompressAlgorithm.ODPS_ZLIB` | ZLIB 压缩（支持 level 和 strategy） |
| `CompressOption.CompressAlgorithm.ODPS_SNAPPY` | Snappy 压缩 |
| `CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME` | LZ4 Frame 压缩 |

```java
CompressOption option = new CompressOption(
    CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME, 0, 0);
RecordWriter writer = session.openRecordWriter(blockId, option);
```

### 分区表写入

```java
// 写入指定分区
PartitionSpec partitionSpec = new PartitionSpec("dt='20250101'");
TableTunnel.UploadSession session = tunnel.createUploadSession(
    projectName, tableName, partitionSpec);
```

### 覆盖写入

```java
// 覆盖表中已有数据
boolean overwrite = true;
TableTunnel.UploadSession session = tunnel.createUploadSession(
    projectName, tableName, overwrite);
```

## 注意事项

1. **Session 有效期**：每个 UploadSession 在服务端的生命周期为 24 小时，超时后需重新创建。
2. **Writer 超时**：RecordWriter 如果 120 秒内没有网络活动，服务端将主动断开连接，需重新打开 Writer。
3. **Block 大小**：建议每个 Block 数据量不小于 64MB，避免产生大量小文件。单个 Block 上限 100GB。
4. **提交后不可变**：`commit()` 成功后数据立即对外可见，且不可回滚。
5. **Block ID 唯一性**：同一个 Block ID 同时只能有一个 Writer 在写入，否则会导致数据混乱。
6. **Session 不可复用**：一个 Session 只能 commit 一次，后续写入需创建新的 Session。

## 相关文档

- [UploadSession API 参考](../../reference/UploadSession)
- [TableTunnel API 参考](../../reference/TableTunnel)
- [Tunnel Stream 流式写入](./tunnel-stream.md)
- [Storage API 高性能写入](./storage-api-write.md)
