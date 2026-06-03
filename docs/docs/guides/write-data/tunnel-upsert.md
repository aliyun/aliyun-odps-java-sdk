---
title: Tunnel Upsert 更新插入
description: 使用 Tunnel UpsertSession 对 Delta Table（主键表）进行数据的插入、更新和删除操作
sidebar_position: 4
module: odps-sdk-tunnel
task: write-data
apis: [TableTunnel, UpsertSession, UpsertStream]
since: "0.48.3"
keywords: [tunnel, upsert, delta table, 主键表, 更新, 删除, flush, CDC]
---

# Tunnel Upsert 更新插入

:::note
Tunnel Upsert 目前仅 Java SDK 支持。Python 和 Go SDK 暂不提供此功能。
:::

Tunnel Upsert 专为 Delta Table（主键表）设计，支持按主键进行数据的插入、更新和删除操作，适合数据库变更同步（CDC）、实时数据修正等场景。

## 前置条件

- 已初始化 `Odps` 客户端实例
- 已创建 `TableTunnel` 实例（参考 [TableTunnel 文档](../../reference/TableTunnel)）
- **目标表必须是 Delta Table（Transactional 表），且定义了主键（Primary Key）**

:::info
Delta Table 是 MaxCompute 支持行级更新/删除的表类型。建表时需要指定主键列和 `TBLPROPERTIES('transactional'='true')`。
:::

## 完整示例

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.streams.UpsertStream;

public class TunnelUpsertExample {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 ODPS 客户端
        AliyunAccount account = new AliyunAccount("<accessId>", "<accessKey>");
        Odps odps = new Odps(account);
        odps.setDefaultProject("<projectName>");
        odps.setEndpoint("<endpoint>");

        // 2. 获取 TableTunnel 实例
        TableTunnel tunnel = odps.tableTunnel();

        // 3. 创建 UpsertSession
        String projectName = "<projectName>";
        String tableName = "<tableName>";

        try (TableTunnel.UpsertSession upsertSession = tunnel
                .buildUpsertSession(projectName, tableName)
                .build()) {

            System.out.println("Upsert Session ID: " + upsertSession.getId());

            // 4. 创建 UpsertStream
            UpsertStream stream = upsertSession.buildUpsertStream().build();

            // 5. 插入/更新记录
            Record record = upsertSession.newRecord();
            record.setString("id", "1001");
            record.setString("name", "alice");
            record.setBigint("score", 95L);
            stream.upsert(record);

            // 6. 更新另一条记录
            Record record2 = upsertSession.newRecord();
            record2.setString("id", "1002");
            record2.setString("name", "bob");
            record2.setBigint("score", 88L);
            stream.upsert(record2);

            // 7. 删除一条记录（只需设置主键列）
            Record deleteRecord = upsertSession.newRecord();
            deleteRecord.setString("id", "1003");
            stream.delete(deleteRecord);

            // 8. flush 数据到服务端
            stream.flush();

            // 9. 关闭 Stream
            stream.close();

            // 10. 提交 Session（数据变为可见）
            upsertSession.commit(false); // false = 同步提交
            System.out.println("Upsert committed successfully.");
        }
    }
}
```

## 代码说明

### 写入流程

1. **创建 UpsertSession**：通过 `buildUpsertSession().build()` 创建会话
2. **创建 UpsertStream**：通过 `upsertSession.buildUpsertStream().build()` 获取写入流
3. **执行操作**：调用 `stream.upsert(record)` 或 `stream.delete(record)` 执行数据变更
4. **flush 缓冲**：调用 `stream.flush()` 将缓冲区数据发送到服务端
5. **关闭 Stream**：调用 `stream.close()` 关闭写入流
6. **提交 Session**：调用 `upsertSession.commit(false)` 使数据对外可见

:::info
数据只有在 `commit()` 后才对外可见。`flush()` 仅将缓冲区数据发送到服务端暂存，并不代表数据可查。
:::

### Record 创建注意事项

必须使用 `upsertSession.newRecord()` 方法创建 Record 实例，不能手动创建 `ArrayRecord`。因为 `UpsertRecord` 内部维护了必要的元数据隐藏列。

```java
// 正确
Record record = upsertSession.newRecord();

// 错误 - 不要这样做
// Record record = new ArrayRecord(schema);
```

### 部分列更新

支持仅更新指定列，未指定的列保持原值不变：

```java
Record record = upsertSession.newRecord();
record.setString("id", "1001");       // 主键列必须设置
record.setBigint("score", 99L);       // 只更新 score 列

List<String> upsertCols = Arrays.asList("id", "score");
stream.upsert(record, upsertCols);
```

### Flush Listener 模式

通过自定义 Listener 可以监听 flush 事件并实现自定义重试逻辑：

```java
UpsertStream stream = upsertSession.buildUpsertStream()
    .setListener(new UpsertSessionImpl.DefaultUpsertSteamListener() {
        @Override
        public void onFlushSuccess(long flushSize) {
            System.out.println("Flush success, size: " + flushSize);
        }

        @Override
        public void onFlushFailed(String errorMessage, int retryCount) {
            System.err.println("Flush failed (retry " + retryCount + "): " + errorMessage);
        }
    })
    .build();
```

:::tip
推荐继承 `UpsertSessionImpl.DefaultUpsertSteamListener`，以保留 TableTunnel 默认的重试逻辑。
:::

### 高并发写入

对于需要高吞吐的场景，可以创建多个 UpsertStream 并行写入。需要注意：相同主键的记录应当路由到同一个 Stream 中，避免并发冲突。

```java
try (TableTunnel.UpsertSession upsertSession = tunnel
        .buildUpsertSession(projectName, tableName)
        .setSlotNum(4)
        .build()) {

    int threadCount = 4;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    for (int t = 0; t < threadCount; t++) {
        final int threadId = t;
        executor.submit(() -> {
            try {
                UpsertStream stream = upsertSession.buildUpsertStream().build();
                // 每个线程写入属于自己的数据（按主键 hash 分配）
                for (Record record : getRecordsForThread(threadId)) {
                    stream.upsert(record);
                }
                stream.flush();
                stream.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.HOURS);

    upsertSession.commit(false);
}
```

## 配置选项

### Session 构建参数

| 方法 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `setSlotNum(long)` | long | 自动 | 并行槽位数量 |
| `setPartitionSpec(String)` | String | - | 目标分区 |
| `setSchemaName(String)` | String | - | 三层模型 Schema 名称 |
| `setUpsertId(String)` | String | - | 复用已有 Session 的 ID |
| `setCommitTimeout(long)` | long | - | 提交超时时间（毫秒） |
| `setLifecycle(long)` | long | 服务端默认 | Session 生命周期（小时，1~24） |
| `setNetworkThreadNum(int)` | int | 1 | Netty 网络 IO 线程数 |
| `setConcurrentNum(int)` | int | 20 | 最大并发 Channel 数 |
| `setConnectTimeout(long)` | long | 180000 | 连接超时时间（毫秒） |
| `setReadTimeout(long)` | long | 300000 | 请求响应超时时间（毫秒） |

### Stream 构建参数

| 方法 | 类型 | 说明 |
|------|------|------|
| `setMaxBufferSize(long)` | long | 数据缓冲区最大容量（字节） |
| `setSlotBufferSize(long)` | long | 每个桶的缓冲区大小（字节） |
| `setCompressOption(CompressOption)` | CompressOption | 压缩选项 |
| `setListener(Listener)` | Listener | flush 事件监听器 |

### 压缩选项

自 0.48.3 起，默认压缩算法为 `ODPS_LZ4_FRAME`：

```java
UpsertStream stream = upsertSession.buildUpsertStream()
    .setCompressOption(new CompressOption(
        CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME, 0, 0))
    .build();
```

## 注意事项

1. **仅适用于 Delta Table**：目标表必须是 Transactional 表且定义了主键，否则会报错。
2. **主键冲突**：多个并发写入同一主键可能导致不可预期的行为。建议对数据按主键进行 shuffle，将相同主键的记录分配到同一线程。
3. **commit 频率**：推荐 commit 间隔每分区不低于一分钟，过于频繁的 commit 会产生大量小文件，影响查询性能，还可能触发服务端限流。
4. **单次 commit**：每个 Session 只能 commit 一次，继续写入需要重建 Session。
5. **资源清理**：务必调用 `close()` 方法释放客户端资源。推荐使用 try-with-resources 语法。
6. **数据可见性**：数据只有在 `commit()` 之后才对外可见，`flush()` 仅将数据发送到服务端暂存。

## 相关文档

- [UpsertSession API 参考](../../reference/UpsertSession)
- [TableTunnel API 参考](../../reference/TableTunnel)
- [Tunnel Upload 批量写入](./tunnel-upload.md)
- [Tunnel Stream 流式写入](./tunnel-stream.md)
