---
title: Tunnel Stream 流式写入
description: 使用 Tunnel StreamUploadSession 进行流式数据写入，flush 后数据即可见，无需手动提交
sidebar_position: 3
module: odps-sdk-tunnel
task: write-data
apis: [TableTunnel, StreamUploadSession]
since: "0.48.0"
keywords: [tunnel, stream, 流式写入, flush, 实时, 动态分区, schema版本]
---

# Tunnel Stream 流式写入

Tunnel Stream 是面向实时写入场景的数据通道，数据 flush 后立即可见，无需像 Tunnel Upload 那样手动提交 Session，适合日志采集、实时监控等需要低延迟可见的场景。

## 前置条件

- 已初始化 `Odps` 客户端实例
- 已创建 `TableTunnel` 实例（参考 [TableTunnel 文档](../../reference/TableTunnel)）
- 目标表已存在

## 完整示例

```java
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.streams.StreamRecordPack;

public class TunnelStreamExample {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 ODPS 客户端
        AliyunAccount account = new AliyunAccount("<accessId>", "<accessKey>");
        Odps odps = new Odps(account);
        odps.setDefaultProject("<projectName>");
        odps.setEndpoint("<endpoint>");

        // 2. 获取 TableTunnel 实例
        TableTunnel tunnel = odps.tableTunnel();

        // 3. 创建 StreamUploadSession
        String projectName = "<projectName>";
        String tableName = "<tableName>";
        TableTunnel.StreamUploadSession session = tunnel
            .buildStreamUploadSession(projectName, tableName)
            .build();

        // 4. 创建 StreamRecordPack
        StreamRecordPack pack = session.newRecordPack();

        // 5. 写入数据
        for (int i = 0; i < 1000; i++) {
            Record record = session.newRecord();
            record.setString("name", "user_" + i);
            record.setBigint("age", (long) (20 + i % 50));
            pack.append(record);
        }

        // 6. flush 数据（flush 后数据立即可见）
        String traceId = pack.flush();
        System.out.println("Flush completed, traceId: " + traceId);
    }
}
```

## 代码说明

### 与批量写入的关键区别

| 特性 | Tunnel Upload | Tunnel Stream |
|------|--------------|---------------|
| 数据可见时机 | commit 后 | flush 后 |
| 是否需要手动 commit | 是 | 否 |
| Block 管理 | 需手动指定 blockId | 无需关注 |
| 事务性 | 原子提交 | 非事务（逐批可见） |
| Session 有效期 | 24 小时 | 长期有效 |

### 写入流程

1. **创建 Session**：通过 `buildStreamUploadSession().build()` 创建流式上传会话
2. **创建 RecordPack**：通过 `session.newRecordPack()` 创建数据包
3. **追加记录**：向 RecordPack 中逐条追加数据
4. **flush 提交**：调用 `pack.flush()` 将数据发送到服务端并立即可见
5. **重复使用**：同一个 RecordPack 可以反复 append 和 flush

### Schema 版本追踪（0.50.0+）

从 SDK 0.50.0 起，当表结构发生变更时，如果 Session 中的 Schema 版本与服务端不一致，flush 操作会抛出 `SchemaMismatchException`。此时需要重新创建 Session 以获取最新的 Schema。

```java
try {
    pack.flush();
} catch (SchemaMismatchException e) {
    // 表结构已变更，需要重建 Session
    session = tunnel.buildStreamUploadSession(projectName, tableName).build();
    pack = session.newRecordPack();
    // 重新写入数据...
}
```

可通过 `allowSchemaMismatch(boolean)` 配置是否允许字段类型不匹配：

```java
TableTunnel.StreamUploadSession session = tunnel
    .buildStreamUploadSession(projectName, tableName)
    .allowSchemaMismatch(false) // 严格模式：Schema 不匹配时报错
    .build();
```

### 动态分区写入（0.55.0+）

从 SDK 0.55.0 起，支持通过 `DynamicPartitionRecordPack` 在一个 Session 中向多个分区写入数据，无需为每个分区单独创建 Session：

```java
// 创建 StreamUploadSession（不指定分区）
TableTunnel.StreamUploadSession session = tunnel
    .buildStreamUploadSession(projectName, tableName)
    .setCreatePartition(true) // 自动创建不存在的分区
    .build();

// 使用动态分区 RecordPack
StreamRecordPack pack = session.newRecordPack();

// 写入不同分区的数据
Record record1 = session.newRecord();
record1.setString("name", "alice");
record1.setBigint("age", 25L);
record1.setString("dt", "20250101"); // 分区列作为普通列写入
pack.append(record1);

Record record2 = session.newRecord();
record2.setString("name", "bob");
record2.setBigint("age", 30L);
record2.setString("dt", "20250102"); // 另一个分区
pack.append(record2);

pack.flush(); // 数据自动路由到对应分区
```

## 配置选项

### Session 构建参数

| 方法 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `setSlotNum(long)` | long | 0 | 并行槽位数量（0 表示自动分配） |
| `setCreatePartition(boolean)` | boolean | false | 自动创建不存在的分区 |
| `setSchemaVersion(String)` | String | 最新版本 | 指定 Schema 版本号 |
| `allowSchemaMismatch(boolean)` | boolean | true | 是否允许字段类型不匹配 |
| `setPartitionSpec(String)` | String | - | 指定目标分区 |
| `setSchemaName(String)` | String | - | 三层模型中的 Schema 名称 |

### 压缩选项

```java
// 使用 Snappy 压缩
StreamRecordPack pack = session.newRecordPack(
    new CompressOption(CompressOption.CompressAlgorithm.ODPS_SNAPPY, 0, 0));
```

| 算法 | 说明 |
|------|------|
| `ODPS_RAW` | 不压缩 |
| `ODPS_ZLIB` | ZLIB 压缩（0.51.2 起为默认值） |
| `ODPS_SNAPPY` | Snappy 压缩 |
| `ODPS_LZ4_FRAME` | LZ4 Frame 压缩 |

:::note
在 0.51.2 版本之前，默认压缩算法为 `ODPS_RAW`（不压缩）。
:::

### Flush 控制

```java
// 带超时控制的 flush
FlushOption option = new FlushOption().timeout(3000); // 3 秒超时
FlushResult result = pack.flush(option);
System.out.println("recordCount: " + result.getRecordCount());
System.out.println("flushSize: " + result.getFlushSize());
```

## 注意事项

1. **非事务性**：Tunnel Stream 不提供事务保证，每次 flush 的数据独立可见。如果写入过程中发生失败，已 flush 的数据不会回滚。
2. **数据量控制**：建议当 RecordPack 中数据量达到 64MB 时触发 flush，避免单次 flush 数据量过小导致文件碎片。
3. **内存管理**：长时间未 flush 的 RecordPack 会持续占用内存，需注意内存使用情况。
4. **避免跨线程操作**：单个 StreamUploadSession 支持创建多个 RecordPack，但需避免跨线程并发操作同一个 RecordPack。
5. **Schema 变更**：表结构变更后需重新创建 Session 获取最新 Schema。
6. **flush 频率**：建议合理控制 flush 频率，过于频繁的 flush 可能影响服务端性能。

## 相关文档

- [StreamUploadSession API 参考](../../reference/StreamUploadSession)
- [TableTunnel API 参考](../../reference/TableTunnel)
- [Tunnel Upload 批量写入](./tunnel-upload.md)
- [Tunnel Upsert 更新插入](./tunnel-upsert.md)
