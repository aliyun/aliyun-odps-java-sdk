---
title: Tunnel 数据通道模块
description: MaxCompute 数据通道，提供高效的表数据批量上传和下载能力
sidebar_position: 2
module: odps-sdk-core
keywords: [tunnel, tabletunnel, upload, download, stream, upsert, compression, retry]
---

# Tunnel 数据通道模块

Tunnel 是 MaxCompute 提供的高效数据通道，用于大规模数据的批量上传和下载。它支持多种上传模式（流式/批量）和写入模式（追加/更新插入），并提供灵活的压缩、重试策略配置，以满足不同业务场景下的数据传输需求。

Tunnel 功能内置于 `odps-sdk-core` 模块中，无需额外引入依赖。

## Maven 依赖

```xml
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-core</artifactId>
    <version>${odps.sdk.version}</version>
</dependency>
```

## 核心功能

| 功能 | 说明 |
|------|------|
| **批量上传** | 通过 UploadSession 实现手动提交的批量数据写入 |
| **流式上传** | 通过 StreamUploadSession 实现自动提交的实时数据写入 |
| **Upsert 写入** | 通过 UpsertSession 对 Delta Table 进行更新/插入操作 |
| **数据下载** | 通过 DownloadSession 实现表数据的批量下载 |
| **数据压缩** | 支持 LZ4、ZSTD、ZLIB、Snappy 等多种压缩算法 |
| **自动重试** | 内置智能重试机制，对限流、网络异常等自动恢复 |
| **Quota 管理** | 支持指定独享 Tunnel Quota 资源 |

## 核心类

| 类 | 说明 |
|------|------|
| `TableTunnel` | Tunnel 入口类，通过 `odps.tableTunnel()` 获取 |
| `UploadSession` | 批量上传会话，数据需手动 commit 后才对查询可见 |
| `DownloadSession` | 数据下载会话，支持按范围读取表数据 |
| `StreamUploadSession` | 流式上传会话，数据由服务端自动提交，适合实时场景 |
| `UpsertSession` | 更新插入会话，用于 Delta Table 的 upsert 操作 |
| `Configuration` | Tunnel 配置类，通过 Builder 模式设置各项参数 |
| `TunnelRetryHandler` | 重试处理器，管理错误重试策略和回退机制 |

## 配置

### Configuration Builder

通过 `Configuration.builder(odps)` 创建配置对象：

```java
import com.aliyun.odps.tunnel.Configuration;

Configuration configuration = Configuration.builder(odps)
    .withQuotaName("my_quota")
    .withTags(Arrays.asList("tag1", "tag2"))
    .withCompressOptions(new CompressOption(
        CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME, 0, 0))
    .withRetryPolicy(new CustomRetryPolicy())
    .withRetryLogger(new CustomRetryLogger())
    .build();

TableTunnel tunnel = odps.tableTunnel(configuration);
```

### 配置项

| 配置项 | 方法 | 说明 |
|--------|------|------|
| Quota 名称 | `withQuotaName(String)` | 指定独享 Tunnel Quota |
| 标签列表 | `withTags(List<String>)` | 设置请求标签 |
| 压缩选项 | `withCompressOptions(CompressOption)` | 设置数据传输压缩方式 |
| 重试策略 | `withRetryPolicy(RetryPolicy)` | 自定义错误重试逻辑 |
| 重试日志 | `withRetryLogger(RetryLogger)` | 重试事件日志记录 |

### 压缩算法对比

| 算法 | 标识 | 特点 | 适用场景 |
|------|------|------|----------|
| 无压缩 | `ODPS_RAW` | 无 CPU 开销 | 低延迟场景 |
| ZLIB | `ODPS_ZLIB` | 压缩率高，CPU 消耗较大 | 带宽敏感场景（默认） |
| Snappy | `ODPS_SNAPPY` | 压缩/解压速度快 | 平衡场景 |
| LZ4 | `ODPS_LZ4_FRAME` | 极高的压缩/解压速度 | 高吞吐场景 |
| Arrow LZ4 | `ODPS_ARROW_LZ4_FRAME` | Arrow 格式专用 LZ4 | Arrow Tunnel |
| Arrow ZSTD | `ODPS_ARROW_ZSTD` | Arrow 格式专用 ZSTD，高压缩率 | Arrow Tunnel |

### 重试策略

SDK 内置了对常见服务端错误的自动重试：

| 错误码 | 处理方式 |
|--------|----------|
| **429** | 服务端限流，无限指数退避重试（最大等待 64 秒） |
| **502/504** | 触发会话重新加载，指数退避重试（1~64 秒） |
| **5xx** | 服务端错误，指数退避重试（1~64 秒） |
| **308** | 仅 UpsertSession，更新缓存 IP 后无限重试 |
| **其他** | 使用用户自定义 RetryPolicy（默认不重试） |

#### 自定义 RetryPolicy

```java
public class CustomRetryPolicy implements TunnelRetryHandler.RetryPolicy {
    @Override
    public boolean shouldRetry(Exception e, int attempt) {
        // 自定义重试判断逻辑
        return attempt < 5;
    }

    @Override
    public long getRetryWaitTime(int attempt) {
        // 指数退避
        return Math.min(1000L * (1 << attempt), 64000L);
    }
}
```

#### 自定义 RetryLogger

```java
public class CustomRetryLogger extends RestClient.RetryLogger {
    @Override
    public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
        System.out.println("Retry #" + retryCount
            + " after " + retrySleepTime + "s due to: " + e.getMessage());
    }
}
```

## 使用示例

### 初始化 TableTunnel

```java
// 无参初始化（使用默认配置）
TableTunnel tunnel = odps.tableTunnel();

// 使用自定义配置初始化
Configuration config = Configuration.builder(odps)
    .withCompressOptions(new CompressOption(
        CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME, 0, 0))
    .build();
TableTunnel tunnel = odps.tableTunnel(config);
```

### 批量上传

```java
TableTunnel.UploadSession session = tunnel.createUploadSession(
    "my_project", "my_table");

// 获取 RecordWriter 写入数据
RecordWriter writer = session.openRecordWriter(0);
Record record = session.newRecord();
record.setString("name", "test");
record.setBigint("id", 1L);
writer.write(record);
writer.close();

// 提交数据
session.commit(new long[]{0});
```

### 数据下载

```java
TableTunnel.DownloadSession session = tunnel.createDownloadSession(
    "my_project", "my_table");

long count = session.getRecordCount();
RecordReader reader = session.openRecordReader(0, count);
Record record;
while ((record = reader.read()) != null) {
    System.out.println(record.getString("name"));
}
reader.close();
```

## 相关文档

- [使用 Tunnel 对表进行上传下载](../core-concept/tabletunnel.md)
- [Configuration 配置类](../api-reference/tunnel/Configuration.md)
- [RetryPolicy 重试逻辑](../api-reference/tunnel/RetryLogic.md)
- [UploadSession 文档](../api-reference/tunnel/UploadSession.md)
- [DownloadSession 文档](../api-reference/tunnel/DownloadSession.md)
- [StreamUploadSession 文档](../api-reference/tunnel/StreamUploadSession.md)
- [UpsertSession 文档](../api-reference/tunnel/UpsertSession.md)
