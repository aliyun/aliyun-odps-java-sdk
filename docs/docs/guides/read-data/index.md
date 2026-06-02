---
title: 读取数据
description: MaxCompute Java SDK 提供三种数据读取方式，适用于不同场景和数据规模。本指南帮助你选择最合适的方案。
sidebar_position: 1
module: odps-sdk-core
task: read-data
apis: [Table, TableTunnel, MaxStorageClient]
since: "0.47.0"
keywords: [读取, 下载, 预览, Tunnel, Storage API, Arrow]
---

# 读取数据

MaxCompute Java SDK 提供三种数据读取方式：数据预览（Table.read）、Tunnel 下载和 Storage API 读取，分别适用于不同的数据规模和性能需求。

## 方式对比

| 特性 | Table.read（预览） | Tunnel Download | Storage API |
|------|-------------------|-----------------|-------------|
| **模块** | odps-sdk-core | odps-sdk-tunnel | odps-sdk-storage-api |
| **数据格式** | Record | Record / Arrow | Arrow |
| **最大数据量** | 1万行 / 10MB | 无限制 | 无限制 |
| **并行读取** | 不支持 | 支持（按行号分块） | 支持（按 Split 分片） |
| **列裁剪** | 支持 | 支持 | 支持 |
| **谓词下推** | 不支持 | 不支持 | 支持 |
| **分区过滤** | 支持 | 支持 | 支持 |
| **Transactional 表** | 支持 | 不支持 | 支持 |
| **Blob 类型** | 不支持 | 不支持 | 支持 |
| **增量读取** | 不支持 | 不支持 | 支持 |
| **适用版本** | >= 0.47.0 | >= 0.48.0 | >= 0.52.0 |

## 选型决策

```
需要读取表数据
    │
    ├─ 数据量 < 1万行，仅用于预览/调试？
    │       → Table.read（数据预览）
    │
    ├─ 批量导出数据，对性能要求一般？
    │       → Tunnel Download
    │
    └─ 以下任一场景：
        ├─ 需要高性能并行读取（Arrow 列式格式）
        ├─ 需要谓词下推减少数据传输
        ├─ 需要读取 Transactional 表
        ├─ 需要读取 Blob 类型数据
        └─ 需要增量读取
                → Storage API
```

## 快速开始

### 数据预览（最简单）

```java
RecordReader reader = table.read(100);
Record record;
while ((record = reader.read()) != null) {
    System.out.println(record.get(0));
}
```

### Tunnel 下载（批量导出）

```java
TableTunnel tunnel = new TableTunnel(odps);
DownloadSession session = tunnel.buildDownloadSession()
    .setProjectName("my_project")
    .setTableName("my_table")
    .build();

try (TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount())) {
    while (reader.hasNext()) {
        Record record = reader.next();
        // 处理数据...
    }
}
```

### Storage API（高性能）

```java
MaxStorageClient client = MaxStorageClient.builder()
    .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
    .credentialsProvider(credentialsProvider)
    .build();

TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
TableReadSession session = client.createTableReadSessionBuilder(tableId)
    .withColumns(Arrays.asList("id", "name"))
    .build();

for (InputSplit split : session.getSplits()) {
    try (ArrowReader reader = session.createReaderBuilder(split).build()) {
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            // 处理 Arrow 批次数据...
        }
    }
}
```

## 相关文档

- [数据预览（Table.read）](./preview.md) - 快速预览少量数据
- [Tunnel 下载](./tunnel-download.md) - 批量导出表数据
- [Storage API 读取](./storage-api-read.md) - 高性能并行读取
- [Blob 数据下载](./blob.md) - 非结构化二进制数据读取
