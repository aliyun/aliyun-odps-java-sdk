---
title: 写入数据
description: MaxCompute Java SDK 写入数据方式概览与选型指南
sidebar_position: 1
task: write-data
keywords: [写入, 导入, upload, stream, upsert, storage-api, 选型]
---

# 写入数据

MaxCompute Java SDK 提供了多种数据写入方式，适用于不同的业务场景。本文帮助您快速了解各方式的特点并选择最合适的方案。

## 写入方式对比

| 方式 | 适用场景 | 数据可见性 | 事务性 | 性能 |
|------|---------|-----------|--------|------|
| [Tunnel Upload](./tunnel-upload.md) | 批量导入 | 提交后可见 | 原子提交 | 中 |
| [Tunnel Stream](./tunnel-stream.md) | 实时写入 | flush 后可见 | 非事务 | 高 |
| [Tunnel Upsert](./tunnel-upsert.md) | 更新插入（Delta Table） | flush 后可见 | 行级 | 高 |
| [Storage API](./storage-api-write.md) | 高性能写入 | 配置决定 | 可选 | 最高 |

## 选型指南

### 按场景选择

- **离线批量导入**：选择 [Tunnel Upload](./tunnel-upload.md)。支持原子提交，可确保数据要么全部可见，要么全部不可见，适合 ETL 作业和定期数据同步。

- **实时/准实时写入**：选择 [Tunnel Stream](./tunnel-stream.md)。数据写入后 flush 即可见，无需等待 commit，适合日志采集、实时监控等场景。

- **数据更新与删除**：选择 [Tunnel Upsert](./tunnel-upsert.md)。针对 Delta Table（主键表）设计，支持按主键进行 insert/update/delete 操作，适合数据库变更同步（CDC）场景。

- **超高吞吐写入**：选择 [Storage API](./storage-api-write.md)。基于 Arrow 列式格式，支持批量和流式两种模式，提供最高的写入性能，适合大规模数据导入和高频写入场景。

### 按特性选择

| 需求 | 推荐方式 |
|------|---------|
| 需要原子性保证（全部成功或全部失败） | Tunnel Upload / Storage API (Batch) |
| 写入后立即可查 | Tunnel Stream / Tunnel Upsert / Storage API (Streaming) |
| 需要更新或删除已有数据 | Tunnel Upsert |
| 追求最大写入吞吐 | Storage API |
| 多线程并行写入 | 以上均支持 |
| 分区表动态分区写入 | Tunnel Stream（0.55.0+） |

## 相关文档

- [Tunnel Upload 批量写入](./tunnel-upload.md)
- [Tunnel Stream 流式写入](./tunnel-stream.md)
- [Tunnel Upsert 更新插入](./tunnel-upsert.md)
- [Storage API 高性能写入](./storage-api-write.md)
