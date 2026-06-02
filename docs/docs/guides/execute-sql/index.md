---
title: 执行 SQL
sidebar_position: 1
description: MaxCompute Java SDK 提供多种 SQL 执行模式，本文帮助您选择最适合的方式。
task: execute-sql
keywords:
  - SQL
  - SQLTask
  - SQLExecutor
  - MCQA
  - MaxQA
  - 离线作业
  - 交互式查询
---

# 执行 SQL

MaxCompute Java SDK 支持三种 SQL 执行模式，分别面向不同的延迟和吞吐场景。本文将帮助您了解各模式的特点并选择最合适的执行方式。

## 模式对比

| 模式 | 延迟 | 适用场景 | 结果获取 | 入口类 |
|------|------|----------|----------|--------|
| 离线 (SQLTask) | 秒~分钟 | ETL / 大查询 / 批量计算 | InstanceTunnel | `SQLTask` / `SQLExecutor` |
| MCQA v1 | 毫秒~秒 | 交互式查询 (基于 Session) | 直接返回 | `SQLExecutor` |
| MCQA v2 (MaxQA) | 毫秒~秒 | 交互式查询 (无 Session) | 直接返回 | `SQLExecutor` |

## 如何选择

### 使用离线模式的场景

- 数据量大（GB、TB 级别以上）的批量 ETL 作业
- 对延迟不敏感，追求最大吞吐量
- 需要复杂的多阶段 SQL 执行计划
- 需要使用 `SQLTask` 原始接口进行精细控制

### 使用 MCQA（交互式）模式的场景

- 数据量中、小的即席查询
- 对延迟敏感，期望秒级返回
- BI 报表、数据探索等交互式分析场景
- 需要连续执行多条 SQL 查询

### MCQA v1 与 v2 的区别

- **MCQA v1**：基于 Session 机制，需要先创建/连接 Session，后续查询在同一 Session 中执行。适合需要长时间保持会话的场景。
- **MCQA v2 (MaxQA)**：无需 Session，每次查询独立提交，使用更简单。通过 Virtual Warehouse 技术使用独享计算资源，减少加速失败。

:::tip 推荐
如果您的项目已开通 MaxQA（MCQA 2.0）资源组，推荐优先使用 MCQA v2 模式，配置更简单且稳定性更高。
:::

## 统一入口：SQLExecutor

SDK 提供了 `SQLExecutor` 作为统一的 SQL 执行接口，通过 `SQLExecutorBuilder` 配置不同的 `ExecuteMode` 即可切换执行模式：

```java
// 离线模式
SQLExecutor offline = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.OFFLINE)
    .build();

// MCQA v1 模式
SQLExecutor mcqa = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE)
    .build();

// MCQA v2 (MaxQA) 模式
SQLExecutor maxqa = SQLExecutorBuilder.builder()
    .odps(odps)
    .executeMode(ExecuteMode.INTERACTIVE)
    .quotaName("your_interactive_quota")
    .enableMcqaV2(true)
    .build();
```

## 相关文档

- [离线模式详细指南](./offline.md)
- [MCQA 交互式查询指南](./mcqa.md)
- [SQLExecutor 高级用法](./sql-executor.md)
- [SQLExecutor API 参考](../../api-reference/SQLExecutor.md)
- [MaxCompute SQL 概述（阿里云官方文档）](https://help.aliyun.com/zh/maxcompute/user-guide/overview-of-maxcompute-sql)
- [查询加速 MCQA（阿里云官方文档）](https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-query-acceleration)
