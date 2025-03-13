---
title: 使用 Tunnel 对表进行上传下载
sidebar_position: 3
---

# TableTunnel 文档

## 概述

MaxCompute 提供了高效、灵活的数据通道 Tunnel，以满足不同场景下的数据导入，数据下载需求。

本指南将介绍如何使用 SDK 利用 TableTunnel 实现数据上传和下载功能。

## 初始化

在使用 `TableTunnel` 之前，需要进行 Odps 对象的初始化操作，对 Odps 对象的初始化操作参考[初始化 Odps 对象](init-odps-client.md)。

根据是否需要自定义TableTunnel的配置，可以使用如下两个接口来初始化TableTunnel。

**无参初始化**
```java
TableTunnel tunnel = odps.tableTunnel();
```

**使用配置文件进行初始化**
```java
// com.aliyun.odps.tunnel.Configuration;
Configuration configuration=Configuration.builder(odps).build();
TableTunnel tunnel = odps.tableTunnel(configuration);
```
有关配置文件的配置项，请参考[Configuration 文档](../api-reference/tunnel/Configuration.md)。


## 使用方式
- 有关 TableTunnel 的初始化方法，和使用方式，请参考[TableTunnel 文档](../api-reference/tunnel/TableTunnel.md)。

## 数据上传

目前，MaxCompute 根据使用场景的不同，提供了两种上传模式（StreamingTunnel 和 BatchTunnel
）以及针对不同表类型的写入模式（upload 和 upsert）。

TableTunnel 根据[上传模式](#上传模式)，[写入模式](#写入模式)的不同，提供了三种写入类（Session）。

- StreamUploadSession
- UploadSession
- UpsertSession

### 上传模式

#### StreamingTunnel

**特点:**
- **自动提交（Auto-commit）:** 数据写入后无需commit，由服务端自动进行提交。更加适合实时或近实时的数据传输场景。
- **适用场景:** 需要快速响应和即时查看数据变更的应用，如实时数据分析、流式处理等。

#### BatchTunnel

**特点:**
- **手动提交（Commit Required）:** 数据在显式调用 commit 操作后才对查询可见，类似于文件批量处理。
- **适用场景:** 大量数据批处理作业，如定期的数据同步、ETL（提取、转换、加载）任务等。

### 写入模式

#### upload（插入/追加）

- **别名:** 也称为 `insert` 或 `append`。
- **适用表类型:** MaxCompute 普通表（无主键约束）。
- **描述:** 简单地向表中追加新数据行，不考虑现有数据。适用于需要收集新增数据而无需考虑数据是否存在的情况。

#### upsert（更新/插入）

- **包含语义:** 结合了 `update` 和 `insert` 的特性。
- **适用表类型:** MaxCompute Delta Table（事务性表，具有主键约束）。
- **描述:** 当写入数据的主键与表中已有的记录匹配时，将更新该记录；如果主键不存在，则执行插入操作。此模式特别适合需要维护数据最新状态的场景，如用户信息更新、库存管理等。此外，`upsert` 还支持通过指定主键进行数据删除操作，进一步增强了数据管理的灵活性。

### 使用建议

- 根据目标表的类型（普通表或Transactional Table）及业务逻辑（是否需要更新已有记录）来决定采用 `upload` 或 `upsert` 写入模式。
- 明确您的业务需求是偏向实时性还是吞吐量，以此来决定使用 `StreamingTunnel` 或 `BatchTunnel`。
- 对于大量数据的处理，考虑使用 `BatchTunnel` 以减少网络交互次数和优化整体效率。
- 在实施 `upsert` 操作前，确保对表的主键设计有充分理解，以避免数据冲突和不必要的性能开销。

**目前 upsert 写入模式还不支持 `StreamingTunnel`。**

### 使用方法
- UpsertSession 使用方法，请参考[UpsertSession 文档](../api-reference/tunnel/UpsertSession.md)。
- UploadSession 使用方法，请参考[UploadSession 文档](../api-reference/tunnel/UploadSession.md)。
- StreamUploadSession 使用方法，请参考[StreamUploadSession 文档](../api-reference/tunnel/StreamUploadSession.md)。


## 数据下载
DownloadSession 作为 TableTunnel 的核心组件，提供了对表数据的下载功能。

有关 DownloadSession 的使用方法，请参考[DownloadSession 文档](../api-reference/tunnel/DownloadSession.md)。



