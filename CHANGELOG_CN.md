# 更新日志

## [0.47.0-public] - 2024-04-08

### 新增
- 对 Stream 新类型的支持，可用于进行增量查询。
- 在 `TableTunnel` 中增加了 `preview` 方法，用于数据预览。
- 引入 `OdpsRecordConverter`，用于对 Record 进行解析和格式化。
- `Projects` 类增加了 `create`（创建）和 `delete`（删除）方法，`update` 方法现已公开。`group-api` 包下的相关操作已被标记为弃用。
- `Schemas` 类增强，支持通过设置 `SchemaFilter` 来过滤 schema，支持 `listSchema` 以及获取 schema 的详细元信息。
- `DownloadSession` 新增参数 `disableModifiedCheck`，用于跳过修改检查。新增参数 `fetchBlockId`，用于跳过获取 block ID 列表。
- `TableWriteSession` 支持写入 `TIMESTAMP_NTZ` / `JSON` 类型，新增参数 `MaxFieldSize`。

### 变更
- `Table` 类的 `read` 方法实现现已更换为 `TableTunnel.preview` 方法，会支持 MaxCompute 新类型，时间类型切换为 Java 8 无时区类型。
- 默认的 `MapWritable` 实现从 `HashMap` 改为 `LinkedHashMap`，以确保有序。
- `Column` 类现支持使用建造者模式（Builder pattern）进行创建。

### 改进
- `TableReadSession` 现支持谓词下推，新增参数 `maxBatchRawSize` 和 `splitMaxFileNum`。
- `UpsertSession` 现支持：
  - 写入部分列。
  - 设置 Netty 线程池的数量（默认更改为 1）。
  - 设置最大并发量（默认值更改为 16）。
- `TableTunnel` 支持设置 `quotaName` 选项。

