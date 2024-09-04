---
title: 更新日志
sidebar_position: 6
---

# 更新日志
## [0.48.8-public] - 2024-08-12
### 增强
- 引入了对复合谓词表达式的内部验证，修复了处理无效或总是真/假谓词时的逻辑，增强了测试覆盖，确保了在复杂查询优化中的稳定性和准确性。


## [0.48.7-public] - 2024-08-07

### 增强

- **TableTunnel 配置优化**：引入 `tags` 属性至 `TableTunnel Configuration`
  ，旨在允许用户为隧道相关操作附上自定义标签。这些标签会被记录在租户层级的 `information schema`
  中，便于日志追踪与管理。

```java
Odps odps;
    Configuration configuration=
    Configuration.builder(odps)
    .withTags(Arrays.asList("tag1","tag2")) // 使用 Arrays.asList 以提升代码规范性
    .build();
    TableTunnel tableTunnel=odps.tableTunnel(configuration);
// 继续执行隧道相关操作
```

- **Instance 增强**：在 `Instance` 类中新增 `waitForTerminatedAndGetResult` 方法，此方法整合了 0.48.6
  及 0.48.7 版本中对 `SQLExecutor`
  接口的优化策略，提升了操作效率。使用方式可参考 `com.aliyun.odps.sqa.SQLExecutorImpl.getOfflineResultSet`
  方法。

### 优化

- **SQLExecutor 离线作业处理优化**：显著减少了端到端延迟，通过改进使得由 `SQLExecutor`
  执行的离线作业能在关键处理阶段完成后即刻获取结果，无需等待作业全部完成，提高了响应速度和资源利用率。

### 修复

- **TunnelRetryHandler NPE修复**：修正了 `getRetryPolicy` 方法中在错误码 (`error code`) 为 `null`
  的情况下潜在空指针异常问题。

## [0.48.6-public] - 2024-07-17

### 新增

- **支持序列化**：
    - 主要数据类型如 `ArrayRecord`、`Column`、`TableSchema` 和 `TypeInfo` 现在支持序列化和反序列化，能够进行缓存和进程间通信。
- **谓词下推**：
    - 新增 `Attribute` 类型的谓词，用于指定列名。

### 变更

- **Tunnel 接口重构**：
    - 重构了 Tunnel 相关接口，加入了无感知的重试逻辑，大大增强了稳定性和鲁棒性。
    - 删除了 `TunnelRetryStrategy` 和 `ConfigurationImpl` 类，分别被 `TunnelRetryHandler`
      和 `Configuration` 所取代。

### 优化

- **SQLExecutor 优化**：
    - 在使用 `SQLExecutor` 接口执行离线 SQL 作业时进行优化，减少每个作业获取结果时的一次网络请求，从而减少端到端延时。

### 修复

- **Table.read Decimal 读取**：
    - 修复了 `Table.read` 接口在读取 `decimal` 类型时，后面补零不符合预期的问题。

## [0.48.5-public] - 2024-06-17

### 新增

- `Table` 接口新增 `getPartitionSpecs` 方法, 相比 `getPartitions` 方法，该方法无需获取分区的详细信息，进而获得更快的执行速度

### 变更

- 移除了`Column`类中的`isPrimaryKey`
  方法。这个方法最初是为了支持用户在创建表时指定某些列为主键。然而，在读取场景下，这个方法容易引起误解，因为它并不会与服务端通信，所以当用户希望知道某列是否为主键时，这个方法并不适用。此外，在使用该方法建表时，主键应当是表级别的字段（因为主键是有序的），而该方法忽略了主键的顺序，设计上不合理。因此，在0.48.5版本中移除了该方法。
  在读取场景，用户应当使用`Table.getPrimaryKey()`方法来获取主键。
  在建表场景，改为在`TableCreator`中增加`withPrimaryKeys`方法以达成建表时指定主键的目的。

### 修复

修复了`RecordConverter`在format String类型的`Record`，当数据类型为`byte[]` 时，会抛出异常的问题

## [0.48.4-public] - 2024-06-04

### 新增

- 使用 `table-api` 写MaxCompute表，现在支持`JSON`和`TIMESTAMP_NTZ`类型
- `odps-sdk-udf` 功能继续完善

### 变更

- Table.read() 接口在遇到 Decimal 类型时，目前将默认去掉尾部的 0（但不会使用科学计数法）

### 修复

- 修复了 ArrayRecord 针对 JSON 类型不支持 getBytes 方法的问题

## [0.48.3-public] - 2024-05-21

### 新增

- 在构建UpsertSession时，现在支持传入 `retryStrategy`。

### 变更

- `UpsertStream.Listener` 的 `onFlushFail(String, int)` 接口被标记为了 `@Deprecated`
  ，使用 `onFlushFail(Throwable, int)` 接口替代。该接口将在 0.50.0 版本中移除。
- Tunnel upsert 的默认压缩算法更改为 `ODPS_LZ4_FRAME`。

### 修复

- 修复了 Tunnel upsert 当压缩算法不为 `ZLIB` 时，数据无法正确写入的问题。
- 修复了 UpsertSession 当用户未显式调用 `close` 时，资源长时间无法释放的问题。
- 修复了 Tunnel 获取数据相关接口（`preview`，`download`），当遇到表内存在不合法 `Decimal`
  类型时（如 `inf`，`nan`），会抛出异常的问题，现在会返回 `null`（与 `getResult` 接口一致）。

## [0.48.2-public] - 2024-05-08

### 重要修复

- 修复了Tunnel
  upsert时，对DATE、DATETIME类型的主键进行分桶时，依赖用户本地时区的问题。这可能导致分桶有误，导致数据查询异常。强烈建议依赖该特性的用户升级到0.48.2版本。

### 新增

- `Table`增加获取分层存储的lifecycle配置的方法`getTableLifecycleConfig()`。
- `TableReadSession` 现支持谓词下推了

## [0.48.1-public] - 2024-05-07

### 新增

Arrow和ANTLR库：在 Maven Shade
插件配置中添加了新的包含项，以更好地处理和打包特定库。这些包含项确保某些关键库被正确地打包进最终的遮蔽(
Shaded)构件中。新加入的库包括：

- org.apache.arrow:arrow-format:jar
- org.apache.arrow:arrow-memory-core:jar
- org.apache.arrow:arrow-memory-netty:jar
- org.antlr:ST4:jar
- org.antlr:antlr-runtime:jar
- org.antlr:antlr4:jar
- org.antlr:antlr4-runtime:jar

### 位置调整

ANTLR和StringTemplate的遮蔽重定位：配置现在包括针对 org.antlr 和 org.stringtemplate.v4
包的更新重定位规则，以防止可能在类路径中存在的这些库的其他版本的潜在冲突。新的遮蔽模式是：
org.stringtemplate.v4 重定位至 com.aliyun.odps.thirdparty.org.stringtemplate.v4
org.antlr 重定位至 com.aliyun.odps.thirdparty.antlr

## [0.48.0-public] - 2024-04-22

### 新增

- 引入了`odps-sdk-udf`模块，支持在UDF中按批读取MaxCompute数据，能在大数据量场景下显著提高性能。
- `Table`现支持获取`ColumnMaskInfo`，用于数据脱敏场景，方便相关信息的获取。
- 新增通过`odps.getRestClient().setProxy(Proxy)`方法设置代理的支持。
- 实现了可迭代的`RecordReader`以及`RecordReader.stream()`方法，允许将其转换为`Record`对象的流。
- 在`TableAPI RestOptions`中新增`upsertConcurrentNum`和`upsertNetworkNum`
  参数，为使用TableAPI进行upsert操作的用户提供更细致的控制。
- 支持使用`Builder`模式来构建`TableSchema`。
- `ArrayRecord`支持`toString`方法。

### 变更

- 现在，当用户使用`StsAccount`但不传递`StsToken`时，将被视作使用`AliyunAccount`。

### 改进

- `UploadSession`现支持配置`GET_BLOCK_ID`参数，当客户端不需要`blockId`时，可以加速创建Session的速度。
- 使用`builder`模式(`TableCreator`)加强了表的创建方法，现在可以更简单地创建表了。

### 修复

- 修复了`Upsert Session`获取连接时，超时时间配置错误的问题。
- 修复了`TimestampWritable`在纳秒为负数时计算出错一秒的问题。

## [0.47.0-public] - 2024-04-08

### 新增

- 对 Stream 新类型的支持，可用于进行增量查询。
- 在 `TableTunnel` 中增加了 `preview` 方法，用于数据预览。
- 引入 `OdpsRecordConverter`，用于对 Record 进行解析和格式化。
- `Projects` 类增加了 `create`（创建）和 `delete`（删除）方法，`update` 方法现已公开。`group-api`
  包下的相关操作已被标记为弃用。
- `Schemas` 类增强，支持通过设置 `SchemaFilter` 来过滤 schema，支持 `listSchema` 以及获取 schema
  的详细元信息。
- `DownloadSession` 新增参数 `disableModifiedCheck`，用于跳过修改检查。新增参数 `fetchBlockId`，用于跳过获取
  block ID 列表。
- `TableWriteSession` 支持写入 `TIMESTAMP_NTZ` / `JSON` 类型，新增参数 `MaxFieldSize`。
- `TABLE_API` 新增 `predicate` 相关类，用于后续支持谓词下推。

### 变更

- `Table` 类的 `read` 方法实现现已更换为 `TableTunnel.preview` 方法，会支持 MaxCompute 新类型，时间类型切换为
  Java 8 无时区类型。
- 默认的 `MapWritable` 实现从 `HashMap` 改为 `LinkedHashMap`，以确保有序。
- `Column` 类现支持使用建造者模式（Builder pattern）进行创建。

### 改进

- `TableReadSession` 新增参数 `maxBatchRawSize` 和 `splitMaxFileNum`。
- `UpsertSession` 现支持：
    - 写入部分列。
    - 设置 Netty 线程池的数量（默认更改为 1）。
    - 设置最大并发量（默认值更改为 16）。
- `TableTunnel` 支持设置 `quotaName` 选项。

