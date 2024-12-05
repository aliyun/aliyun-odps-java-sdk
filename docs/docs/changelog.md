---
title: 更新日志
sidebar_position: 6
---

# 更新日志
## [0.51.0-public] - 2024-12-05

### 功能
- **MapReduce** 支持多重管道输出 (multi pipeline output)。
- **VolumeBuilder** 新增 `accelerate` 方法，用于在 external volume 过大时，使用 dragonfly 加速下载过程。
- **Table** 新增 `TableType OBJECT_TABLE` 和判断方法 `isObjectTable`。
- **Project** `list` 方法增加过滤条件 `enableDr`，用于过滤项目是否开启存储容灾。
- **Cluster** 新增字段 `clusterRole`、`jobDataPath`、`zoneId`。

### 变更
- **TableBatchReadSession** 类变量 `predicate` 现在设置为 transient。
- **Attribute** 增加转义逻辑，并不再会 double quote。
- **SQLTask** 恢复了在 0.49.0 版本移除的 `SQLTask.run(Odps odps, String project, String sql, String taskName, Map<String, String> hints, Map<String, String> aliases, int priority)` 方法，以解决用户的 MR 作业依赖老版本 SDK 时可能发生的接口冲突问题。

### 修复
- **Table.changeOwner** 修复 SQL 拼写错误。
- **Instance.getTaskSummary** 移除自 0.50.2 版本开始的不合理打印的 debug 日志。
- **TruncTime** 在建表/toString 时，使用反引号对 `columnName` 进行 quote。
> **注意：** 此版本还包括“0.51.0-public.rc0”和“0.51.0-public.rc1”的所有更改。

## [0.50.6-public] - 2024-11-27
- **Logview** 新增对 Logview V2 的支持，V2 版本保障了数据安全，更多信息参考 [2024年11月14日-MaxCompute Logview安全升级](https://help.aliyun.com/zh/maxcompute/product-overview/2024-service-notices) 。可以通过 `new Logview(odps, 2)` 创建，SQLExecutor 通过 `logviewVersion` 方法指定。

## [0.51.0-public.rc1] - 2024-11-22
### 功能与变更
- **Column** `ColumnBuilder` 新增 `withGenerateExpression` 方法，用于构造 auto-partition 列
- **TableSchema**
  - 新增 `generatePartitionSpec` 方法，用于从`Record`中生成分区信息
  - `setPartitionColumns` 方法现在接收`List<Column>`，而不是`ArrayList<Column>`
- **TableCreator**
  - 新增对`GenerateExpression`的支持，新增方法`autoPartitionBy`，现在可以创建 AutoPartition 表了
  - 新增对`ClusterInfo`的支持，现在可以创建 Hash/Range Cluster 表了
  - 新增指定 `TableFormat`，现在可以指定创建`APPEND`,`TRANSACTION`,`DELTA`,`EXTERNAL`,`VIEW`格式的表
  - 新增`selectStatement`参数，用于`create table as` 和 `create view as` 场景
  - 新增`getSql`方法，用于获取创建表的 SQL 语句
  - 现在会对所有的 `Comment` 参数进行 quote，以支持包含特殊字符的 `Comment` 参数
  - 将 DataHub 相关的建表参数（`hubLifecycle`, `shardNum`) 整合为 `DataHubInfo`
  - 重命名`withJars`方法为`withResources`，以表示不仅可以使用JAR类型资源
  - 重命名`withBucketNum`方法为`withDeltaTableBucketNum`，以表示该方法仅用于 Delta Table
  - 修改了 `withHints`，`withAlias`，`withTblProperties`，`withSerdeProperties` 方法的逻辑，现在会覆盖之前设置的值，而不是合并
  - 移除了`createExternal`方法，现在使用`create`方法即可
- **Table**
  - 新增 `getSchemaVersion` 方法，用户获取当前表结构的版本，用户每次进行 SchemaEvolution 都会更新版本号，目前该字段仅用于在创建 StreamTunnel 时指定
  - 新增 `setLifeCycle`，`changeOwner`，`changeComment`，`touch`，`changeClusterInfo`，`rename`，`addColumns`，`dropColumns`方法，以支持对表结构进行修改
- **StreamTunnel** 修改初始化逻辑，当指定 `allowSchemaMismatch` 为 `false` 时，会自动重试直到使用最新版本的表结构(超时时间为5min)

### 修复
- **GenerationExpression** 修复了当建表时`TruncTime`为大写，reload table 会抛出异常的问题
- **TypeInfoParser** 能够正确处理 `Struct` 类型，字段被反引号quote的 `TypeInfo`了

## [0.51.0-public.rc0] - 2024-11-18

### 功能
- **GenerateExpression** 增加对分区列的生成列表达式功能的支持，和第一个生成列表达式`TruncTime`，使用方式请参考[Example](https://github.com/aliyun/aliyun-odps-java-sdk/blob/v0.51.0-public/odps-examples/basic-examples/src/main/java/GenerateExpressionSample.java)
- **UpsertStream** 支持写入主键为 `TIMESTAMP_NTZ` 类型的值
- **Table** 新增对 cdc 相关数据的查询，`getCdcSize()`，`getCdcRecordNum()`，`getCdcLatestVersion()`，`getCdcLatestTimestamp()`
- **SQLExecutor** MCQA 2.0 作业支持获取 InstanceProgress 信息

### 变更
- **Quote** 对 Struct 类型的 TypeInfo，和其他拼装 SQL 的方法，使用反引号对名字进行 quote
- **AutoClosable** 为了提醒用户正确关闭资源，对下列资源类，增加了相应的 `close()` 方法，以提醒用户正确关闭资源。
  - `odps-sdk-core` 包下的 `UpsertStream`，
  - `odps-sdk-impl` 包下的 `LocalOutputStreamSet`，`ReduceDriver.ReduceContextImpl`，`MapDriver.DirectMapContextImpl`，`LocalRecordWriter`
  - `odps-sdk-udf` 包下的 `VectorizedOutputer`，`VectorizedExtractor`，`RecordWriter`，`RecordReader`，`Outputer`，`Extractor`

## [0.50.5-public] - 2024-11-13

### 功能

- **TableAPI**
  为可以安全重试的网络请求类型的报错增加了相应的重试逻辑，从而提高了接口的稳定性。在 `RestOptions`
  中增加了 `retryWaitTimeInSeconds` 配置项，用于设置重试等待时间。
- **SQLTask** 新增了 `run` 方法的重载，支持传入 `mcqaConnHeader` 参数，以便提交 MCQA 2.0 作业。
- **SQLExecutor** 支持通过指定 `hints` 中的 `odps.task.wlm.quota` 来设置提交 MCQA 2.0 作业时的
  interactive quota。
- **RestClient** 新增了 `retryWaitTime` 参数，以及相应的 getter 和 setter 方法，以配置网络请求的重试等待时间。
- **Configuration** 新增了 `socketRetryTimes` 参数以及相应的 getter 和 setter 方法，用于配置 Tunnel
  网络请求的重试等待时间。如果未设置，则使用 `RestClient` 中的配置，否则使用此配置。

### 变更

- **Instances** 移除了 `get`
  的重载方法 `get(String projectName, String id, String quotaName, String regionId)`
  ，该方法在 `0.50.2-public` 版本中新增，用于获取 MCQA 2.0 实例。现在，用户在使用 `get` 方法时无须区分作业是否为
  MCQA 2.0 作业，因此移除该方法可以直接使用 `get(String projectName, String id)` 方法来获取实例。

### 修复

- **Table.read()** 修复了在数据预览时，配置的网络相关参数（如超时时间、重试逻辑）无法正确生效的问题。
- **Streams** 修复了 `create` 方法中，如果指定了 `version` 会报错的问题。同时增加了 `version`
  的默认值（1），表示表的初始版本。

## [0.50.4-public] - 2024-10-29

### 功能

- **PartitionSpec** 新增`(String, boolean)`
  构造方法，通过布尔参数指定是否对分区值进行trim操作，以满足某些场景（如使用char类型作为分区字段）用户不希望trim的需求。

### 变更

- **Instance** 在调用stop方法时，抛出的OdpsException将不再被二次包装。

### 修复

- **SQLExecutor**
  - 修复了在MCQA 1.0模式下，用户指定`fallbackPolicy.isFallback4AttachError`时未正确生效的问题。
  - 修复了在MCQA 2.0模式下，作业失败时`cancel`方法抛出异常的问题。
  - 修复了在MCQA 2.0模式下，当isSelect判断错误时，通过instanceTunnel取结果报错的问题。
- **Table** 修复了`getPartitionSpecs`方法会trim分区值，导致无法获取存在的分区的问题。

## [0.50.3-public] - 2024-10-23
### 功能
- **SQLExecutor** 在 MCQA 1.0 模式下，允许增加自定义回退策略，新增类`FallbackPolicy.UserDefinedFallbackPolicy`。

## [0.50.2-public] - 2024-10-23
### 功能
- **SQLExecutor** 增强 MCQA 2.0 功能：
  - `isActive` 将返回 false，指示在 MCQA 2.0 模式下没有活跃的 Session。
  - 新增 `cancel` 方法，用于中止正在执行的作业。
  - `getExecutionLog` 现在返回当前日志的深拷贝并清空当前日志，避免重复获取。
  - 在 `SQLExecutorBuilder` 新增 `quota` 方法，支持复用已加载的 `Quota`，减少加载时间。
  - 在 `SQLExecutorBuilder` 新增 `regionId` 方法，允许指定 quota 所在的 region。
- **Quotas** 新增带 `regionId` 参数的 `getWlmQuota` 方法，用于获取指定 regionId 的 quota。
- **Quota** 新增 `setMcqaConnHeader` 方法，支持用户通过自定义的 McqaConnHeader 重载 quota，以适配 MCQA 2.0。
- **Instances** 新增适用于 MCQA 2.0 的 `get` 方法，需额外传入 MCQA 2.0 的 QuotaName 和 RegionId。
- **Instance** 进一步适配 MCQA 2.0 作业。
- **TableSchema** `basicallyEquals` 方法将不再严格检查两个类的 Class 类型一致性。
### 优化
- **SQLExecutor** `run` 方法中的 hints 现在会进行深拷贝，保护用户传入的 Map，支持不可变类型（如 `ImmutableMap`）。
### 修复
- **Stream** 修复 `create` 方法中的潜在 SQL 语法错误。

## [0.50.1-public] - 2024-10-11

### 修复

-  **TableAPI** 修复了使用`SplitRecordReaderImpl`获取结果时，拿到了`ArrayRecord`无法正确`toString`的问题。
-  **TableAPI** 修复了使用`SplitRecordReaderImpl`获取结果时，如果`Split`对应的`Record`数量为0，在`get`
   操作时会抛出数组越界异常的问题。
-  **TableAPI** 修复了复合谓词`CompositePredicate`在遇到空谓词时，可能额外增加一次操作符的问题。

## [0.50.0-public] - 2024-10-09

### 功能
- 新增 `SchemaMismatchException` 异常：当使用 `StreamUploadSession` 时，如果用户上传的 Record 结构与表结构不匹配，将抛出该异常。此异常将额外携带最新的 schema version，方便用户重建 Session 并进行重试操作。
- 在 `StreamUploadSession.Builder` 中新增 `allowSchemaMismatch` 方法，用于指定是否容忍用户上传的 Record 结构与表结构不匹配时是否抛出异常。默认值为 `true`。

### 修复
- 修复了在 Odps 中指定 `tunnelEndpoint` 时，使用 `StreamUploadSession` 无法生效的问题。
- 修复了 `TunnelRetryHandler` 潜在的 NPE 问题。


## [0.50.0-rc1] - 2024-09-19
### 功能
- **SQLExecutor** 新增 `isUseInstanceTunnel` 方法：
  - 用来判断是否使用 instanceTunnel 取结果
### 修复
- 修复了使用 SQLExecutor 执行 MCQA 2.0 作业时，执行 CommandApi 任务会影响下一次作业，导致取结果时抛出NPE的问题。

## [0.50.0-rc0] - 2024-09-18

### 功能
- **SQLExecutor** 支持提交 MCQA 2.0 作业
  - SQLExecutorBuilder 新增方法 `enableMcqaV2`
  - SQLExecutorBuilder 新增对字段的 getter 方法
- SQLExecutor 新增 `getQueryId` 方法：
  - 对于离线作业和 MCQA 2.0 作业，会返回当前执行的作业 InstanceId
  - 对于 MCQA 1.0 作业，会返回 InstanceId 和 SubQueryId
- **TableAPI** `EnvironmentSettings` 新增 `SharingQuotaToken` 参数，以支持提交作业时携带Quota资源共享临时凭证
- **Quotas** 新增 `getWlmQuota` 方法：
  - 能够根据 projectName 和 quotaNickName 获取到 quota 的详细信息，比如是否属于交互式 quota
- **Quota 类**新增 `isInteractiveQuota` 方法，用来判断 quota 是否属于交互式 quota（适用于 MCQA 2.0）
- 新增 `getResultByInstanceTunnel(Instance instance, String taskName, Long limit, boolean limitEnabled)` 方法：
  - 用来无限制地通过 instanceTunnel 获取结果（解除限制需要更高的权限）
- **UpsertSession.Builder** 新增 `setLifecycle` 方法，用来配置 Session 生命周期

### 修复
- 修复了使用 SQLExecutor 执行离线作业时，指定 `limitEnabled` 取结果但不生效的问题
- 修改了 SQLExecutor 执行离线作业时，`getQueryId` 方法会返回作业的 instanceID 而非 null
- 修复了 SQLExecutor 执行离线作业时，当遇到非 select 语句时，使用 instanceTunnel 取结果不再抛出异常，而是回退到非 tunnel 逻辑
- 修复了使用 DownloadSession 下载数据时，发生错误且读取数量刚好等于要读取记录的数量 - 1 时重建漏掉一条数据的问题
- **Odps 类**的 `clone` 方法现在能正确克隆包括 `tunnelEndpoint` 等其他字段
- **Instance** 的 `getRawTaskResults` 方法现在在处理同步作业时不会多次发起请求

## [0.49.0-public] - 2024-09-12

### 功能

- **OdpsRecordConverter 功能增强**：现在支持将数据转换为 SQL 兼容格式，比如对于 `LocalDate`
  类型，数据可以转换为 `"DATE 'yyyy-mm-dd'"` 格式。同时对于 `Binary` 类型，现在支持了 hex 表示格式。

- **开放存储谓词下推常量增强**：改进了 `Constant` 类行为，新增了 `Constant.of(Object, TypeInfo)`
  方法。现在当设定或识别出类型为时间类型时，可以正确转变为 SQL
  兼容格式（也就是可以正确下推时间类型了）。同时修复了一些其他类型的问题，当无法转换成 SQL
  兼容模式时，会在创建 `Session` 的时候抛出 `IllegalArgumentException`。

- **UpsertSession 实现 Closable 方法**：提醒用户应当正确释放 UpsertSession 的本地资源。

- **SQLExecutorBuilder 新增方法** `offlineJobPriority`：用来设置当作业发生回退时，离线作业的优先级。

- **Table 类新增方法** `getLastMajorCompactTime`：用来获取表最后一次 major compact 的时间。

- **Instance 类新增方法** `create(Job job, boolean tryWait)`：当用户执行 `tryWait` 为 `true`
  时，作业会尝试在服务端等待一段时间，以更快获取结果。

- **Resource 类增强**：现在能够判断对应的资源是否属于临时资源。

- **CreateProjectParma 类增强** 新增`defaultCtrlService`参数，用来指定项目的默认控制集群。

### 修复

- **UpsertStream NPE 修复**：修复了在 flush 时，当发生本地错误时抛出 NPE 而无法正确重试的问题。

- **Varchar/Char 类型修复**：修复了 `Varchar/Char` 类型获取其长度时，当遇到中文符号或表情等特殊字符，会错误的计算两次的问题。

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

