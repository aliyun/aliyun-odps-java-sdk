# 更新日志

## [0.57.2-public] - 2026-05-07

### 🐛 问题修复
* **[Instance]**: **为 MaxQA 实例添加凭据确保机制** - 当通过 `Instances.get(id)` 获取 MCQA 2.0 实例时，自动从响应头获取 `x-odps-mcqa-conn` 和 `x-odps-mcqa-query-cookie` 凭据，确保后续 `/mcqa` 前缀请求能正确路由。
    * *相关 API*: `Instance.ensureMaxQACredential()`

## [0.57.1-public] - 2026-04-08

### 📦 维护
* **[Storage API]**: 添加 Storage API 模块文档
* **[Build]**: 修复 POM 配置问题

## [0.57.0-public] - 2026-03-25

### ✨ 新功能
* **[Storage API][Preview]**: **全新 `odps-sdk-storage-api` 模块** - 引入高性能 Storage API 客户端 `MaxStorageClient`，基于 Arrow 列式格式对 MaxCompute 表进行读写。支持通过 InputSplit 分片实现分布式并行读取、写入会话的 commit/abort 生命周期管理、表数据预览、Blob 下载以及 Instance 结果读取。
    * *相关 API*: `MaxStorageClient`, `MaxStorageClient.Builder`, `TableReadSession`, `TableWriteSession`, `InstanceReadSession`, `BlobManager`
* **[Arrow Helper]**: **全新 `odps-arrow-helper` 模块** - 将 Arrow 相关工具类抽取为独立模块，包含 `TableIdentifier`、`InstanceIdentifier`、`StreamIdentifier`、各类型 Arrow Accessor、`ArrowReaderBuilder`、`ArrowStreamRecordReader`、`SchemaUtils` 等。
    * *相关 API*: `TableIdentifier`, `InstanceIdentifier`, `StreamIdentifier`, `ArrowReaderBuilder`
* **[SQLExecutor]**: **Storage API 结果集集成** - `SQLExecutorImpl` 在查询结果包含 `BLOB` 列时，自动通过 Storage API（`StorageAPIResultSet`）下载结果，提升兼容性与数据传输效率。
    * *相关 API*: `StorageAPIResultSet`, `InternalBlobHelper`
* **[SQLExecutor]**: **MaxQA Fallback 配置** - 新增 `FallbackInfo` 和 `MaxQAConnInfo`，支持配置 MaxQA Quota 的回退策略，当主 Quota 不可用时可自动回退到指定 Quota。
    * *相关 API*: `FallbackInfo`, `MaxQAConnInfo`, `SQLExecutorBuilder.maxQAConnInfo()`, `SQLExecutorBuilder.enableMaxQA()`
* **[RestClient]**: **请求/响应拦截器链** - 新增 `InterceptorChain`、`RequestInterceptor`、`ResponseInterceptor`、`InterceptorContext`，支持可插拔的 HTTP 请求响应拦截。
    * *相关 API*: `InterceptorChain`, `RequestInterceptor`, `ResponseInterceptor`
* **[Commons]**: **新增 `JsonString` 类型** - 新增 `JsonString`，作为轻量级 `JsonValue` 实现，封装原始 JSON 字符串，适用于无需解析 JSON 结构的场景。
    * *相关 API*: `JsonString`
* **[Commons]**: **新增 `RecordReader` / `RecordWriter` 接口** - 将通用的 `RecordReader` 和 `RecordWriter` 接口下沉至 `odps-sdk-commons`，便于跨模块复用。
    * *相关 API*: `RecordReader`, `RecordWriter`
* **[Odps]**: **Catalog API Host 及过期元数据读取配置** - `Odps` 新增 `setCatalogApiHost()` / `getCatalogApiHost()` 和 `setAllowStaleMetadataRead()` / `isAllowStaleMetadataRead()`，分别用于配置 Catalog API 端点和允许读取过期元数据。
    * *相关 API*: `Odps.setCatalogApiHost()`, `OdpsOptions.allowStaleMetadataRead`
* **[Quota]**: **获取 MaxQA 连接信息** - 新增 `Quotas.getMaxQAConnInfo(quotaName)`，支持获取指定 Quota 的 MaxQA 连接信息。
    * *相关 API*: `Quotas.getMaxQAConnInfo()`
* **[Partition]**: **分区状态管理** - 新增 `Partition.State` 枚举和 `setState()` 方法，支持设置分区状态。
    * *相关 API*: `Partition.State`, `Partition.setState()`

### 🚀 功能增强与性能优化
* **[OdpsType]**: **`OdpsType` 枚举新增数字编码** - 每个 `OdpsType` 枚举值携带稳定的整型 code，并支持通过 `OdpsType.fromCode(int)` 反向查找，便于序列化和协议兼容。
* **[Arrow Helper]**: **Arrow Accessor 层重构** - 将所有按类型实现的 Arrow 列访问器迁移至新的 `odps-arrow-helper` 模块，提升模块化程度和可复用性。
* **[TunnelBufferedWriter]**: **异步 flush 支持** - `TunnelBufferedWriter` 新增非阻塞异步 flush 能力，采用双 buffer 交换机制（`flush(boolean blocking)`），提供背压控制，提升流式上传的写入吞吐量。
    * *相关 API*: `TunnelBufferedWriter.flush(boolean blocking)`
* **[UpsertStream]**: **异步 flush 及 Buffer 优化** - `UpsertStreamImpl` 支持通过可配置的 `ExecutorService` 执行异步 flush，采用按 bucket 双 buffer 交换机制，并新增 `sync()` 方法支持显式屏障同步。
    * *相关 API*: `UpsertStream.Builder.setAsyncFlushService()`, `UpsertStreamImpl.sync()`

### 📦 依赖更新
* **新增**: `com.squareup.okhttp3:okhttp:4.12.0`（在 `odps-sdk-storage-api` 中已 shade）

## [0.56.1-public] - 2026-02-04

### 🐛 问题修复
* **[CVE]**: **更新 commons-io 至 2.20.0** - 通过将 commons-io 库升级到 2.20.0 版本，修复了安全漏洞。
* **[CVE]**: **使用 at.yawk.lz4 替代 org.lz4.pure-java** - 将 org.lz4.pure-java 依赖替换为 at.yawk.lz4，以解决安全漏洞问题。

## [0.56.0-public] - 2026-01-30

### ✨ 新功能
* **[Account]**: **重构认证体系**，采用新的 `Credentials` 类替代旧版 `Credential`，并新增 `getRegionId()` 方法支持区域 ID 配置，使认证机制更加灵活和标准化。
    * *相关 API*: `Account.getCredentials()`, `Account.getRegionId()`
* **[Instance]**: **增强 MaxQA 查询结果状态管理**，新增 `SelectResultStatus` 枚举以区分查询结果的完整状态（完整/截断/无结果），便于用户判断数据是否完整返回。
    * *相关 API*: `Instance.ResultDescriptor.SelectResultStatus`, `Instance.ResultDescriptor.getSelectResultStatus()`
* **[ProxyConfig]**: **新增 Netty DNS 解析器控制选项**，允许通过 `withDisableNettyLocalResolver()` 禁用本地 Netty DNS 解析器，适用于需要代理服务器进行 DNS 解析的场景。
    * *相关 API*: `ProxyConfig.Builder.withDisableNettyLocalResolver()`

### 🚀 功能增强与性能优化
* **[Arrow]**: **优化 Arrow 数据读取器**，统一使用 `CommonsCompressionFactory` 处理压缩数据，提升大规模数据读取性能和兼容性。
* **[Tunnel]**: **改进 Tunnel Endpoint 解析逻辑**，优先使用 `odps.getTunnelEndpoint()` 配置项，简化端点配置流程。
* **[RestClient]**: **优化重试等待时间计算**，仅使用连接超时时间而非连接+读取超时时间总和，使重试策略更加精准。
* **[TableSchema]**: **新增批量分区列配置方法** `withPartitionColumns()`，简化多分区列的创建流程。
    * *相关 API*: `TableSchema.Builder.withPartitionColumns()`

### 🐛 问题修复
* **[Instance]**: **修复任务失败时的错误处理**，在 `waitForSuccess()` 和 `isSelect()` 方法中增加 `checkTaskFailed()` 检查，确保在任务失败时能及时抛出异常并提供详细的失败信息。

### 📦 依赖更新
* **升级**: `commons-io`: `2.11.0` → `2.14.0`
* **替换**: `commons-lang:2.6` → `org.apache.commons:commons-lang3:3.18.0`
* **升级**: `io.netty:netty-all`: `4.1.119.Final` → `4.1.130.Final`
* **新增**: `org.locationtech.jts:jts-core:1.19.0`
* **替换**: `com.aliyun:aliyun-java-auth:0.2.16-beta` → `com.aliyun:credentials-api:1.0.0`

## [0.55.1-public] - 2026-01-22

### 🐛 问题修复
*   **Arrow 字典解码**
    修复了 ArrowBatchNonReusedReader 中字典解码未使用压缩工厂的问题，提升 Arrow 数据读取的稳定性和性能。

## [0.55.0-public] - 2025-12-12

### ✨ 新增功能
*   **Account 凭证信息获取**
    所有 Account 实现类新增 `getCredential()` 方法，支持获取当前账号的 AccessKey ID、AccessKey Secret 和 Security Token 信息，便于账号信息管理和安全审计。
*   **Tunnel 动态分区写入**
    新增 `DynamicPartitionRecordPack` 和 `PartitionRecord`，支持单次写入多个分区，大幅提升多分区数据写入效率。通过 `TableTunnel.StreamUploadSession.Builder.setDynamicPartition()` 启用。
*   **Tunnel Arrow 缓冲读取器**
    为非 Delta 表新增基于 Arrow 格式的高性能缓冲读取能力，优化大规模数据读取性能。通过 `DownloadSession.openBufferedArrowRecordReader()` 使用。
*   **Tunnel 批次追踪**
    `FlushResult` 接口新增 `getBatchId()` 方法，`StreamUploadSession` 新增 `getLastBatchId()` 和 `getLastBatchCommitTime()` 方法，支持追踪 flush 批次 ID 和提交时间。
*   **Arrow 异步流式读取**
    新增 `ArrowStreamAsyncReader` 类，支持在独立线程中异步读取 Arrow 数据流，提升 IO 密集型场景下的读取性能。通过 `ReaderOptions.Builder.withAsync()` 和 `withAsyncQueue()` 配置。
*   **性能监控指标**
    新增 `RateLimitCost` 和 `ServerProcessCost` 两个计数器指标，用于监控限流成本和服务器处理时间，帮助用户优化读取性能。
*   **Quota 自动扩容配置**
    `Quota` 类新增 `autoScaleCPULimit`、`autoScaleMemoryLimit` 和 `autoScaleGPULimit` 字段，支持获取项目的自动扩容配额信息。
*   **Project 属性获取改进**
    `Project.getAllProperties()` 方法现在会从服务器获取包含从 group 继承来的完整配置信息，提供更全面的项目属性视图。

### 🚀 功能优化
*   **Arrow 字典解码优化**
    `ArrowBatchNonReusedReader` 和 `ArrowBatchReusedReader` 改进了字典编码的处理逻辑，修复了字典解码未使用压缩工厂的问题，提升 Arrow 数据读取的稳定性和性能。
*   **TunnelRecordReader 增强**
    `DownloadSession` 新增 `openRecordReader()` 方法，支持通过 `sizeLimit` 参数控制读取的数据大小限制。
*   **缓冲读取器参数优化**
    `openBufferedRecordReader()` 方法新增 `bufferSize` 参数，允许用户更精细地控制缓冲区大小，优化内存使用。
*   **按原始大小读取支持**
    `DownloadSession` 新增 `isSupportReadByRawSize()` 方法，支持判断当前会话是否支持按原始数据大小进行读取。

### 🐛 问题修复
*   **Instance 异步任务结果获取**
    修复了在异步场景下调用 `getTaskResult()` 方法时，如果任务结果列表为空会返回 null 的问题，现在会抛出明确的异常信息。
*   **Arrow 字典编码字段处理**
    改进了 Arrow Reader 对嵌套结构中字典编码字段的处理逻辑，确保字典数据正确加载和解码。

## [0.54.0-public] - 2025-10-21

### ✨ 新增功能
*   **Java 21 支持**
    新增对 Java 21 的支持，添加了 JDK 21 配置文件和相关构建配置
*   **新数据类型支持**
    新增 GEOGRAPHY 和 BLOB 数据类型支持，扩展了数据处理能力
*   **AspectJ 支持**
    添加 AspectJ Maven 插件支持，为面向切面编程提供基础
*   **类加载器增强**
    改进了类加载器在不同 Java 版本中的兼容性，添加了 getLoadedJars 方法
*   **Blob 类**
    新增 Blob 类，用于处理存储服务中的大对象引用
*   **GeographyObject 接口**
    添加 GeographyObject 接口，用于处理地理数据类型
*   **Proxy 支持**
    新增 ProxyConfig 类，支持 HTTP、HTTPS、SOCKS4、SOCKS5 等多种代理配置
*   **增量读取支持**
    Table API 新增对增量读取的支持，支持基于版本或时间戳的增量数据读取

### 🐞 问题修复
*   **PartitionSpec**
    修复了分区规范解析问题，改进了分割逻辑
*   **安全权限**
    移除了对 sun.security.util.SecurityConstants 的直接依赖，改用标准的 RuntimePermission
*   **类加载器兼容性**
    修复了类加载器在不同 Java 版本中的兼容性问题
*   **OdpsOptions**
    修复了 OdpsOptions 实例化问题，确保正确传递 Odps 实例引用

### 📦 依赖升级
*   aspectjrt: `1.8.9` → `1.9.7`
*   mockito-core: `1.10.8` → `4.11.0`
*   maven-shade-plugin: `3.2.1` → `3.5.1`
*   maven-compiler-plugin: `3.1` → `3.13.0` (JDK 21)
*   maven-surefire-plugin: `2.22.2` → `3.2.5` (JDK 21)
*   maven-javadoc-plugin: `2.10.4` → `3.11.2` (JDK 21)
*   移除了 org.codehaus.jackson:jackson-mapper-asl 依赖

## [0.53.2-public] - 2025-09-11

### 🐞 问题修复
*   **LogView**
    修复了 LogView 主机配置并改进了版本处理逻辑
*   **UpsertRecord**
    修复了列名处理中的大小写敏感性问题
*   **CSVRecordParser**
    移除了掩盖解析错误的不必要 try-catch 块
*   **TableTunnel**
    增强了 upsert 操作中的错误处理并改进了 Arrow 选项配置
*   **ArrowWriterImpl**
    添加了 flush 方法实现以更好地管理资源

### ✨ 新增功能
*   **MaxStorageDownloadOption**
    新增对时间戳和日期时间单位的配置支持，以更好地处理数据类型
*   **SQLExecutorBuilder**
    优化了获取结果的配置并改进了线程管理
*   **BatchWriter**
    添加了默认的 flush 方法以更好地符合接口规范
*   **SessionRecordSetIterator**
    恢复了 SessionRecordSetIterator 类以保持向后兼容性

### 📄 文档更新
*   更新了附录文档并添加了 MaxCompute 类型的映射指南

## [0.53.1-public] - 2025-08-20

### ✨ 新增功能
*   **SQLExecutorImpl**
    删除了内部"SessionRecordSetIterator"类并将其提取为独立类

### 📦 依赖升级
*   snappy-java: `1.1.10.3` → `1.1.10.7`
*   guava: `32.1.1-jre` → `33.4.8-jre`
*   netty-all: `4.1.86.Final` → `4.1.119.Final`


## [0.53.0-public] - 2025-07-25

### ✨ 新增功能
*   **VectorizedOutputer**
    新增 `getWriteBytes()` 方法，支持获取写入字节数统计。
*   **TableBatchReadSession**
    新增基于 JSON 的序列化/反序列化方法，提升数据交互灵活性。
*   **CreateProjectParam**
    新增 `defaultQuota()` 方法，支持设置项目默认配额。
*   **Aliyun V4 签名**
    支持配置 `corporation` 参数，适配专有云环境部署需求。
*   **AklessAccount**
    完整支持 V4 签名协议。
*   **TableTunnel**
    新增 ZSTD (Zstandard) 压缩算法支持，优化数据传输效率。
*   **MaxCompute Query Acceleration (MaxQA)**
    支持并发读取超大结果集，显著提升性能（*注意：并发操作将增加内存消耗，需根据集群容量合理配置并发度*）。
*   **Preview 功能**
    支持通过 Tunnel 执行标签（Tag）操作。
*   **InstanceTunnel**
    新增 `getDownloadSession(String projectName, String instanceID, String sessionId)` 方法，简化下载会话获取流程。

### 🐞 问题修复
*   修复 `Predicate` 对 `DateTime` 类型处理时未使用本地时区的问题，确保时区一致性。
*   修复 `Table` 类在调用 `reloadExtended()` 时可能覆盖 `reload()` 数据的问题。

### 📦 依赖升级
*   commons-codec: `1.13` → `1.18.1`
*   jackson-databind: `2.15.2` → `2.18.2`
*   commons-compress: `1.4` → `1.20`
*   Apache Arrow: `4.0.0` → `17.0.0`
*   新增 zstd-jni 依赖: `1.5.7-2`（支持 ZSTD 压缩）

## [0.52.3-public] - 2025-06-14
### 🎉 新增功能
- **OdpsOptions**
  新增 Odps 实例级别的一些变量，可以通过 `odps.options()` 获取。现有两个方法：
  - `setUseLegacyLogview` = true/false/null
    当为 true，使用 logview，当为 false，使用 jobinsight（logview v2），当为 null（默认值），智能判断当前 region 是否能够使用 jobinsight，如是使用 jobinsight，否则使用 logview。
    ⚠️ **兼容性提示**：此前版本默认使用的是 logview，更新到此版本后，获取logview时可能获取到 jobinsight 地址，注意这点以避免兼容性问题。
  - `setSkipCheckIfEpv2` = true/false
    默认为 false，在 0.51.7 版本中，getTable 等接口增加了对 EPv2 项目的支持，但会影响接口性能。可以通过将此配置设置 true，会跳过Epv2的项目，提高性能。

- **ArrayRecord**
  在主要的初始化 Record 场景，比如通过构造函数初始化ArrayRecord，通过Tunnel Session newRecord 方法生成 Record，都新增了 caseSensitive 参数，用来标识使用该 Record setByName 时，是否区分大小写。
  ⚠️ **历史兼容说明**：在 0.51.8 版本中，我们让 Record 不再区分大小写（因为 MaxCompute 引擎不区分大小写），但这会导致一些性能损失。因此在本版本，我们提供了方式来恢复原行为。

- **SchemaMismatchRuntimeException**
  新增了一种异常类型，来试图 try best 的告诉用户：在 Tunnel 写入过程中，传入的数据和表模式不匹配 可能是表模式发生了变化，请重建 Tunnel Session。该类是 `IllegalArgumentException` 的子类。


## [0.52.2-public] - 2025-06-03
### 问题修复
- **CommandApi**
  修复了在 SQLExecutor 开启 CommandApi 功能时，在处理 desc extended table 返回结果中 Comment 字段错误的为 Boolean 类型的问题。


## [0.52.1-public] - 2025-05-08
### 🎉 新增功能
- **SQLExecutor**
  SQLExecutorBuilder 新增参数 `skipCheckIfSelect`（仅非 MaxQA 场景生效），默认为 false，当为 true 时，会跳过 select 语句的校验，提高极限性能。
  可以在请求场景主要为查询语句时使用，在处理非查询语句时，处理延时会变长。

## [0.52.0-public] - 2025-04-17

### 🎉 新增功能
- **TableAPI**
  - `TableWriteSessionBuilder` 新增 `enhanceWriteCheck` 参数，增强写入校验能力
  - `TableCreator` 新增 `Append2 Table` 预览功能（🚧 Preview）

- **DownloadSession**
  - 新增 `enableMaxStorage` 配置，支持通过 StorageAPI 下载 Delta Table（🚧 Preview）

- **MaxQA**
  - 实现 CSV 数据解析到强类型记录功能（`Parse CSV To Record`）
  - 服务端 Ready 后，`getResult`（非 Tunnel 模式）将返回类型化数据（原全量 String 类型）（🚧 Preview）

- **SQL**
  - 新增 `SQLTaskOption` 和 `CreateInstanceOption` 配置类，简化 `SQLTask.run()` 方法重载
  - 支持通过正则表达式提交 Merge Task 作业
  - 新增 UniqueId 机制，确保同 ID 作业幂等提交

- **ObjectConverter**
  - 新增 `BINARY_FORMAT_QUOTED_PRINTABLE` 格式解析支持

### 🛠️ 功能优化
- **ArrowStreamRecordReader**
  重构类实现，支持将任意来源的 `ArrowReader` 转换为 `RecordReader`

- **ArrayRecord**
  - 优化类型校验逻辑：
    ✅ 将 `set` 方法可能抛出的 `ClassCastException` 改为 `IllegalArgumentException`
    ✅ 增强错误信息可读性
    ✅ 避免 JVM 对异常的隐式优化（如错误信息被截断为 null）



## [0.51.11-public] - 2025-03-18

### 主要改动
- **结构优化**：`Instance` 类中 `ResultDescriptor` 子类由 `Map<String, String>` 升级为独立的 POJO 对象，专门适配 **MCQA 2.0 场景**
- **兼容保障**：完全兼容旧版逻辑，现有代码无需修改即可正常运行

### 注意事项
- 未来服务端可能调整 `ResultDescriptor` 数据格式，建议 **MCQA 2.0 用户升级到本版本**

## [0.51.10-public] - 2025-03-11
### 功能增强
- **TableTunnel 指标支持**
  Upload/Download 方法新增指标收集能力
  [文档参考](link_to_document)

- **TunnelBufferedReader 实现**
  新增 `TunnelBufferedReader` 类，支持通过短连接下载表/实例数据

- **可排序数据结构**
  新增 `ReorderableRecord` 和 `ReorderableStruct` 实体类
  [设计说明](https://github.com/aliyun/aliyun-odps-java-sdk/releases/tag/v0.51.10-SNAPSHOT)

## [0.51.9-public] - 2025-02-26
### 问题修复
- **结构体字段转义修复**
  修复 `TypeInfo` 中 `getName(true)` 方法未对嵌套结构体字段名添加反引号的问题

## [0.51.8-public] - 2025-02-20
### 变更
- **Record** `set(String columnName, Object value)` 方法现在会忽略 columnName 的大小写。`getColumn` 方法返回的列名将始终为小写。

  ⚠️ **兼容性提示**： 注意，这项改动会影响 ArrayRecord 初始化和 setByName 时的性能，用户应当相应的性能测试，我们在 0.52.3 版本中增加了开关来关闭这项功能。

### 功能
- **Table** 新增`getMetadataJson`和`getExtendedInfoJson`方法
- **Partition** 新增`getMetadataJson`,`getExtendedInfoJson`,`getCdcSize`,`getCdcRecordNum`方法
- **CommandApi** 增强 DescribeTableCommand，现在将额外返回 `MetadataJson` 和 `ExtendedInfoJson` 字段
- **PartitionSpec** 改进构建失败时的报错信息，使报错更加明晰

## [0.51.7-public] - 2025-02-13
### 功能
- **MCQA** 在通过 InstanceTunnel 取结果，发生失败回退的场景，加入回退日志
- **EPV2** 新增对 EPV2（External Project V2）的支持，包括`ListTable`, `ListSchema`, `DescribeTable` 等接口。

  ⚠️ **兼容性提示**：这会略微影响这些接口的性能（功能不受影响），需要用户注意，我们在 0.52.3 版本中增加了开关来关闭这项功能。


## [0.51.6-public] - 2025-01-26
### 修复
- **TypeInfo** 修复了当 `StructTypeInfo` 嵌套在 `ArrayTypeInfo` 或 `MapTypeInfo` 内时，`getTypeName(true)` 方法不会对嵌套内字段名进行 quote 的问题。


## [0.51.5-public] - 2025-01-14
### 修复
- **MCQA2** 修复了 MCQA2 作业，可能会使用tunnel取结果时，无法正确抛出异常的问题

## [0.51.4-public] - 2025-01-14

### 功能
- **MCQA2** 增加若干项优化，提升了 MCQA2 作业的执行效率。MCQA2 作业的模式变为 `ExecuteMode.INTERACTIVE_V2`，与 MCQA1 的 `ExecuteMode.INTERACTIVE` 区别开
- **SQLExecutor** 新增 `getExecuteMode` 方法，用于获取作业执行模式

### 变更
- **UpsertStream** 在 0.51.0 版本，修改了 `close` 方法的函数签名（不再抛出 `TunnelException`），在本版本中恢复，以保证接口兼容性。
- **ClusterInfo** 在 0.51.0 版本，toString 方法有所变更，在本版本中恢复，以保证接口兼容性。
- **TunnelRetryStrategy**，**ConfigurationImpl** 类在 0.48.6 版本被移除，在本版本中恢复（但不会起到任何效果！），以保证接口兼容性。

## [0.51.3-public] - 2025-01-07
### 功能
- **MCQA2** SQLExecutorImpl 新增 `setProject` 方法，用于指定提交作业使用的默认项目

### 变更
- **StreamTunnel** 在调用 append 方法时，当 Record 列数量大于 Session Schema 列数量，现在将抛出 `SchemaMismatchException(extend IOExcption)`，而不是抛出 `IOException`，并优化了错误信息

## [0.51.2-public] - 2024-12-20
### 功能
- **Authorization** 引入`credential-java`鉴权包，现在能够使用`AlibabaCloudCredentialsProvider`进行鉴权
- **StreamUploadSession** 新增对 Slot 更新的感知和自动重试逻辑
- **table-api** 引入`TableRetryHandler`类，为`table-api`添加重试逻辑
- **udf** 中`InputSplitter`新增方法 `setLimit`

### 变更
- **TypeInfo** `StructTypeInfo` 新增方法 `getTypeName(boolean quote)`，在 `0.51.0-public (rc0)` 版本，`StructTypeInfo` 默认会对字段名使用反引号进行 quote，我们怀疑这项变更对用户有影响，因此决定恢复原行为（默认不进行quote）
  而是当用户需要 quote 时，可以调用 `getTypeName(true)`

### 修复
- **TypeInfo** 当对字段使用反引号进行 quote 时，现在会正确对字段名进行转义
- **MCQA2** 修复了 MCQA2 作业调用 `getRawTaskResults` 接口取不到结果的问题


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
> **注意：** 此版本还包括"0.51.0-public.rc0"和"0.51.0-public.rc1"的所有更改。

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
- **TypeInfo** 对 Struct 类型的 TypeInfo，和其他拼装 SQL 的方法，使用反引号对名字进行 quote
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

- **SQLExecutor** 在 MCQA 1.0
  模式下，允许增加自定义回退策略，新增类`FallbackPolicy.UserDefinedFallbackPolicy`。

## [0.50.2-public] - 2024-10-23

### 功能

- **SQLExecutor** 增强 MCQA 2.0 功能：
  - `isActive` 将返回 false，指示在 MCQA 2.0 模式下没有活跃的 Session。
  - 新增 `cancel` 方法，用于中止正在执行的作业。
  - `getExecutionLog` 现在返回当前日志的深拷贝并清空当前日志，避免重复获取。
  - 在 `SQLExecutorBuilder` 新增 `quota` 方法，支持复用已加载的 `Quota`，减少加载时间。
  - 在 `SQLExecutorBuilder` 新增 `regionId` 方法，允许指定 quota 所在的 region。
- **Quotas** 新增带 `regionId` 参数的 `getWlmQuota` 方法，用于获取指定 regionId 的 quota。
- **Quota** 新增 `setMcqaConnHeader` 方法，支持用户通过自定义的 McqaConnHeader 重载 quota，以适配 MCQA
  2.0。
- **Instances** 新增适用于 MCQA 2.0 的 `get` 方法，需额外传入 MCQA 2.0 的 QuotaName 和 RegionId。
- **Instance** 进一步适配 MCQA 2.0 作业。
- **TableSchema** `basicallyEquals` 方法将不再严格检查两个类的 Class 类型一致性。

### 优化

- **SQLExecutor** `run` 方法中的 hints 现在会进行深拷贝，保护用户传入的
  Map，支持不可变类型（如 `ImmutableMap`）。

### 修复

- **Stream** 修复 `create` 方法中的潜在 SQL 语法错误。

## [0.50.1-public] - 2024-10-11

### 修复

- **TableAPI** 修复了使用`SplitRecordReaderImpl`获取结果时，拿到了`ArrayRecord`无法正确`toString`的问题。
- **TableAPI** 修复了使用`SplitRecordReaderImpl`获取结果时，如果`Split`对应的`Record`数量为0，在`get`
  操作时会抛出数组越界异常的问题。
- **TableAPI** 修复了复合谓词`CompositePredicate`在遇到空谓词时，可能额外增加一次操作符的问题。

## [0.50.0-public] - 2024-10-09

### 功能

- 新增 `SchemaMismatchException` 异常：当使用 `StreamUploadSession` 时，如果用户上传的 Record
  结构与表结构不匹配，将抛出该异常。此异常将额外携带最新的 schema version，方便用户重建 Session
  并进行重试操作。
- 在 `StreamUploadSession.Builder` 中新增 `allowSchemaMismatch` 方法，用于指定是否容忍用户上传的
  Record 结构与表结构不匹配时是否抛出异常。默认值为 `true`。

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
-
新增 `getResultByInstanceTunnel(Instance instance, String taskName, Long limit, boolean limitEnabled)`
方法：
- 用来无限制地通过 instanceTunnel 获取结果（解除限制需要更高的权限）

- **UpsertSession.Builder** 新增 `setLifecycle` 方法，用来配置 Session 生命周期

### 修复

- 修复了使用 SQLExecutor 执行离线作业时，指定 `limitEnabled` 取结果但不生效的问题
- 修改了 SQLExecutor 执行离线作业时，`getQueryId` 方法会返回作业的 instanceID 而非 null
- 修复了 SQLExecutor 执行离线作业时，当遇到非 select 语句时，使用 instanceTunnel 取结果不再抛出异常，而是回退到非
  tunnel 逻辑
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

