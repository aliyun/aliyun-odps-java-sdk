# Changelog
## [0.57.0-public] - 2026-03-05

### ✨ New Features
* **[Storage API][Preview]**: **New `odps-sdk-storage-api` Module** - Introduced a brand-new high-performance Storage API client (`MaxStorageClient`) for reading and writing MaxCompute tables using the Arrow columnar format. Supports distributed batch reads via input splits, write sessions with commit/abort semantics, table preview, blob download, and instance result reading.
    * *Related APIs*: `MaxStorageClient`, `MaxStorageClient.Builder`, `TableReadSession`, `TableWriteSession`, `InstanceReadSession`, `BlobManager`
* **[Arrow Helper]**: **New `odps-arrow-helper` Module** - Extracted Arrow-related utilities into a standalone module, including `TableIdentifier`, `InstanceIdentifier`, `StreamIdentifier`, Arrow type accessors, `ArrowReaderBuilder`, `ArrowStreamRecordReader`, and `SchemaUtils`.
    * *Related APIs*: `TableIdentifier`, `InstanceIdentifier`, `StreamIdentifier`, `ArrowReaderBuilder`
* **[SQLExecutor]**: **Storage API Result Set Integration** - `SQLExecutorImpl` now automatically routes result download through the Storage API (`StorageAPIResultSet`) when query results contain `BLOB` columns, improving compatibility and data transfer efficiency.
    * *Related APIs*: `StorageAPIResultSet`, `InternalBlobHelper`
* **[SQLExecutor]**: **MaxQA Fallback Configuration** - Added `FallbackInfo` and `MaxQAConnInfo` to support configuring MaxQA quota fallback behavior, allowing automatic fallback to a specified quota when the primary quota is unavailable.
    * *Related APIs*: `FallbackInfo`, `MaxQAConnInfo`, `SQLExecutorBuilder.maxQAConnInfo()`, `SQLExecutorBuilder.enableMaxQA()`
* **[RestClient]**: **Request/Response Interceptor Chain** - Added `InterceptorChain`, `RequestInterceptor`, `ResponseInterceptor`, and `InterceptorContext` to support pluggable HTTP request/response interception.
    * *Related APIs*: `InterceptorChain`, `RequestInterceptor`, `ResponseInterceptor`
* **[Commons]**: **Added `JsonString` Type** - Added `JsonString` as a lightweight `JsonValue` implementation that wraps a raw JSON string, suitable for scenarios that do not require parsing.
    * *Related APIs*: `JsonString`
* **[Commons]**: **Added `RecordReader` and `RecordWriter` Interfaces** - Extracted generic `RecordReader` and `RecordWriter` interfaces into `odps-sdk-commons` for reuse across modules.
    * *Related APIs*: `RecordReader`, `RecordWriter`
* **[Odps]**: **Catalog API Host & Stale Metadata Read** - Added `setCatalogApiHost()` / `getCatalogApiHost()` and `setAllowStaleMetadataRead()` / `isAllowStaleMetadataRead()` to `Odps` for configuring the Catalog API endpoint and allowing stale metadata reads.
    * *Related APIs*: `Odps.setCatalogApiHost()`, `OdpsOptions.allowStaleMetadataRead`
* **[Quota]**: **MaxQA Connection Info Retrieval** - Added `Quotas.getMaxQAConnInfo(quotaName)` to fetch MaxQA connection information for a given quota.
    * *Related APIs*: `Quotas.getMaxQAConnInfo()`
* **[Partition]**: **Partition State Management** - Added `Partition.State` enum and `setState()` method to support setting partition state.
    * *Related APIs*: `Partition.State`, `Partition.setState()`

### 🚀 Enhancements & Performance
* **[OdpsType]**: **Added Numeric Codes to `OdpsType` Enum** - Each `OdpsType` enum constant now carries a stable integer code and supports reverse lookup via `OdpsType.fromCode(int)`, facilitating serialization and protocol compatibility.
* **[Arrow Helper]**: **Refactored Arrow Accessor Layer** - Moved all per-type Arrow column accessors (`ArrowBigIntAccessor`, `ArrowDecimalAccessor`, `ArrowTimestampAccessor`, etc.) into the new `odps-arrow-helper` module, improving modularity and reusability.
* **[TunnelBufferedWriter]**: **Asynchronous Flush Support** - `TunnelBufferedWriter` now supports non-blocking async flush with a double-buffer swap mechanism (`flush(boolean blocking)`), providing backpressure and improving write throughput for streaming uploads.
    * *Related APIs*: `TunnelBufferedWriter.flush(boolean blocking)`
* **[UpsertStream]**: **Async Flush & Buffer Optimization** - `UpsertStreamImpl` now supports asynchronous flush via a configurable `ExecutorService`, with per-bucket double-buffer swap and a `sync()` method for explicit barrier synchronization.
    * *Related APIs*: `UpsertStream.Builder.setAsyncFlushService()`, `UpsertStreamImpl.sync()`
* **[CI]**: **Added CodeQL Security Scanning** - Integrated GitHub Actions CodeQL workflow for automated security vulnerability scanning.

### 📦 Dependency Updates
* **Add**: `com.squareup.okhttp3:okhttp:4.12.0` (shaded in `odps-sdk-storage-api`)

## [0.56.1-public] - 2026-02-04

### 🐛 Bug Fixes
* **[CVE]**: **Update commons-io to 2.20.0** - Fixed security vulnerabilities in commons-io library by upgrading to version 2.20.0.
* **[CVE]**: **Use at.yawk.lz4 instead of org.lz4.pure-java** - Replaced org.lz4.pure-java dependency with at.yawk.lz4 to address security vulnerabilities.

## [0.56.0-public] - 2026-01-30

### ✨ New Features
* **[Account]**: **Refactored Authentication System** - Adopted new `Credentials` class to replace the legacy `Credential`, and added `getRegionId()` method to support region ID configuration, making the authentication mechanism more flexible and standardized.
    * *Related APIs*: `Account.getCredentials()`, `Account.getRegionId()`
* **[Instance]**: **Enhanced MaxQA Query Result Status Management** - Added `SelectResultStatus` enum to distinguish query result completeness states (FULL/TRUNCATED/NO), allowing users to determine whether data has been returned completely.
    * *Related APIs*: `Instance.ResultDescriptor.SelectResultStatus`, `Instance.ResultDescriptor.getSelectResultStatus()`
* **[ProxyConfig]**: **Added Netty DNS Resolver Control Option** - Allows disabling the local Netty DNS resolver via `withDisableNettyLocalResolver()`, suitable for scenarios requiring the proxy server to perform DNS resolution.
    * *Related APIs*: `ProxyConfig.Builder.withDisableNettyLocalResolver()`

### 🚀 Enhancements & Performance
* **[Arrow]**: **Optimized Arrow Data Reader** - Unified use of `CommonsCompressionFactory` for handling compressed data, improving large-scale data read performance and compatibility.
* **[Tunnel]**: **Improved Tunnel Endpoint Resolution Logic** - Now prioritizes the `odps.getTunnelEndpoint()` configuration item, simplifying the endpoint configuration process.
* **[RestClient]**: **Optimized Retry Wait Time Calculation** - Now uses only the connection timeout instead of the sum of connection + read timeout, making the retry strategy more precise.
* **[TableSchema]**: **Added Batch Partition Column Configuration Method** `withPartitionColumns()`, simplifying the creation of multiple partition columns.
    * *Related APIs*: `TableSchema.Builder.withPartitionColumns()`

### 🐛 Bug Fixes
* **[Instance]**: **Fixed Error Handling for Task Failures** - Added `checkTaskFailed()` check in `waitForSuccess()` and `isSelect()` methods, ensuring timely exception throwing with detailed failure information when tasks fail.

### 📦 Dependency Updates
* **Upgrade**: `commons-io`: `2.11.0` → `2.14.0`
* **Replace**: `commons-lang:2.6` → `org.apache.commons:commons-lang3:3.18.0`
* **Upgrade**: `io.netty:netty-all`: `4.1.119.Final` → `4.1.130.Final`
* **Add**: `org.locationtech.jts:jts-core:1.19.0`
* **Replace**: `com.aliyun:aliyun-java-auth:0.2.16-beta` → `com.aliyun:credentials-api:1.0.0`

## [0.55.1-public] - 2026-01-22

### 🐛 Bug Fixes
*   **Arrow Dictionary Decoding**
    Fixed an issue where dictionary decoding was not using the compression factory from the ArrowBatchNonReusedReader, improving Arrow data reading stability and performance.

## [0.55.0-public] - 2025-12-12

### ✨ New Features
*   **Account Credential Information Retrieval**
    All Account implementations now support the `getCredential()` method, allowing users to retrieve the AccessKey ID, AccessKey Secret, and Security Token for account management and security auditing.
*   **Tunnel Dynamic Partition Writing**
    Added `DynamicPartitionRecordPack` and `PartitionRecord` to support writing to multiple partitions in a single operation, significantly improving multi-partition data writing efficiency. Enable via `TableTunnel.StreamUploadSession.Builder.setDynamicPartition()`.
*   **Tunnel Arrow Buffered Reader**
    Added high-performance buffered reading capability based on Arrow format for non-Delta tables, optimizing large-scale data reading performance. Use via `DownloadSession.openBufferedArrowRecordReader()`.
*   **Tunnel Batch Tracking**
    Added `getBatchId()` method to `FlushResult` interface, and `getLastBatchId()` and `getLastBatchCommitTime()` methods to `StreamUploadSession` for tracking flush batch IDs and commit times.
*   **Arrow Async Streaming Reader**
    Added `ArrowStreamAsyncReader` class to support asynchronous Arrow data stream reading in a separate thread, improving performance in IO-intensive scenarios. Configure via `ReaderOptions.Builder.withAsync()` and `withAsyncQueue()`.
*   **Performance Monitoring Metrics**
    Added two counter metrics, `RateLimitCost` and `ServerProcessCost`, for monitoring rate limiting costs and server processing time, helping users optimize read performance.
*   **Quota Auto-Scaling Configuration**
    Added `autoScaleCPULimit`, `autoScaleMemoryLimit`, and `autoScaleGPULimit` fields to the `Quota` class to support retrieving project auto-scaling quota information.
*   **Project Properties Retrieval Improvement**
    The `Project.getAllProperties()` method now retrieves complete configuration information including inherited properties from group, providing a more comprehensive view of project properties.

### 🚀 Enhancements & Performance
*   **Arrow Dictionary Decoding Optimization**
    Improved dictionary encoding handling in `ArrowBatchNonReusedReader` and `ArrowBatchReusedReader`, fixing the issue where dictionary decoding was not using the compression factory, enhancing Arrow data reading stability and performance.
*   **TunnelRecordReader Enhancement**
    Added `openRecordReader()` method to `DownloadSession`, supporting control of data size limit via `sizeLimit` parameter.
*   **Buffered Reader Parameter Optimization**
    Added `bufferSize` parameter to `openBufferedRecordReader()` method, allowing users to fine-tune buffer size for optimal memory usage.
*   **Read by Raw Size Support**
    Added `isSupportReadByRawSize()` method to `DownloadSession` to support checking if the current session supports reading by raw data size.

### 🐛 Bug Fixes
*   **Instance Async Task Result Retrieval**
    Fixed an issue where `getTaskResult()` would return null when the task result list is empty in async scenarios. Now throws a clear exception message.
*   **Arrow Dictionary Encoded Field Processing**
    Improved Arrow Reader's handling of dictionary encoded fields in nested structures, ensuring dictionary data is loaded and decoded correctly.

## [0.54.0-public] - 2025-10-21

✨ New Features

1. Java 21 Support - Added JDK 21 configuration files and build configurations
2. New Data Types - Added support for GEOGRAPHY and BLOB data types
3. AspectJ Support - Added AspectJ Maven plugin support for aspect-oriented programming
4. ClassLoader Enhancements - Improved compatibility across Java versions with getLoadedJars method
5. Blob Class - Added Blob class for handling large object references in storage services
6. GeographyObject Interface - Added interface for handling geographic data types
7. Proxy Support - Added ProxyConfig class supporting HTTP, HTTPS, SOCKS4, SOCKS5 configurations
8. Incremental Read Support - Added support for version-based and timestamp-based incremental data reading in Table API

🐞 Bug Fixes

1. PartitionSpec - Fixed partition specification parsing issues and improved splitting logic
2. Security Permissions - Removed direct dependency on sun.security.util.SecurityConstants, using standard RuntimePermission instead
3. ClassLoader Compatibility - Fixed class loader compatibility issues across different Java versions
4. OdpsOptions - Fixed instantiation issues ensuring correct Odps instance reference passing

📦 Dependency Upgrades

1. aspectjrt: 1.8.9 → 1.9.7
2. mockito-core: 1.10.8 → 4.11.0
3. maven-shade-plugin: 3.2.1 → 3.5.1
4. maven-compiler-plugin: 3.1 → 3.13.0 (JDK 21)
5. maven-surefire-plugin: 2.22.2 → 3.2.5 (JDK 21)
6. maven-javadoc-plugin: 2.10.4 → 3.11.2 (JDK 21)
7. Removed org.codehaus.jackson:jackson-mapper-asl dependency

## [0.53.2-public] - 2025-09-11

### ✨ New Features
*   **MaxStorageDownloadOption**  
    Added support for configuring timestamp and datetime units for better data type handling
*   **SQLExecutorBuilder**  
    Optimized fetch result configuration and improved thread management
*   **BatchWriter**  
    Added default flush method for better interface compliance
*   **SessionRecordSetIterator**  
    Restored SessionRecordSetIterator class to maintain backward compatibility

### 🐞 Bug Fixes
*   **LogView**  
    Fixed LogView host configuration and improved version handling logic
*   **UpsertRecord**  
    Fixed case sensitivity issues in column name handling
*   **CSVRecordParser**  
    Removed unnecessary try-catch block that was masking parsing errors
*   **TableTunnel**  
    Enhanced error handling in upsert operations and improved Arrow options configuration
*   **ArrowWriterImpl**  
    Added flush method implementation for better resource management

### 📄 Documentation
*   Updated appendix documentation and added type mapping guide for MaxCompute types

## [0.53.1-public] - 2025-08-20

### ✨ New Features
*   **SQLExecutorImpl**  
    Removed internal `SessionRecordSetIterator` class and extracted it as a standalone class to improve code organization.

### 📦 Dependency Upgrades
*   snappy-java: `1.1.10.3` → `1.1.10.7`
*   guava: `32.1.1-jre` → `33.4.8-jre`
*   netty-all: `4.1.86.Final` → `4.1.119.Final`

## [0.53.0-public] - 2025-07-25

### ✨ New Features
*   **VectorizedOutputer**  
    Added `getWriteBytes()` method to retrieve written byte count statistics.
*   **TableBatchReadSession**  
    Introduced JSON-based serialization/deserialization methods for enhanced data interoperability.
*   **CreateProjectParam**  
    Added `defaultQuota()` method to configure default project quotas.
*   **V4 Signature**  
    Added support for `corporation` configuration parameter, enabling compatibility with dedicated cloud environments.
*   **AklessAccount**  
    Full support for V4 signature protocol.
*   **TableTunnel**  
    Added ZSTD (Zstandard) compression algorithm support for optimized data transfer efficiency.
*   **MaxCompute Query Acceleration (MaxQA)**  
    Enabled concurrent reading for ultra-large result sets, significantly improving performance (*Note: Concurrent operations increase memory consumption; adjust concurrency levels based on cluster capacity*).
*   **Preview Feature**  
    Added support for tag operations via Tunnel.
*   **InstanceTunnel**  
    Added `getDownloadSession(String projectName, String instanceID, String sessionId)` method for simplified download session retrieval.

### 🐞 Bug Fixes
*   Fixed `DateTime` timezone handling in `Predicate` to consistently use local timezone.
*   Resolved data overwrite issue where `reloadExtended()` could override `reload()` results in the `Table` class.

### 📦 Dependency Upgrades
*   commons-codec: `1.13` → `1.18.1`
*   jackson-databind: `2.15.2` → `2.18.2`
*   commons-compress: `1.4` → `1.20`
*   Apache Arrow: `4.0.0` → `17.0.0`
*   Added zstd-jni dependency: `1.5.7-2` (for ZSTD compression support)

## [0.52.3-public] - 2025-06-14
### 🎉 New Features
- **OdpsOptions**  
  Adds instance-level variables for Odps that can be accessed through `odps.options()`. Two methods are currently available:
  - `setUseLegacyLogview` = true/false/null  
    When set to true, uses logview; when false, uses jobinsight (logview v2); when null (default), intelligently determines if jobinsight is available in the current region and uses it if possible, otherwise falls back to logview.  
    ⚠️ **Compatibility Note**: Previous versions defaulted to logview. After upgrading, calls to get logview URLs might return jobinsight addresses - verify this to avoid compatibility issues.
  - `setSkipCheckIfEpv2` = true/false  
    Defaults to false. In version 0.51.7, interfaces like getTable gained support for EPv2 projects but this impacts interface performance. Setting this configuration to true skips EPv2 project checks to improve performance.

- **ArrayRecord**  
  Added a `caseSensitive` parameter in major Record initialization scenarios (constructor initialization of ArrayRecord, and Tunnel Session's newRecord method) to control whether field names are case-sensitive during setByName operations.  
  ⚠️ **Historical Compatibility Note**: In version 0.51.8, we made Record case-insensitive (since MaxCompute engine is case-insensitive), but this introduced performance overhead. This version provides a way to restore the original behavior.

- **SchemaMismatchRuntimeException**  
  Added a new exception type that makes a best-effort attempt to inform users:
  - Data mismatches table schema during Tunnel writes
  - Table schema might have changed - please recreate the Tunnel Session  
    This class extends `IllegalArgumentException`.


## [0.52.2-public] - 2025-06-03
### Issue Fixed
- **CommandApi**
  Fixed an issue where the Comment field is Boolean type is handled when SQLExecutor is enabled when CommandApi function is turned on.

## [0.52.1-public] - 2025-05-08
### 🎉 New features
- **SQLExecutor**
  SQLExecutorBuilder added the parameter `skipCheckIfSelect` (only Non-MaxQA scenarios are effective), which defaults to false. When true, the check of the select statement will be skipped to improve the performance under high concurrency.
  It can be used when the request scenario is mainly a query statement. When processing non-query statements, the processing delay will become longer.

## [0.52.0-public] - 2025-04-17

### 🎉 New Features
- **TableAPI**
  - Added `enhanceWriteCheck` parameter to `TableWriteSessionBuilder` to strengthen write validation capabilities
  - Added `Append2 Table` preview feature in `TableCreator` (🚧 Preview)

- **DownloadSession**
  - Introduced `enableMaxStorage` configuration to support downloading Delta Tables via StorageAPI (🚧 Preview)

- **MaxQA**
  - Implemented CSV data parsing to strongly-typed records (`Parse CSV To Record`)
  - After server readiness, `getResult` (non-Tunnel mode) will return typed data (previously all String types) (🚧 Preview)

- **SQL**
  - Added `SQLTaskOption` and `CreateInstanceOption` configuration classes to simplify overloads of `SQLTask.run()`
  - Added support for submitting Merge Tasks using regular expressions
  - Implemented Unique ID mechanism to ensure idempotent submission of jobs with the same ID

- **ObjectConverter**
  - Added support for `BINARY_FORMAT_QUOTED_PRINTABLE` format parsing

### 🛠️ Functionality Improvements
- **ArrowStreamRecordReader**  
  Refactored class implementation to support converting any source `ArrowReader` to `RecordReader`

- **ArrayRecord**
  - Optimized type validation logic:  
    ✅ Changed potential `ClassCastException` in `set` method to `IllegalArgumentException`  
    ✅ Improved error message readability  
    ✅ Prevented JVM implicit optimizations (e.g., truncated error messages to null)

## [0.51.11-public] - 2025-03-18
### Key Changes
- **MCQA 2.0 Structural Improvement**: The `ResultDescriptor` subclass in `Instance` class has been upgraded from `Map<String, String>` to a dedicated POJO object, specifically optimized for **MCQA 2.0 scenarios**
- **Backward Compatibility**: Fully compatible with legacy logic - existing code will continue to function without modification

### Important Notes
- Future server-side updates may adjust `ResultDescriptor` data formats. **MCQA 2.0 users are recommended to upgrade to this version**

## [0.51.10-public] - 2025-03-11
### Features
- **TableTunnel Metrics Support**  
  Upload/Download methods now support metrics collection.  
  [Documentation Reference](link_to_document)

- **TunnelBufferedReader Implementation**  
  Added `TunnelBufferedReader` for downloading table/instance data via short-lived connections.

- **Reorderable Data Structures**  
  Introduced `ReorderableRecord` and `ReorderableStruct` entities.  
  [Design Details](https://github.com/aliyun/aliyun-odps-java-sdk/releases/tag/v0.51.10-SNAPSHOT)


## [0.51.9-public] - 2025-02-26
### Fixes
- **Struct Field Escaping**  
  Fixed `getName(true)` not adding backticks to all nested struct field names in `TypeInfo`.

## [0.51.8-public] - 2025-02-20
### Changes
- **Record** The `set(String columnName, Object value)` method now ignores the case of `columnName`. The `getColumn` method will always return column names in lowercase.
  ⚠️ **Compatibility Note**: This change will affect the performance of ArrayRecord initialization and setByName operations. Users should conduct corresponding performance tests. We have introduced a toggle in version 0.52.3 to disable this feature.

### Features
- **Table** Added `getMetadataJson` and `getExtendedInfoJson` methods.
- **Partition** Added `getMetadataJson`, `getExtendedInfoJson`, `getCdcSize`, and `getCdcRecordNum` methods.
- **CommandApi** Enhanced the `DescribeTableCommand` to include additional `MetadataJson` and `ExtendedInfoJson` fields in the response.
- **PartitionSpec** Improved error messages for build failures to provide clearer debugging information.

## [0.51.7-public] - 2025-02-13
### Features
- **EPV2** Added support for EPV2 (External Project V2), including `ListTable`, `ListSchema`, `DescribeTable` interfaces
  ⚠️ **Compatibility Note**: This will slightly impact the performance of these interfaces (functionality remains unaffected) and requires user attention. We have added a configuration option in version 0.52.3 to turn off this feature.
- **MCQA** Added fallback logging when retrieving results via InstanceTunnel encounters failure rollback scenarios


## [0.51.6-public] - 2025-01-26
### Fixes
- **TypeInfo** Fixed an issue where `StructTypeInfo` nested within `ArrayTypeInfo` or `MapTypeInfo` would not quote field names in nested structures when using `getTypeName(true)` method

## [0.51.5-public] - 2025-01-14
### Fixes
- **MCQA2** Fixed the problem that MCQA2 jobs may not throw exceptions correctly when using instance tunnel to obtain results.

## [0.51.4-public] - 2025-01-14

### Features
- **MCQA2**: Added several optimizations to improve the execution efficiency of MCQA2 jobs. MCQA2 jobs now use `ExecuteMode.INTERACTIVE_V2` mode, distinguishing it from MCQA1's `ExecuteMode.INTERACTIVE`
- **SQLExecutor**: Added new `getExecuteMode` method to retrieve job execution mode

### Changes
- **UpsertStream**: In version 0.51.0, the signature of the `close` method was modified (no longer throwing `TunnelException`). This version restores it to maintain API compatibility.
- **ClusterInfo**: The toString method was changed in version 0.51.0. This version restores it to maintain API compatibility.
- **TunnelRetryStrategy** and **ConfigurationImpl** classes: These were removed in version 0.48.6. This version restores them (though they won't have any effect!) to maintain API compatibility.

## [0.51.3-public] - 2025-01-07
### Features
- **MCQA2** SQLExecutorImpl adds a new `setProject` method to specify the default project used for submitting jobs.

### Changes
- **StreamTunnel** when calling the append method, if the number of Record columns exceeds the number of Session Schema columns, it will now throw a `SchemaMismatchException (extends IOException)` instead of throwing an `IOException`, and the error message has been optimized.

## [0.51.2-public] - 2024-12-20
### Features
- **Authorization** Introduced the `credential-java` authorization package, now supporting authentication with `AlibabaCloudCredentialsProvider`.
- **StreamUploadSession** Added awareness for Slot updates and automatic retry logic.
- **table-api** Introduced the `TableRetryHandler` class, adding retry logic to the `table-api`.
- **udf** The `InputSplitter` now includes the method `setLimit`.

### Changes
- **TypeInfo** The `StructTypeInfo` class now includes the method `getTypeName(boolean quote)`. In version `0.51.0-public (rc0)`, `StructTypeInfo` defaulted to quoting field names with backticks. We suspect that this change may affect users, so we decided to revert to the original behavior (not quoting by default). Users can now call `getTypeName(true)` when quoting is needed.

### Fixes
- **TypeInfo** Field names will now be correctly escaped when quoted with backticks.
- **MCQA2** Fixed an issue where the `getRawTaskResults` interface call in MCQA2 jobs could not retrieve results.


## [0.51.0-public] - 2024-12-05
### Features
- **MapReduce** Supports multi pipeline output.
- **VolumeBuilder** Added the `accelerate` method to speed up the download process using dragonfly when the external volume is too large.
- **Table** Introduced `TableType OBJECT_TABLE` and the method `isObjectTable` to verify it.
- **Project** The `list` method now includes a filter condition `enableDr` to filter projects based on whether data disaster recovery is enabled.
- **Cluster** New fields added: `clusterRole`, `jobDataPath`, and `zoneId`.

### Changes
- **TableBatchReadSession** The `predicate` class variable is now set to transient.
- **Attribute** added escaping logic and will no longer double quote.
- **SQLTask** Restored the `SQLTask.run(Odps odps, String project, String sql, String taskName, Map<String, String> hints, Map<String, String> aliases, int priority)` method removed in version 0.49.0 to resolve potential interface conflicts when users' MR jobs depend on older versions of the SDK.

### Fixes
- **Table.changeOwner** Fixed SQL spelling error.
- **Instance.getTaskSummary** Removed unreasonable debug logging introduced since version 0.50.2.
- **TruncTime** Uses backticks to quote `columnName` during table creation/toString.
> **Note:** This version also includes all changes from "0.51.0-public.rc0" and "0.51.0-public.rc1".

## [0.50.6-public] - 2024-11-27
### Features

- **Logview** Added support for Logview V2, details see [November 14, 2024 (UTC+8): Notice on the security upgrade for MaxCompute LogView
  ](https://www.alibabacloud.com/help/en/maxcompute/product-overview/2024-service-notices). It can be created
  using `new Logview(odps, 2)`, and SQLExecutor specifies the version through the `logviewVersion`
  method.

## [0.51.0-public.rc1] - 2024-11-22
### Features and Changes
- **Column** `ColumnBuilder` adds a new `withGenerateExpression` method for constructing auto-partition columns
- **TableSchema**
  - Added `generatePartitionSpec` method, used to generate partition information from `Record`
  - The `setPartitionColumns` method now accepts `List<Column>` instead of `ArrayList<Column>`
- **TableCreator**
  - Added support for `GenerateExpression` and introduced the method `autoPartitionBy`, which allows for the creation of AutoPartition tables.
  - Added support for `ClusterInfo`, enabling the creation of Hash/Range Cluster tables.
  - Added the option to specify `TableFormat`, allowing for the creation of tables in `APPEND`, `TRANSACTION`, `DELTA`, `EXTERNAL`, and `VIEW` formats.
  - Introduced the `selectStatement` parameter for `create table as` and `create view as` scenarios.
  - Added the `getSql` method to obtain the SQL statement for table creation.
  - Now quotes all `Comment` parameters to support those that contain special characters.
  - Integrated DataHub-related table creation parameters (`hubLifecycle`, `shardNum`) into `DataHubInfo`.
  - Renamed the `withJars` method to `withResources` to indicate it can use resources other than JAR files.
  - Renamed the `withBucketNum` method to `withDeltaTableBucketNum` to indicate this method is for Delta Tables only.
  - Modified the logic of `withHints`, `withAlias`, `withTblProperties`, and `withSerdeProperties` methods, now overwriting previous values instead of merging.
  - Removed the `createExternal` method; you can now use the `create` method instead.
- **Table** 
  - Introduced the `getSchemaVersion` method, allowing users to retrieve the current schema version of the table. The version number is updated each time a Schema Evolution occurs, and this field is used primarily for specifying when creating a StreamTunnel.
  - Added `setLifeCycle`, `changeOwner`, `changeComment`, `touch`, `changeClusterInfo`, `rename`, `addColumns`, `dropColumns` methods to support modification of table structure.
- **StreamTunnel** Modified the initialization logic; if `allowSchemaMismatch` is set to `false`, it will automatically retry until the latest version of the table structure is used (with a timeout of 5 minutes).

### Fixes
- **GenerationExpression** Fixed the issue where an exception would be thrown when the `TruncTime` was uppercase during table creation and reloading the table.
- **TypeInfoParser** Can now correctly handle `Struct` types, with fields quoted using backticks in `TypeInfo`.


## [0.51.0-public.rc0] - 2024-11-18

### Features
- **GenerateExpression** added support for generating expression lists for partition columns, along with the first generated expression `TruncTime`. For usage, please refer to [Example](https://github.com/aliyun/aliyun-odps-java-sdk/blob/v0.51.0-public/odps-examples/basic-examples/src/main/java/GenerateExpressionSample.java)
- **UpsertStream** supports writing values with primary keys of type `TIMESTAMP_NTZ`
- **Table** added new methods for querying CDC-related data: `getCdcSize()`, `getCdcRecordNum()`, `getCdcLatestVersion()`, `getCdcLatestTimestamp()`
- **SQLExecutor** MCQA 2.0 job supports retrieving InstanceProgress information

### Changes
- **TypeInfo** added backticks for quoting names in Struct type TypeInfo and other methods that assemble SQL
- **AutoClosable** to remind users to properly close resources, added corresponding `close()` methods to the following resource classes to prompt users to close resources correctly:
  - `UpsertStream` in the `odps-sdk-core` package,
  - `LocalOutputStreamSet`, `ReduceDriver.ReduceContextImpl`, `MapDriver.DirectMapContextImpl`, `LocalRecordWriter` in the `odps-sdk-impl` package
  - `VectorizedOutputer`, `VectorizedExtractor`, `RecordWriter`, `RecordReader`, `Outputer`, `Extractor` in the `odps-sdk-udf` package

## [0.50.5-public] - 2024-11-13

### Features

- **TableAPI** added retry logic for errors in network requests that can be safely retried,
  improving the stability of the interface. A new configuration option, `retryWaitTimeInSeconds`,
  has been added to `RestOptions` to specify the retry wait time.
- **SQLTask** added an overload of the `run` method that supports passing in the `mcqaConnHeader`
  parameter for submitting MCQA 2.0 jobs.
- **SQLExecutor** now supports specifying the `odps.task.wlm.quota` hint to set the interactive
  quota when submitting MCQA 2.0 jobs.
- **RestClient** introduced a new `retryWaitTime` parameter along with corresponding getter and
  setter methods to configure the retry wait time for network requests.
- **Configuration** added a new `socketRetryTimes` parameter with corresponding getter and setter
  methods to configure the retry wait time for Tunnel network requests. If not set, it will use the
  configuration in `RestClient`; otherwise, this configuration will be used.

### Changes

- **Instances** removed the overloaded `get`
  method `get(String projectName, String id, String quotaName, String regionId)`, which was added in
  version `0.50.2-public` to retrieve MCQA 2.0 instances. Now, users do not need to distinguish
  whether a job is an MCQA 2.0 job when using the `get` method, so this method has been removed.
  Users can directly use the `get(String projectName, String id)` method to retrieve instances.

### Fixes

- **Table.read** fixed an issue where the configured network-related parameters (such as timeout
  and retry logic) did not take effect correctly during data preview.
- **Streams** fixed an issue where specifying the `version` in the `create` method would cause an
  error. A default value of `1` has also been added for `version`, indicating the initial version of
  the table.

## [0.50.4-public] - 2024-10-29

### Features

- **PartitionSpec** Added a new constructor `(String, boolean)` that uses a boolean parameter to
  specify whether to trim partition values. This caters to scenarios (such as using char type as a
  partition field) where users may not want to trim partition values.

### Changes

- **Instance** The OdpsException thrown when calling the stop method will no longer be wrapped a
  second time.

### Fixes

- **SQLExecutor**
    - Fixed an issue in MCQA 1.0 mode where the
      user-specified `fallbackPolicy.isFallback4AttachError` did not take effect correctly.
    - Fixed an issue in MCQA 2.0 mode where the `cancel` method threw an exception when the job
      failed.
    - Fixed an issue in MCQA 2.0 mode where using instanceTunnel to fetch results resulted in an
      error when the isSelect check was incorrect.
- **Table** Fixed an issue with the `getPartitionSpecs` method that trimmed partition values,
  causing the retrieval of non-existing partitions.

## [0.50.3-public] - 2024-10-23

### Features

- **SQLExecutor** In MCQA 1.0 mode, it is allowed to add custom fallback policies, add
  subclass `FallbackPolicy.UserDefinedFallbackPolicy`.

## [0.50.2-public] - 2024-10-23

### Features

- **SQLExecutor** Enhanced MCQA 2.0 functionality:
    - `isActive` will return false, indicating that there are no active Sessions in MCQA 2.0 mode.
    - Added a `cancel` method to terminate ongoing jobs.
    - `getExecutionLog` now returns a deep copy of the current log and clears the current log,
      preventing duplicates.
    - New `quota` method in `SQLExecutorBuilder` allows reusing already loaded `Quota`, reducing
      load times.
    - New `regionId` method in `SQLExecutorBuilder` allows specifying the region where the quota is
      located.
- **Quotas** Added `getWlmQuota` method with `regionId` parameter to fetch quota for a specified
  regionId.
- **Quota** Introduced `setMcqaConnHeader` method to allow users to override quota using a custom
  McqaConnHeader, supporting MCQA 2.0.
- **Instances** Added `get` method applicable for MCQA 2.0 jobs, requiring additional parameters for
  QuotaName
  and RegionId.
- **Instance** Further adapted for MCQA 2.0 jobs.
- **TableSchema** `basicallyEquals` method will no longer strictly check for identical Class types.

### Optimization

- **SQLExecutor** The `run` method's hints will now be deep-copied, preserving the user-provided Map
  and
  supporting immutable types (e.g., `ImmutableMap`).

### Fixes

- **Stream** Fixed potential SQL syntax errors in the `create` method.

## [0.50.1-public] - 2024-10-11

### Fixes

- **TableAPI** Fixed an issue where `ArrayRecord` could not correctly invoke `toString` when
  using `SplitRecordReaderImpl` to retrieve results.
- **TableAPI** Fixed an issue where a `get` operation would throw an array index out of bounds
  exception when the number of `Records` corresponding to a `Split` is 0 while
  using `SplitRecordReaderImpl` to retrieve results.
- **TableAPI** Fixed an issue with composite predicates `CompositePredicate` that could lead to an
  additional operator being added when encountering an empty predicate.

## [0.50.0-public] - 2024-10-09

### Features

- Added `SchemaMismatchException`: This exception will be thrown when using `StreamUploadSession` if
  the Record structure uploaded by the user does not match the table structure. This exception will
  additionally carry the latest schema version to assist users in rebuilding the Session and
  performing retry operations.
- Added `allowSchemaMismatch` method in `StreamUploadSession.Builder`: This method specifies whether
  to tolerate mismatches between the user's uploaded Record structure and the table structure
  without throwing an exception. The default value is `true`.

### Fixes

- Fixed an issue where specifying `tunnelEndpoint` in Odps was ineffective when
  using `StreamUploadSession`.
- Fixed a potential NPE issue in `TunnelRetryHandler`.

## [0.50.0-rc1] - 2024-09-19

### Features

- **SQLExecutor** added `isUseInstanceTunnel` method:
    - Used to determine whether to use instanceTunnel to obtain results

### Fix

- Fixed an issue where when using SQLExecutor to execute MCQA 2.0 jobs, executing the CommandApi
  task would affect the next job, causing NPE to be thrown when retrieving results.

## [0.50.0-rc0] - 2024-09-18

### Features

- **SQLExecutor** supports submitting MCQA 2.0 jobs
    - SQLExecutorBuilder adds method `enableMcqaV2`
    - SQLExecutorBuilder adds getter methods for fields
- SQLExecutor adds `getQueryId` method:
    - For offline jobs and MCQA 2.0 jobs, it returns the currently executing job's InstanceId
    - For MCQA 1.0 jobs, it returns the InstanceId and SubQueryId
- **TableAPI** adds `SharingQuotaToken` parameter in `EnvironmentSettings` to support sharing quota
  resources during job submission
- **Quotas** introduces `getWlmQuota` method:
    - Allows retrieval of detailed quota information based on projectName and quotaNickName,
      including whether it belongs to interactive quotas
- **Quota class** adds `isInteractiveQuota` method to determine if a quota belongs to interactive
  quotas (suitable for MCQA 2.0)
-

Adds `getResultByInstanceTunnel(Instance instance, String taskName, Long limit, boolean limitEnabled)`
method:
- Allows unlimited retrieval of results via instanceTunnel (lifting restrictions requires higher
permissions)

- **UpsertSession.Builder** adds `setLifecycle` method to configure the session lifecycle

### Fixes

- Fixed the issue where using SQLExecutor to execute offline jobs with `limitEnabled` specified
  resulted in no effect
- Modified the SQLExecutor so that `getQueryId` method returns the job's instanceID instead of null
  when executing offline jobs
- Fixed the issue where using instanceTunnel to retrieve results on encountering non-select
  statements no longer throws exceptions, instead falling back to non-tunnel logic
- Fixed the problem of missing one data entry when using DownloadSession to download data and an
  error occurred while the read count equaled the number of records to be read minus one
- The `clone` method of the **Odps class** now correctly clones other fields,
  including `tunnelEndpoint`
- The **Instance**'s `getRawTaskResults` method now does not make multiple requests when processing
  synchronous jobs

## [0.49.0-public] - 2024-09-12

### Features

- **OdpsRecordConverter Enhancement**: Now supports converting data to SQL-compatible formats. For
  example, for the `LocalDate` type, data can be converted to `"DATE 'yyyy-mm-dd'"` format.
  Additionally, for the `Binary` type, hex representation format is now supported.

- **Enhanced Predicate Pushdown for Storage Constants**: Improved the behavior of the `Constant`
  class and added the `Constant.of(Object, TypeInfo)` method. Now, when setting or identifying types
  as time types, the conversion to SQL-compatible format can be done correctly (enabling correct
  pushdown of time types). Other type conversion issues have been fixed;
  an `IllegalArgumentException` will be thrown during session creation when conversion to
  SQL-compatible mode is not possible.

- **UpsertSession Implements Closable Interface**: Notifies users to properly release local
  resources of the UpsertSession.

- **SQLExecutorBuilder New Method** `offlineJobPriority`: Allows setting the priority of offline
  jobs when a job rolls back.

- **New Method in Table Class** `getLastMajorCompactTime`: Used to retrieve the last time the table
  underwent major compaction.

- **New Method in Instance Class** `create(Job job, boolean tryWait)`: When the `tryWait` parameter
  is true, the job will attempt to wait on the server for a period of time to obtain results more
  quickly.

- **Resource Class Enhancement**: Now able to determine if the corresponding resource is a temporary
  resource.

- **CreateProjectParma class enhancement** Added `defaultCtrlService` parameter to specify the
  default control cluster of the project.

### Fixes

- **UpsertStream NPE Fix**: Fixed an issue where an NPE was thrown during flush when a local error
  occurred, preventing a proper retry.

- **Varchar/Char type fix**: Fixed the problem that when the `Varchar/Char` type obtains its length
  and encounters special characters such as Chinese symbols or emoticons, it will be incorrectly
  calculated twice.

## [0.48.8-public] - 2024-08-12

### Enhancement

- Introduced internal validation of compound predicate expressions, fixed logic when handling
  invalid or always true/false predicates, enhanced test coverage, and ensured stability and
  accuracy in complex query optimization.

## [0.48.7-public] - 2024-08-07

### Enhancements

- **TableTunnel Configuration Optimization**: Introduced the `tags` attribute
  to `TableTunnel Configuration`, enabling users to attach custom tags to tunnel operations for
  enhanced logging and management. These tags are recorded in the tenant-level `information schema`.

```java
Odps odps;
    Configuration configuration=
    Configuration.builder(odps)
    .withTags(Arrays.asList("tag1","tag2")) // Utilize Arrays.asList for code standardization
    .build();
    TableTunnel tableTunnel=odps.tableTunnel(configuration);
// Proceed with tunnel operations
```

- **Instance Enhancement**: Added the `waitForTerminatedAndGetResult` method to the `Instance`
  class, integrating optimization strategies from versions 0.48.6 and 0.48.7 for the `SQLExecutor`
  interface, enhancing operational efficiency. Refer
  to `com.aliyun.odps.sqa.SQLExecutorImpl.getOfflineResultSet` for usage.

### Improve

- **SQLExecutor Offline Job Processing Optimization**: Significantly reduced end-to-end latency by
  enabling immediate result retrieval after critical processing stages of offline jobs executed
  by `SQLExecutor`, without waiting for the job to fully complete, thus boosting response speed and
  resource utilization.

### Fixes

- **TunnelRetryHandler NPE Fix**: Rectified a potential null pointer exception issue in
  the `getRetryPolicy` method when the error code (`error code`) was `null`.

## [0.48.6-public] - 2024-07-17

### Added

- **Serializable Support**:
    - Key data types like `ArrayRecord`, `Column`, `TableSchema`, and `TypeInfo` now support
      serialization and deserialization, enabling caching and inter-process communication.
- **Predicate Pushdown**:
    - Introduced `Attribute` type predicates to specify column names.

### Changed

- **Tunnel Interface Refactoring**:
    - Refactored Tunnel-related interfaces to include seamless retry logic, greatly enhancing
      stability and robustness.
    - Removed `TunnelRetryStrategy` and `ConfigurationImpl` classes, which are now replaced
      by `TunnelRetryHandler` and `Configuration` respectively.

### Improve

- **SQLExecutor Optimization**:
    - Improved performance when executing offline SQL jobs through the `SQLExecutor` interface,
      reducing one network request per job to fetch results, thereby decreasing end-to-end latency.

### Fixed

- **Decimal Read in Table.read**:
    - Fixed issue where trailing zeroes in the `decimal` type were not as expected in
      the `Table.read` interface.

## [0.48.5-public] - 2024-06-17

### Added

- Added the `getPartitionSpecs` method to the `Table` interface. Compared to the `getPartitions`
  method, this method does not require fetching detailed partition information, resulting in faster
  execution.

### Changes

- Removed the `isPrimaryKey` method from the `Column` class. This method was initially added to
  support users in specifying certain columns as primary keys when creating a table. However, it was
  found to be misleading in read scenarios, as it does not communicate with the server. Therefore,
  it is not suitable for determining whether a column is a primary key. Moreover, when using this
  method for table creation, primary keys should be table-level fields (since primary keys are
  ordered), and this method neglected the order of primary keys, leading to a flawed design. Hence,
  it has been removed in version 0.48.5.

  For read scenarios, users should use the `Table.getPrimaryKey()` method to retrieve primary keys.
  For table creation, users can now use the `withPrimaryKeys` method in the `TableCreator` to
  specify primary keys during table creation.

### Fixes

- Fixed an issue in the `RecordConverter` where formatting a `Record` of type `String` would throw
  an exception when the data type was `byte[]`.

## [0.48.4-public] - 2024-06-04

### New

- Use `table-api` to write MaxCompute tables, now supports `JSON` and `TIMESTAMP_NTZ` types
- `odps-sdk-udf` functions continue to be improved

### Change

- When the Table.read() interface encounters the Decimal type, it will currently remove the trailing
  0 by default (but will not use scientific notation)

### Fix

- Fixed the problem that ArrayRecord does not support the getBytes method for JSON type

## [0.48.3-public] - 2024-05-21

### Added

- Support for passing `retryStrategy` when building `UpsertSession`.

### Changed

- The `onFlushFail(String, int)` interface in `UpsertStream.Listener` has been marked
  as `@Deprecated` in favor of `onFlushFail(Throwable, int)` interface. This interface will be
  removed in version 0.50.0.
- Default compression algorithm for Tunnel upsert has been changed to `ODPS_LZ4_FRAME`.

### Fixed

- Fixed an issue where data couldn't be written correctly in Tunnel upsert when the compression
  algorithm was set to something other than `ZLIB`.
- Fixed a resource leak in `UpsertSession` that could persist for a long time if `close` was not
  explicitly called by the user.
- Fixed an exception thrown by Tunnel data retrieval interfaces (`preview`, `download`) when
  encountering invalid `Decimal` types (such as `inf`, `nan`) in tables; will now return `null` to
  align with the `getResult` interface.

## [0.48.2-public] - 2024-05-08

### Important fixes

- Fixed the issue of relying on the user's local time zone when bucketing primary keys of DATE and
  DATETIME types during Tunnel upsert. This may lead to incorrect bucketing and abnormal data query.
  Users who rely on this feature are strongly recommended to upgrade to version 0.48.2.

### Added

- `Table` adds a method `getTableLifecycleConfig()` to obtain the lifecycle configuration of
  hierarchical storage.
- `TableReadSession` now supports predicate pushdown

## [0.48.1-public] - 2024-05-07

### Added

Arrow and ANTLR Libraries: Added new includes to the Maven Shade Plugin configuration for better
handling and packaging of specific libraries. These includes ensure that certain essential libraries
are correctly packaged into the final shaded artifact. The newly included libraries are:

- org.apache.arrow:arrow-format:jar
- org.apache.arrow:arrow-memory-core:jar
- org.apache.arrow:arrow-memory-netty:jar
- org.antlr:ST4:jar
- org.antlr:antlr-runtime:jar
- org.antlr:antlr4:jar
- org.antlr:antlr4-runtime:jar

### Relocation Adjustments

Shaded Relocation for ANTLR and StringTemplate: The configuration now includes updated relocation
rules for org.antlr and org.stringtemplate.v4 packages to prevent potential conflicts with other
versions of these libraries that may exist in the classpath. The new shaded patterns are:
org.stringtemplate.v4 relocated to com.aliyun.odps.thirdparty.org.stringtemplate.v4
org.antlr relocated to com.aliyun.odps.thirdparty.antlr

## [0.48.0-public] - 2024-04-22

### Added

- Introduced `odps-sdk-udf` module to allow batch data reading in UDFs for MaxCompute, significantly
  improving performance in high-volume data scenarios.
- `Table` now supports retrieving `ColumnMaskInfo`, aiding in data desensitization scenarios and
  relevant information acquisition.
- Support for setting proxies through the use of `odps.getRestClient().setProxy(Proxy)` method.
- Implementation of iterable `RecordReader` and `RecordReader.stream()` method, enabling conversion
  to a Stream of `Record` objects.
- Added new parameters `upsertConcurrentNum` and `upsertNetworkNum` in `TableAPI RestOptions` for
  more detailed control for users performing upsert operations via the TableAPI.
- Support for `Builder` pattern in constructing `TableSchema`.
- Support for `toString` method in `ArrayRecord`.

### Improved

- `UploadSession` now supports configuration of the `GET_BLOCK_ID` parameter to speed up session
  creation when the client does not need `blockId`.
- Enhanced table creation method using the `builder` pattern (`TableCreator`), making table creation
  simpler.

### Fixed

- Fixed a bug in `Upsert Session` where the timeout setting was configured incorrectly.
- Fixed the issue where `TimestampWritable` computed one second less when nanoseconds were negative.

## [0.47.0-public] - 2024-04-08

### Added

- Support for new Stream type that enables incremental queries.
- `preview` method to the `TableTunnel` for data preview purposes.
- `OdpsRecordConverter` for parsing and formatting records.
- Enhancements to the `Projects` class with `create` and `delete` methods now available,
  and `update` method made public. Operations related to the `group-api` package are now marked as
  deprecated.
- Improved `Schemas` class to support filtering schemas with `SchemaFilter`, listing schemas, and
  retrieving detailed schema metadata.
- `DownloadSession` introduces new parameter `disableModifiedCheck` to bypass modification checks
  and `fetchBlockId` to skip block ID list retrieval.
- `TableWriteSession` supports writing `TIMESTAMP_NTZ` / `JSON` types and adds a new
  parameter `MaxFieldSize`.
- `TABLE_API` adds `predicate` related classes to support predicate pushdown in the future.

### Changed

- The implementation of the `read` method in the `Table` class is now replaced
  with `TableTunnel.preview`, supporting new types in MaxCompute and time types switched to Java 8
  time types without timezone.
- The default `MapWritable` implementation switched from `HashMap` to `LinkedHashMap` to ensure
  order.
- `Column` class now supports creation using the Builder pattern.

### Improved

- `TableReadSession` now introduces new parameters `maxBatchRawSize` and `splitMaxFileNum`.
- `UpsertSession` enhancements:
    - Supports writing partial columns.
    - Allows setting the number of Netty thread pools with the default changed to 1.
    - Enables setting maximum concurrency with the default value changed to 16.
- `TableTunnel` now supports setting `quotaName` option.

