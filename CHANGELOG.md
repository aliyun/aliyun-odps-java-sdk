# Changelog
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

