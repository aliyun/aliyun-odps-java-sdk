# Changelog

## [0.47.0-public] - 2024-04-08

### Added
- Support for new Stream type that enables incremental queries.
- `preview` method to the `TableTunnel` for data preview purposes.
- `OdpsRecordConverter` for parsing and formatting records.
- Enhancements to the `Projects` class with `create` and `delete` methods now available, and `update` method made public. Operations related to the `group-api` package are now marked as deprecated.
- Improved `Schemas` class to support filtering schemas with `SchemaFilter`, listing schemas, and retrieving detailed schema metadata.
- `DownloadSession` introduces new parameter `disableModifiedCheck` to bypass modification checks and `fetchBlockId` to skip block ID list retrieval.
- `TableWriteSession` supports writing `TIMESTAMP_NTZ` / `JSON` types and adds a new parameter `MaxFieldSize`.

### Changed
- The implementation of the `read` method in the `Table` class is now replaced with `TableTunnel.preview`, supporting new types in MaxCompute and time types switched to Java 8 time types without timezone.
- The default `MapWritable` implementation switched from `HashMap` to `LinkedHashMap` to ensure order.
- `Column` class now supports creation using the Builder pattern.

### Improved
- `TableReadSession` now supports predicate pushdown and introduces new parameters `maxBatchRawSize` and `splitMaxFileNum`.
- `UpsertSession` enhancements:
  - Supports writing partial columns.
  - Allows setting the number of Netty thread pools with the default changed to 1.
  - Enables setting maximum concurrency with the default value changed to 16.
- `TableTunnel` now supports setting `quotaName` option.

