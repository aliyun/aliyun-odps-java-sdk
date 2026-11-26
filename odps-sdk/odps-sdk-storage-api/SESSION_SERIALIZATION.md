# Session descriptors

`com.aliyun.odps.storage.models.SessionDescriptor` is an immutable, versioned
description owned by **storage-api**. Users need no table-api dependency and do not
handle Gson trees, wire response classes or Java object serialization.

## Native API

After planning a read session on the driver:

```java
String json = driverReadSession.getDescriptor().toJson();
// Transfer json to the worker through the application's transport.
SessionDescriptor descriptor = SessionDescriptor.fromJson(json);
try (TableReadSession workerSession = workerClient
    .createTableReadSessionBuilder(descriptor.getTableIdentifier())
    .withSessionDescriptor(descriptor).build()) {
  // getTableSchema()/getSplits() are local; createReaderBuilder(split) opens data streams.
}
```

Batch-compatible writes use the same pattern:

```java
SessionDescriptor descriptor = SessionDescriptor.fromJson(driverWriteSession.getDescriptor().toJson());
try (TableWriteSession workerSession = workerClient
    .createTableWriteSessionBuilder(descriptor.getTableIdentifier())
    .withSessionDescriptor(descriptor).build()) {
  // createBlockWriter(block, attempt) reserves quota for that attempt.
  // Return its BlockWriteResult to the original driver for global commit.
}
```

Parsing and binding an existing description do not create/get a remote session.
The worker's MaxStorageClient supplies its own endpoint and credentials. Sessions
borrow that client's stub and allocator; closing a restored session does not close
the worker client or abort the global write session. Keep the client alive while
its readers/writers are in use.

A restored write session supports block data operations only. Global commit/abort
are rejected. The original driver, or an explicit ID-based coordinator reload,
owns those operations. A description exported after commit starts cannot reopen
writers even if the last observed remote status is still NORMAL.

Native write export supports BATCH_COMPATIBLE only. BATCH and STREAMING are
explicitly rejected until their distinct recovery contracts are defined.

## Format

The numeric version is 1. The envelope contains provider=v2, kind (batch-read,
incremental-read or batch-write), table identity, allowlisted metadata, and optional
opaque routeToken. Write descriptors include explicit timestamp/datetime Arrow
units. Native ROW_OFFSET read descriptions also preserve readOptions.rowsPerSplit.

Metadata preserves schema (including system/blob columns and column IDs), split
planning/statistics, incremental options, or write block limit/ordering/distribution
and commit-started state. Runtime clients, credentials, settings, allocators,
streams and reserved block quota tokens are never exported. Treat descriptions
as internal task data because they contain session identifiers and routing data.

Unknown optional fields may be ignored; unsupported versions/kinds and malformed
required metadata are rejected. Future incompatible changes require a new version
and an explicit migration policy. Export after planning and before dispatch;
descriptions are metadata snapshots, not a checkpoint of concurrent commit activity.

## Internal table-api adapter

Table-api is a Connector implementation layer. It consumes the storage-api codec
and native worker write restoration. Its package-private SessionSerialization and
Java SessionProxy bridge Spark's existing Serializable transport by carrying only
the JSON description. Each table-api reader/writer owns its independently created
worker client; caller allocators remain borrowed.

V1 transport is unchanged. Old v2 Java implementation-object streams are explicitly
rejected; use matching SDKs on driver and executors. Legacy v2 read identifier/details
JSON remains importable by the internal read builder. Cross-provider restoration
and arbitrary-version durable checkpoints are not promised.
