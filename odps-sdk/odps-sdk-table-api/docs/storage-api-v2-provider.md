# Storage API v2 session provider (identifier "v2")

`odps-sdk-table-api` ships an optional read and batch-write provider for the MaxCompute
Storage API v2 protocol (`/api/storage/v3` wire, served by the same front end as v1).
Select it explicitly:

```java
TableBatchReadSession session = new TableReadSessionBuilder()
    .identifier(TableIdentifier.of("project", "default", "table"))
    .withSettings(settings)
    .withSessionProvider("v2")   // default remains "v1" (legacy Storage API REST)
    .buildBatchReadSession();
```

Both providers are discovered through `ServiceLoader`
(`META-INF/services/com.aliyun.odps.table.Session$Provider`). The `"tunnel"` provider
implemented inside Spark/Flink connectors is unaffected.

## Capability matrix — read side

| Feature | v1 (default) | v2 provider |
| --- | --- | --- |
| Column pruning / partition columns | yes | yes |
| Partition pruning (`requiredPartitions`) | yes | yes |
| Bucket pruning (`requiredBucketIds`) | yes | yes |
| Filter predicate pushdown + fallback | yes | yes |
| Split mode SIZE | yes | yes |
| Split mode ROW_OFFSET | yes | yes |
| Split mode BUCKET | yes | yes |
| Split mode PARALLELISM | yes | **not supported** — fails fast at session build |
| Arrow + configurable ZSTD/LZ4_FRAME compression | yes | native v2 compression policy; legacy codec selection is ignored |
| Record reader (`createRecordReader`) | yes | yes, Arrow-backed conversion |
| Caller-owned batches (`reuseBatch=false`) | yes | yes, ordinary and extended Arrow IPC |
| Extended Arrow IPC server metrics frames | yes | yes |
| Incremental read (version/timestamp range, cdc/append, StartTxnId) | yes | yes |
| Snapshot / LatestVersion / LatestTxnId | yes | yes |
| Estimated stats (`enableEstimateStats`) | yes | yes |
| Unique map key (`enableUniqueMapKey`) | yes | yes |
| Session JSON persistence (`toJson` / `fromJson`) | yes | within v2 only; restore requires `withSettings` (see below) |
| Async / disk-spill buffered reading | yes | async yes; **disk spill not wired** in v2 reader |
| Read cancellation (`CancellableSplitReader`) | yes | **not supported** in v2 reader |
| Local data-proxy environment (`LOCAL` mode) | yes | remote endpoints only |

## Capability matrix — write side

| Feature | v1 (default) | v2 provider |
| --- | --- | --- |
| Batch write with block/attempt writers | yes | yes (`BATCH_COMPATIBLE` mode) |
| Static partition target | yes | yes |
| Commit / abort / required ordering & distribution metadata | yes | yes |
| Enhance write check (commit message validation) | yes | yes |
| Dynamic partition writes | yes | batch-compatible mode; partition limits and invalid-row options forwarded to the service |
| Hash/range cluster write capability | yes | forwarded; caller must obey required distribution and ordering |
| Streaming writes (`TableStreamingWriteSession`) | no implementation in SDK | **not offered** |
| Upsert (`TableUpsertSession`) | no implementation in SDK | **not offered** |
| Record writers (`createRecordWriter`) | no implementation in SDK | **not supported**, Arrow only |
| `WaitFlyingWritersTimeoutSeconds` commit option | yes | forwarded; explicit values must be 1–600 seconds |
| Session JSON restore of a live write session | partial | restore via `sessionId` reload only |

Streaming and upsert remain on the connector-side `"tunnel"` providers until their
table-api interfaces are implemented.

## Batch upload buffering

`WriterOptions.Builder.withUploadBufferSize(bytes)` controls aggregation of the
V2 writer's compressed Arrow output before it enters the upload queue. The default
is **262144 bytes (256 KiB) per active block writer**. Set it to `0` to disable
aggregation for diagnostics; negative values are rejected. Normal callers do not
need to configure this option.

```java
WriterOptions options = WriterOptions.newBuilder()
    .withSettings(settings)
    .build();
BatchWriter<VectorSchemaRoot> writer =
    session.createArrowWriter(0, WriterAttemptId.of(0), options);
```

This is a local writer option, independent of `withChunkSize` (upload chunk
limit) and Arrow row batch size. V2 batch-compatible uploads still use ZSTD.
V1 ignores the new option. Explicit `flush()` drains pending bytes and waits
for network flush; close/commit drains the final partial buffer. Abort cancels
the upload without flushing pending local bytes.

Native Storage API callers can use
`TableWriteSessionBuilder.withUploadBufferSize(bytes)` with
`WriteMode.BATCH_COMPATIBLE`. For distributed workers, set it on the builder
that restores the session descriptor: it is not serialized into the descriptor
or sent to the service. Native BATCH/STREAMING builders reject an explicitly
configured upload buffer.

## Known limitations and workarounds

1. **PARALLELISM split mode**: the v2 client models the mode but the session planner does
   not plan parallelism splits yet. Use SIZE or ROW_OFFSET splits; the provider throws
   `UnsupportedOperationException` at `buildBatchReadSession()` so jobs fail fast rather
   than silently changing split counts.
2. **Route token durability**: v2 issues a route token pinning requests to one front-end
   instance. It is cached in memory per session and deliberately kept out of serialized
   session JSON; a session restored in another process relies on the router's re-dispatch
   (`x-odps-max-storage-route-token` absence is tolerated server-side).
3. **Cross-provider session JSON**: a v2 `toJson()` payload cannot be loaded by the v1
   provider and vice versa (different response field sets). Keep the provider identifier
   alongside persisted session state. Unlike v1, restoring a v2 read session requires explicit
   environment settings on the builder:

   ```java
   new TableReadSessionBuilder()
       .withSessionProvider("v2")
       .withSettings(settings)
       .fromJson(json)
       .buildBatchReadSession();
   ```

   Bare `fromJson(json)` without `withSettings(settings)` is a known unsupported use case;
   credentials are not embedded in session JSON.
4. **Disk spill / cancellable reads**: the v2 reader returns plain `SplitReader`s. Jobs
   relying on `DiskSpillBufferOptions` or `CancellableSplitReader` should stay on v1
   until those paths are ported.
5. **Dynamic partition validation**: batch-compatible sessions forward
   `DynamicPartitionLimit`, `InvalidLimit`, and `InvalidStrategy`. The tested service accepts
   `Exception`; `Drop` and `Limit` are rejected by the service, as with v1. The project/service
   partition-count limit still applies; forwarding options does not bypass validation.
   Streaming/upsert writes are outside this provider's batch-compatible support.
6. **Auth surface**: the provider reads AK/STS credentials from the account bridge on each
   request and applies configured `AppAccount` and `AppStsAccount` additional signatures
   to the resulting primary Authorization header. It does not reuse the primary account's
   custom signer or promise v4 signing compatibility; validate those environments separately.

## Testing

Offline coverage (no live service):
- `odps-sdk-table-api/src/test/java/com/aliyun/odps/table/read/impl/v2/` — request mapping,
  split planning, session JSON round-trip against recorded wire fixtures.
- `odps-sdk-table-api/src/test/java/com/aliyun/odps/table/write/impl/v2/` — batch
  compatible create/commit request shapes and commit-message checks.
- `odps-sdk-storage-api/src/test/java/com/aliyun/odps/storage/` — model serialization
  regressions for the fields added for this provider.

Live-service E2E (credentials required) tracks the same matrix items marked "yes".

## Allocators and extended Arrow IPC

Arrow writers use the allocator supplied through `WriterOptions.withBufferAllocator`,
including writers created from reloaded sessions. The allocator remains caller-owned:
close each `VectorSchemaRoot`, then close the allocator after the writers have finished.

The standalone Storage API client also decodes negotiated extended Arrow IPC streams.
Both synchronous and asynchronous readers consume tagged Arrow substreams and keep
server metrics frames separate from table data. The table-api provider exposes these
metrics through its existing counters.

## Record conversion and batch ownership

Storage API v2 already provides `ArrowReader.getAsRecordReader()` when a table schema is
available. The table-api provider exposes the equivalent `createRecordReader` contract
through the shared `SplitRecordReaderImpl` adapter, preserving table-api schemas, metrics,
and ownership options on its v2 Arrow stream.

With `reuseBatch=false`, each returned Arrow batch belongs to the caller and remains valid
when the reader advances or closes. Close every returned batch to release its memory.
With `reuseBatch=true`, batches remain reader-owned and must not be retained after advancing
or closing the reader. Disk-spill buffering remains unsupported; setting its options does
not enable spilling in v2. Use v1 for workloads that require spill budgets or spill metrics.

## Existing v1 configuration

The v2 provider forwards `sharingQuotaToken` and tunnel tags to all Storage API requests,
including read streams and block uploads. Both application-signature options in
`Credentials` are retained for read and write sessions.

Write `ArrowOptions` control both the create-session request and the Arrow vectors used
by block writers. When reopening a write session by `sessionId`, supply the same
`ArrowOptions` used at creation: the service response does not return those units.
Omitting them selects the usual nanosecond timestamp / millisecond datetime defaults.

Explicit `RestOptions` connection, read and write timeouts (seconds), User-Agent, and
`ignoreCerts` are applied to the v2 HTTP transport. Certificate and hostname verification
remain enabled unless `ignoreCerts=true` is explicitly supplied.

For metadata and read-stream establishment, explicit `retryTimes` bounds retries after the
initial request, including throttling;
zero disables these SDK retries. `retryWaitTimeInSeconds` sets the delay (zero allows
immediate retries). Permanent service errors such as 403 are not retried. Without explicit
retry settings, the native Storage API policy remains in effect, including its throttling
backoff. The v1 and v2 retry algorithms are not identical, and transport recovery / session
commit recovery are separate from these per-request SDK retries.

These compatibility bridges do not remove the limitations above. Migration acceptance
must cover the customer's actual options, account types, data types and read/write paths;
provider-only test substitution is not proof of seamless migration for every v1 workload.

## Batch-compatible streaming and worker settings

The table-api v2 batch provider always selects `BATCH_COMPATIBLE`, including sessionId
reload. It retains block/attempt and commit-message semantics from v1. Block writers stream
Arrow batches through a bounded transport queue; `flush()` waits for pending bytes to reach
the network, and `close()` completes the Arrow stream and waits for the block result.
A block becomes visible only after session commit. The caller can reuse or close a batch
after `write()` returns.

This also changes the standalone `BATCH_COMPATIBLE` writer from buffering/replaying a
whole block to one-shot streaming. A failed or aborted upload cannot be resumed or replayed:
create a new writer with the next attempt number and resend the block, as with v1. Upload
write/flush failures are retained by later commit calls; abort cancels in-flight I/O.

`ReaderOptions.withSettings` and `WriterOptions.withSettings` select the data-plane identity,
endpoints, quota, tags and HTTP settings for each worker. Session operations continue using
the session builder's settings. Independent worker clients close with their reader/writer;
caller-owned Arrow allocators remain open.

Compression is controlled by the native v2 protocol, not by the legacy table-api
`CompressionCodec` option. Reads retain the fixed `x-lz4-frame` negotiation; compatible
block writes use fixed ZSTD Arrow IPC compression, matching the native v2 writer. Passing NO_COMPRESSION, ZSTD or LZ4_FRAME
in reader/writer options does not override that policy. Compatibility refers to the
block/attempt and data ownership contracts, not exposing every v1 transport setting.

Commit rejects a non-null message
from another provider before sending a request; it never silently drops that block.

## Commit recovery and cleanup

An unsuccessful commit RPC may still have committed on the service. The provider reloads
its state and polls COMMITTING using the configured async interval and timeout. COMMITTED
is success; only a freshly confirmed NORMAL state permits one retry of the commit request.
An interrupted wait, an expired polling timeout, or a failed status query reports an
unknown outcome. Keep the session id and recover its status before starting a replacement
session; blindly replaying the job may duplicate data. Interrupt status is preserved.

`cleanup()` aborts only a NORMAL session on which this provider has not started commit.
It never aborts a known-success or potentially committing session. After an uncertain
commit, cleanup leaves server state intact for recovery; it does not imply a rollback.
Repeated commit calls on a known-success session are harmless. The optional
`WaitFlyingWritersTimeoutSeconds` is forwarded on the initial request and any retry;
omitting it retains the service default.
