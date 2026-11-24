# Arrow compression implementations

`CompressionCodec.LZ4_FRAME_NATIVE` explicitly selects the official LZ4 Frame API through JavaCPP for both compression and decompression. It reads independent and dependent blocks. Arrow IPC metadata remains `LZ4_FRAME`; the name does not introduce a new server compression format.

```java
ReaderOptions readerOptions = ReaderOptions.newBuilder()
    .withCompressionCodec(CompressionCodec.LZ4_FRAME_NATIVE)
    // Configure settings and the Arrow allocator as usual.
    .build();

WriterOptions writerOptions = WriterOptions.newBuilder()
    .withCompressionCodec(CompressionCodec.LZ4_FRAME_NATIVE)
    .build();

// The lower-level helper has the same explicit selection:
ArrowReader reader = ArrowReaderBuilder.newBuilder(inputStream, allocator)
    .withCompression(CompressionCodec.LZ4_FRAME_NATIVE).build();
ArrowWriter writer = ArrowWriterFactory.newBuilder(outputStream)
    .withCompression(CompressionCodec.LZ4_FRAME_NATIVE).build();
```

Compression defaults and all writer implementations remain unchanged. Table API V1 reads configured with `LZ4_FRAME` and Table API V2 LZ4 reads prefer Native automatically. Explicit native selection remains supported, including extended IPC streams and both batch ownership modes. Table API V1 and V2 batch writers honor only their existing explicit native choice. HTTP compression names remain unchanged.

## Automatic LZ4 decoding for Table API V1 and V2 reads

Table API V1 readers configured with `LZ4_FRAME`, and `ArrowReaderBuilder.withCompression(LZ4_FRAME)`, automatically prefer native LZ4F. V1 uncompressed and ZSTD selections retain their existing implementations. Table API V2 readers prefer native LZ4F when an Arrow stream contains an LZ4-compressed buffer, including default options or an existing `ZSTD` option. No `LZ4_FRAME_NATIVE` setting is required. V1 still requests `LZ4_FRAME`; the V2 data plane still requests `x-lz4-frame`.

The automatic choice is shared by V1 and V2 reads and made lazily, once per classloader on its first LZ4 buffer. Missing bindings, unsupported platforms, native linkage failures or security restrictions during initialization select the compatible Java LZ4 implementation instead. Both implementations support linked/dependent blocks. The selected backend and an initialization failure reason are logged once. Corrupt data, decompression errors and resource exhaustion propagate to the caller; they never trigger a retry with another decoder.

For diagnostics or rollback, set `-Dodps.table.lz4.forceJava=true` before the first automatic LZ4 read. The old `odps.table.v2.lz4.forceJava` key is retained as a compatibility alias for the shared read selection. If the new key is present, its value takes precedence, including `false`. Explicit `LZ4_FRAME_NATIVE` remains strict, takes precedence over both keys and fails when native libraries cannot load. Writers are unaffected. `READ_INSTANCE` is the shared read factory; the existing public `V2_READ_INSTANCE` field remains an alias for binary compatibility.

## Runtime dependencies

The helper uses the published `org.bytedeco:lz4:1.9.4-1.5.8` bindings and matching native artifacts. It does not build or maintain a private JNI library, frame parser, or history-dictionary implementation. These are the released LZ4 **1.9.4** binaries.

Runtime classifiers for **Linux x86_64/glibc**, **macOS x86_64**, and **macOS arm64** are regular dependencies. The same Maven dependency graph works when built on Mac and executed on Linux. Do not filter these native resources out of an application fat JAR. Linux ARM64, musl/Alpine, Windows, and other targets are not included in this option's supported platform set.

JavaCPP extracts the native libraries into its cache. A writable location that permits native library loading is required; `-Dorg.bytedeco.javacpp.cachedir=/path/to/cache` can configure it. Missing/incompatible libraries or a read-only/noexec cache can prevent native loading. Automatic LZ4 reads then use Java; explicit `LZ4_FRAME_NATIVE` fails and never silently substitutes a different decoder. Malformed or checksum-failing frames are always errors.

The codec borrows Arrow direct-buffer views and allocates output through the caller's Arrow allocator. Each call owns and frees its LZ4F context and temporary size pointers, including failure paths; a codec instance can be shared across threads. Configure the allocator's budget for the workload. Lengths are validated against the Java buffer integer limit and allocator limits.

## Offline tests

```sh
# From the repository root (use -Pjdk21 on a JDK21 build):
mvn -gs maven-settings.xml test -pl odps-sdk/odps-sdk-table-api -am \
  '-Dtest=OdpsLz4NativeCompressionCodecTest,NativeLz4ReadPathTest,Lz4ReadAutoSelectionTest' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The tests use synthetic linked-block fixtures, in-memory IPC, and loopback HTTP servers with dummy accounts. No MaxCompute service or credentials are required. Coverage includes cross-implementation reads, 64 KiB and 8 MiB boundaries, malformed input and lengths, allocator limits, sliced direct buffers, concurrent context ownership, native unavailability, and V1/V2 protocol compatibility.
