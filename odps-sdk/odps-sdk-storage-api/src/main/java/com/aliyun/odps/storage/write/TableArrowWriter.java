/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.storage.write;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Blob;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.MaxStorageException;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.Constants;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.io.RawArrowRequestBody;
import com.aliyun.odps.storage.internal.models.BlobWriteItem;
import com.aliyun.odps.storage.internal.models.BlobWriteResponse;
import com.aliyun.odps.storage.internal.models.CloseWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.GetWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.internal.models.WriteSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.table.arrow.compression.OdpsZstdCompressionCodec;
import com.aliyun.odps.table.arrow.writers.ArrowCompressVectorUnloader;
import com.aliyun.odps.table.utils.SchemaUtils;
import com.aliyun.odps.utils.StringUtils;

import okhttp3.RequestBody;

/**
 * A buffered ArrowWriter implementation.
 * <p>
 * It serializes and caches the {@link VectorSchemaRoot} batches written by the user in memory.
 * When the {@link #flush()} method is called, it packages all cached batch data into a single
 * Arrow IPC Stream format request and sends it over the network at once.
 * <p>
 * This design ensures the safety of reusable VectorSchemaRoot objects for users,
 * but consumes more memory to cache the data.
 * <p>
 * <b>Threading model:</b> {@link #writeBatch}, {@link #flush}, {@link #flushAsync} and
 * {@link #close} must all be invoked by a single thread (the "writer thread"). When an
 * {@link ExecutorService} is configured via {@link TableWriterBuilder#withExecutorService},
 * network uploads are offloaded to that executor while the writer thread proceeds with the
 * next buffer; this is safe because the buffer swap is performed synchronously on the writer
 * thread before the upload task is submitted. Calling these methods from multiple threads is
 * not supported.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 * @author Refactored by Model
 */
public class TableArrowWriter implements ArrowWriter {

  private static final Logger log = LoggerFactory.getLogger(TableArrowWriter.class);
  protected final WriteSchema tableSchema;
  protected final String sessionId;
  protected final String streamId;
  protected final long streamVersion;
  protected final TableIdentifier tableId;
  protected final PartitionSpec staticPartitionSpec;
  protected final StorageStub storageStub;
  private final IpcOption ipcOption;
  private List<byte[]> cachedBatches;
  private volatile Schema schema;
  private final long bufferSize;
  private long cachedSize;
  private long bytesWritten;
  private long recordCount;
  protected final BufferAllocator allocator;
  private final boolean autoFlushEnabled;
  private final ExecutorService executorService;
  private final ReentrantLock flushLock;
  private final Semaphore flushPermits;
  private final int maxPendingBuffers;
  private volatile Future<Void> lastFlushFuture;
  private volatile MaxStorageException lastAsyncException;

  protected final List<Integer> primaryKeyColumnIndices;

  protected final WriteMode writeMode;
  protected final String streamingTableId;
  protected final Long streamingSchemaVersion;

  private String routeToken;

  /** Last request ID from writeTable response, for client logging. */
  private volatile String lastRequestId;

  /** Last StagingId from writeTable response (backend session id for streaming write). */
  private volatile String lastStagingId;

  /** Access token for Exactly-Once mode. */
  private String accessToken;

  /** Current row offset for Exactly-Once mode. */
  private long rowOffset = 0;

  /** Whether Exactly-Once mode is enabled. */
  private final boolean exactlyOnceMode;

  TableArrowWriter(TableWriterBuilder builder, CreateWriteStreamResponse response) {
    this.sessionId = builder.getSessionId();
    this.streamId = builder.getStreamId();
    this.allocator = builder.getAllocator();
    this.streamVersion = builder.getStreamVersion();
    this.tableId = builder.getTableId();
    this.storageStub = builder.getStorageStub();
    this.bufferSize = builder.getBufferSize();
    this.tableSchema = response.getDataSchema();
    this.staticPartitionSpec = builder.getStaticPartitionSpec();
    this.ipcOption = new IpcOption();
    this.cachedBatches = new ArrayList<>();
    this.autoFlushEnabled = builder.isAutoFlushEnabled();
    this.executorService = builder.getExecutorService();
    this.maxPendingBuffers = builder.getMaxPendingBuffers();
    if (this.executorService != null) {
      this.flushLock = new ReentrantLock();
      this.flushPermits = new Semaphore(maxPendingBuffers);
    } else {
      this.flushLock = null;
      this.flushPermits = null;
    }
    this.lastFlushFuture = null;
    this.lastAsyncException = null;
    this.bytesWritten = 0;
    this.cachedSize = 0;
    this.recordCount = 0;
    this.primaryKeyColumnIndices = new ArrayList<>();

    this.writeMode = builder.getWriteMode();
    this.streamingTableId = response.getTableId();
    this.streamingSchemaVersion = response.getSchemaVersion();

    List<Column> columns = tableSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      if (column.isDistributionKey()) {
        primaryKeyColumnIndices.add(i);
      }
    }
    this.routeToken = response.getRouteToken();
    this.accessToken = response.getAccessToken();
    this.exactlyOnceMode = builder.isExactlyOnceMode();

    // Validate access token in Exactly-Once mode
    if (this.exactlyOnceMode && StringUtils.isNullOrEmpty(this.accessToken)) {
      throw new ClientException(
        "Server did not return a valid access token for Exactly-Once mode. " +
        "Please ensure the server supports Exactly-Once semantics.");
    }

    if (response instanceof com.aliyun.odps.storage.internal.models.GetWriteStreamResponse) {
      com.aliyun.odps.storage.internal.models.GetWriteStreamResponse getResponse =
          (com.aliyun.odps.storage.internal.models.GetWriteStreamResponse) response;
      if (getResponse.getRowOffset() != null) {
        this.rowOffset = getResponse.getRowOffset();
      }
    }
  }

  @Override
  public void writeBatch(VectorSchemaRoot root) {
    if (root == null || root.getRowCount() == 0) {
      return;
    }

    boolean hasOperationColumn =
            tableSchema.getSystemColumns() != null
                    && tableSchema.getSystemColumns().stream()
                    .map(Column::getName)
                    .anyMatch(Constants.OPERATION_COLUMN_NAME::equals);
    if (hasOperationColumn) {
      validateAndSetOperationColumn(root);
    }
    checkLastAsyncException();

    writeInternal(root);
    if (this.autoFlushEnabled && this.cachedSize >= this.bufferSize) {
      flushAsync();
    }
  }

  /**
   * Creates a new VectorSchemaRoot instance that is compatible with this writer.
   * <p>
   * The caller is responsible for managing the lifecycle of the returned VectorSchemaRoot,
   * specifically by calling {@link VectorSchemaRoot#close()} in a try-with-resources block
   * to prevent memory leaks.
   *
   * @return A new, empty VectorSchemaRoot ready to be populated.
   */
  public VectorSchemaRoot createVectorSchemaRoot() {
    return VectorSchemaRoot.create(SchemaUtils.toArrowSchema(this.tableSchema.getColumns()), this.allocator);
  }

  private void writeInternal(VectorSchemaRoot root) {
    // Save schema on first write
    if (this.schema == null) {
      this.schema = root.getSchema();
    }
    // Key step: Immediately serialize the VSR content to ArrowRecordBatch byte array to implement "snapshot"
    // This prevents data pollution issues when users reuse VSR objects.
    VectorUnloader unloader = new ArrowCompressVectorUnloader(root, true,
                                                              new OdpsZstdCompressionCodec(), true);

    try (ArrowRecordBatch batch = unloader.getRecordBatch();
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         WriteChannel channel = new WriteChannel(Channels.newChannel(baos))) {

      MessageSerializer.serialize(channel, batch, ipcOption);
      byte[] serializedBatch = baos.toByteArray();

      // 将序列化后的字节数组存入缓存
      cachedBatches.add(serializedBatch);
      this.bytesWritten += serializedBatch.length;
      this.recordCount += root.getRowCount();
      this.cachedSize += serializedBatch.length;
    } catch (IOException e) {
      throw new ClientException(e);
    }
  }

  @Override
  public long bytesWritten() {
    return this.bytesWritten;
  }

  public void flush() {
    checkLastAsyncException();

    if (flushLock != null) {
      flushLock.lock();
      try {
        doFlush();
      } finally {
        flushLock.unlock();
      }
    } else {
      doFlush();
    }
  }

  private void doFlush() {
    waitForLastFlush();

    if (cachedBatches.isEmpty()) {
      log.info("Sync flush skipped: empty buffer [stream={}]", streamId);
      return;
    }

    long rowsToFlush = this.recordCount;
    flushInternal(cachedBatches, schema, rowsToFlush);
    cachedBatches.clear();
    this.cachedSize = 0;
    this.recordCount = 0;
  }

  /**
   * 检查并抛出异步链路发生的异常
   */
  private void checkLastAsyncException() throws MaxStorageException {
    MaxStorageException failure = lastAsyncException;
    if (failure != null) {
      // Keep the writer in a failed state, but create a fresh exception for every public call.
      // Reusing the async task's Throwable lets caller-side cleanup try to suppress the same
      // instance and fail with "Self-suppression not permitted".
      throw copyAsyncException(failure);
    }
  }

  private MaxStorageException copyAsyncException(MaxStorageException failure) {
    if (failure instanceof ServiceException) {
      ServiceException serviceFailure = (ServiceException) failure;
      return new ServiceException(serviceFailure.getHttpStatus(), serviceFailure.getErrorCode(),
                                  serviceFailure.getMessage(), serviceFailure.getRequestId(), failure);
    }
    if (failure instanceof ClientException) {
      return new ClientException(failure.getMessage(), failure);
    }
    return new MaxStorageException(failure.getMessage(), failure);
  }

  private void waitForLastFlush() throws MaxStorageException {
    Future<Void> future = lastFlushFuture;
    if (future != null) {
      try {
        future.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof MaxStorageException) {
          throw copyAsyncException((MaxStorageException) cause);
        }
        throw new ClientException("Async flush execution failed", cause);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ClientException("Interrupted while waiting for async flush", e);
      } finally {
        lastFlushFuture = null;
      }
    }
  }

  public Blob uploadBlob(long columnId, InputStream data) {
    if (!primaryKeyColumnIndices.isEmpty()) {
      throw new ClientException(
        "Cannot upload blob to PK Delta Table when use RecordWriter or not use 'batch-upload' mode. "
        + "Use ArrowWriter and set TableWriterBuilder.withBatchBlobUploadEnabled(true) to avoid this exception.");
    }

    BlobWriteResponse
      blobWriteResponse =
      storageStub.tableWriteBlob(tableId, sessionId, streamId, streamVersion, staticPartitionSpec,
                                 columnId, data);
    return Blob.fromReference(blobWriteResponse.getBlobReference());
  }

  public List<Blob> batchUploadBlob(long columnId, List<byte[]> dataList) {
    return batchUploadBlob(columnId, dataList, null, null);
  }

  public List<Blob> batchUploadBlob(long columnId, List<byte[]> dataList, String mimeType) {
    return batchUploadBlob(columnId, dataList, mimeType, null);
  }

  /**
   * Uploads multiple blobs in a single batch request.
   * <p>
   * This is significantly more efficient than calling {@link #uploadBlob(long, InputStream)}
   * repeatedly, as all blobs are uploaded in a single HTTP request instead of N individual requests.
   *
   * <p>Note: This method is not supported for tables with primary keys (Delta Tables).
   *
   * @param columnId the column ID of the BLOB column
   * @param dataList a list of byte arrays, each containing the raw data for one blob
   * @param mimeType the MIME type of the blob data (e.g. "image/png"), or null if not specified
   * @param customFileName the custom file name of the blob data, or null if not specified
   * @return a list of {@link Blob} references in the same order as the input list
   * @throws ClientException if called on a Delta Table or if the server response is inconsistent
   */
  public List<Blob> batchUploadBlob(long columnId, List<byte[]> dataList, String mimeType,
                                    String customFileName) {
    if (!primaryKeyColumnIndices.isEmpty()) {
      throw new ClientException(
        "Cannot batch upload blob to PK Delta Table. "
        + "Use ArrowWriter and set TableWriterBuilder.withBatchBlobUploadEnabled(true) to avoid this exception.");
    }

    if (dataList == null || dataList.isEmpty()) {
      return new ArrayList<>();
    }

    List<BlobWriteItem> items = new ArrayList<>(dataList.size());
    for (byte[] data : dataList) {
      BlobWriteItem item = BlobWriteItem.builder()
        .data(data)
        .columnId(columnId)
        .mimeType(mimeType)
        .customFileName(customFileName)
        .build();
      items.add(item);
    }

    BlobWriteResponse response = storageStub.tableBatchWriteBlob(
      tableId, sessionId, streamId, streamVersion, items);

    List<String> references = response.getBlobReferences();
    if (references == null || references.size() != dataList.size()) {
      throw new ClientException(
        String.format("Mismatch between sent items (%d) and received references (%d).",
                      dataList.size(), references == null ? 0 : references.size()));
    }

    List<Blob> result = new ArrayList<>(references.size());
    for (String ref : references) {
      result.add(Blob.fromReference(ref));
    }
    return result;
  }

  @Override
  public void close() {
    // Always attempt to close the server-side stream, even if flush failed, so we don't
    // leave dangling streams that the server has to time out. Surface the original
    // failure to the caller after best-effort cleanup.
    Throwable failure = null;
    try {
      flush();
      waitForLastFlush();
    } catch (Throwable t) {
      failure = t;
      log.error("Flush during close failed; will still attempt to close write stream [stream={}]",
          streamId, t);
    }

    // Legacy unpartitioned streaming uses session id "default" — no TableCloseWriteStream.
    // STREAMING with an explicit session (e.g. static partition) must close the stream like BATCH.
    boolean legacyDefaultStreaming =
        writeMode.isStreaming()
            && Constants.AUTO_COMMIT_SESSION_ID.equals(sessionId);
    boolean skipCloseStream =
        legacyDefaultStreaming || Constants.AUTO_COMMIT_DEFAULT_STREAM_ID.equals(streamId);

    if (!skipCloseStream) {
      try {
        CloseWriteStreamRequest closeWriteStreamRequest =
            CloseWriteStreamRequest.newBuilder().
                withSessionId(sessionId).
                withStreamId(streamId).
                withStreamVersion(streamVersion).
                build();
        storageStub.closeWriteStream(tableId, closeWriteStreamRequest, routeToken, writeMode);
      } catch (Throwable t) {
        if (failure == null) {
          failure = t;
        } else {
          failure.addSuppressed(t);
        }
      }
    }

    if (failure != null) {
      if (failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      }
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      throw new ClientException("Failed to close TableArrowWriter", failure);
    }
  }

  public WriteSchema getWriteSchema() {
    return tableSchema;
  }
  /**
   * Asynchronously flushes the current buffer when an {@link ExecutorService} is configured.
   * <p>
   * The current buffer is handed off to the executor for network upload and a fresh buffer is
   * allocated for subsequent writes. Multiple buffers may be in flight simultaneously, up to
   * {@code maxPendingBuffers}. When that limit is reached, this method blocks until a permit
   * becomes available (backpressure). Since the executor is single-threaded, in-flight buffers
   * are uploaded sequentially in submission order.
   * <p>
   * If no {@link ExecutorService} was configured on the builder, this method falls back to a
   * synchronous {@link #flush()} and returns an already-completed future.
   * <p>
   * Must be called from the same thread as {@link #writeBatch} and {@link #flush}.
   *
   * @return a future that completes when the submitted async flush finishes
   */
  public Future<Void> flushAsync() {
    checkLastAsyncException();

    if (executorService == null || flushLock == null) {
      flush();
      return CompletableFuture.completedFuture(null);
    }

    flushLock.lock();
    try {
      if (cachedBatches.isEmpty()) {
        log.debug("Async flush skipped: empty buffer [stream={}]", streamId);
        return CompletableFuture.completedFuture(null);
      }

      // Backpressure: block if maxPendingBuffers already in flight
      try {
        flushPermits.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ClientException("Interrupted waiting for flush permit", e);
      }

      // Capture data for flushing
      final Schema schemaToFlush = this.schema;
      final long recordCountToFlush = this.recordCount;
      final long bytesToFlush = this.cachedSize;
      final List<byte[]> batchesToFlush = cachedBatches;

      // Allocate a fresh buffer for subsequent writes (no swap)
      cachedBatches = new ArrayList<>();
      this.cachedSize = 0;
      this.recordCount = 0;

      // Submit async upload; single-threaded executor guarantees FIFO ordering
      lastFlushFuture = executorService.submit(() -> {
        try {
          flushInternal(batchesToFlush, schemaToFlush, recordCountToFlush);
        } catch (MaxStorageException e) {
          log.error("Async flush failed [stream={}, rows={}, bytes={}]",
              streamId, recordCountToFlush, bytesToFlush, e);
          lastAsyncException = e;
          throw e;
        } catch (Exception oe) {
          log.error("Async flush failed [stream={}, rows={}, bytes={}]",
              streamId, recordCountToFlush, bytesToFlush, oe);
          lastAsyncException = new ClientException(oe);
          throw lastAsyncException;
        } finally {
          batchesToFlush.clear();
          flushPermits.release();
        }
        return null;
      });
      return lastFlushFuture;

    } finally {
      flushLock.unlock();
    }
  }

  /**
   * Internal method that performs the actual flush operation.
   */
  private void flushInternal(List<byte[]> batches, Schema flushSchema, long flushRecordCount)
    throws MaxStorageException {
    if (batches.isEmpty()) {
      return;
    }

    if (flushSchema == null) {
      throw new IllegalStateException(
        "Schema is not initialized. Cannot flush without writing any batch.");
    }

    // Create a special RequestBody that assembles data in Arrow IPC Stream format
    RequestBody arrowStreamBody = new RawArrowRequestBody(batches, flushSchema, ipcOption);

    // Always pass the row count for this flush payload (doFlush/flushAsync capture it before reset).
    HttpResponse response =
        storageStub.writeTable(tableId, sessionId, streamId, streamVersion, flushRecordCount,
            arrowStreamBody, routeToken, streamingTableId, streamingSchemaVersion,
            exactlyOnceMode ? rowOffset : -1, accessToken, writeMode);
    // Extract route token from response headers for next flush
    String newToken = response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER);
    if (newToken != null) {
      this.routeToken = newToken;
    }
    String rid = response.getRequestId();
    if (rid != null && !rid.isEmpty()) {
      this.lastRequestId = rid;
    }

    // Parse StagingId (and ExactlyOnceRowOffset when in EO mode) from the writeTable response.
    com.aliyun.odps.storage.internal.models.WriteStreamResponse writeResponse =
        storageStub.parseWriteStreamResponse(response);
    if (writeResponse != null) {
      if (writeResponse.getStagingId() != null) {
        this.lastStagingId = writeResponse.getStagingId();
      }
      if (exactlyOnceMode && writeResponse.getExactlyOnceRowOffset() != null) {
        this.rowOffset = writeResponse.getExactlyOnceRowOffset();
      }
    }
  }

  /**
   * Returns the request ID of the last successful write (flush). For client-side logging.
   */
  public String getLastRequestId() {
    return lastRequestId;
  }

  /**
   * Returns the StagingId returned by the most recent successful {@code TableWrite} flush.
   *
   * <p>The StagingId is the backend streaming session id that received the last flushed batch.
   * For streaming write it can be compared with {@link TableWriteSession#getMinUncommittedStagingId()}
   * to reason about async visibility progress.
   *
   * @return the last StagingId, or {@code null} if no flush has succeeded yet
   */
  public String getLastStagingId() {
    return lastStagingId;
  }

  /**
   * Returns the current row offset for Exactly-Once mode.
   *
   * <p>In Exactly-Once mode, this method calls the getWriteStream API to fetch
   * the latest row offset from the server and updates the access token.
   * In non-Exactly-Once mode, it returns the local row offset directly.
   *
   * @return the current row offset
   */
  public long getRowOffset() {
    if (exactlyOnceMode) {
      GetWriteStreamRequest request = GetWriteStreamRequest.newBuilder()
          .withTableIdentifier(tableId)
          .withSessionId(sessionId)
          .withStreamId(streamId)
          .withStreamVersion(streamVersion)
          .withExactlyOnceMode(true)
          .build();
      com.aliyun.odps.storage.internal.models.GetWriteStreamResponse response =
          storageStub.getWriteStream(request, routeToken, writeMode);
      if (response.getRowOffset() != null) {
        this.rowOffset = response.getRowOffset();
      }
      if (StringUtils.isNotBlank(response.getAccessToken())) {
        this.accessToken = response.getAccessToken();
      }
    }
    return rowOffset;
  }

  /**
   * Sets a new row offset for Exactly-Once mode.
   *
   * <p>This method first flushes any cached data with the current row offset,
   * then updates the row offset to the new value. This is useful when the client
   * needs to resume writing from a specific position (e.g., after recovering from
   * a failure with a known committed offset).
   *
   * <p>Note: This method is only meaningful in Exactly-Once mode. In non-Exactly-Once
   * mode, this method has no effect on the write behavior.
   *
   * @param newRowOffset the new row offset to set
   * @throws MaxStorageException if flush operation fails
   */
  public void setRowOffset(long newRowOffset) throws MaxStorageException {
    // Flush any cached data with the current row offset before changing it
    flush();
    waitForLastFlush();
    this.rowOffset = newRowOffset;
  }

  /**
   * Returns whether Exactly-Once mode is enabled for this writer.
   *
   * @return true if Exactly-Once mode is enabled
   */
  public boolean isExactlyOnceMode() {
    return exactlyOnceMode;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public RecordWriter getAsRecordWriter(long rowCountPerBatch) {
    boolean hasOperationColumn =
        tableSchema.getSystemColumns() != null
            && tableSchema.getSystemColumns().stream()
                .map(Column::getName)
                .anyMatch(Constants.OPERATION_COLUMN_NAME::equals);
    if (!hasOperationColumn && tableSchema.getColumns() != null) {
      hasOperationColumn =
          tableSchema.getColumns().stream()
              .map(Column::getName)
              .anyMatch(Constants.OPERATION_COLUMN_NAME::equals);
    }

    if (hasOperationColumn) {
      return new DeltaTableRecordWriter(this, rowCountPerBatch);
    } else {
      return new AppendTableRecordWriter(this, rowCountPerBatch);
    }
  }

  /**
   * Returns true if an async flush is still in flight (submitted but not yet completed).
   * Callers can use this to avoid blocking the writer thread on a timer-triggered flush.
   */
  public boolean hasPendingFlush() {
    Future<Void> f = lastFlushFuture;
    return f != null && !f.isDone();
  }

  public long getCachedSize() {
    return cachedSize;
  }

  /**
   * Validates and sets the operation column for tables with primary keys.
   * Checks that the operation column is not empty, contains only OPERATION_UPSERT or OPERATION_DELETE values,
   * and sets null values to OPERATION_UPSERT.
   *
   * @param root the VectorSchemaRoot to validate
   */
  private void validateAndSetOperationColumn(VectorSchemaRoot root) {
    FieldVector operationVector = root.getVector(Constants.OPERATION_COLUMN_NAME);
    if (operationVector == null) {
      throw new ClientException(
        "Operation column '" + Constants.OPERATION_COLUMN_NAME + "' is required when writing to a table with primary keys.");
    }

    if (!(operationVector instanceof org.apache.arrow.vector.TinyIntVector)) {
      throw new ClientException(
        "Operation column must be of type TinyIntVector for primary key tables.");
    }

    org.apache.arrow.vector.TinyIntVector tinyIntVector = (org.apache.arrow.vector.TinyIntVector) operationVector;

    for (int i = 0; i < root.getRowCount(); i++) {
      if (tinyIntVector.isNull(i)) {
        // Set to OPERATION_UPSERT if null
        tinyIntVector.setSafe(i, Constants.OPERATION_UPSERT);
      } else {
        // Validate the value is either OPERATION_UPSERT or OPERATION_DELETE
        byte operationValue = tinyIntVector.get(i);
        if (operationValue != Constants.OPERATION_UPSERT && operationValue != Constants.OPERATION_DELETE) {
          throw new ClientException(
            String.format("Invalid operation value '%d' at row %d. Must be '%d' (UPSERT) or '%d' (DELETE).",
              operationValue, i, Constants.OPERATION_UPSERT, Constants.OPERATION_DELETE));
        }
      }
    }
  }


  /**
   * ⅰ. recordBatch.Slice(i) 切出对应行，产生一个只有一行的record batch
   * ⅱ. 对这个只有一行的record batch进行选PK列。PK列会在stream的schema中提供，保持顺序一致，产生一个【仅包含PK列的】【只有一行的】record batch
   * ⅲ. 对这个record batch进行arrow ipc序列化，最后base64编码成一个string
   */
  protected String generateDistributionKeyString(VectorSchemaRoot originalRoot, int rowIndex,
                                                 List<Integer> pkColumnIndices) {
    if (pkColumnIndices.isEmpty()) {
      return null;
    }
    try (VectorSchemaRoot singleRowRoot = originalRoot.slice(rowIndex, 1)) {

      // 步骤 ⅱ: 对这个只有一行的 record batch 进行选PK列
      List<Field> pkFields = new ArrayList<>();
      List<FieldVector> pkVectors = new ArrayList<>();
      List<Field> originalFields = singleRowRoot.getSchema().getFields();
      List<FieldVector> originalVectors = singleRowRoot.getFieldVectors();

      for (int pkIndex : pkColumnIndices) {
        pkFields.add(originalFields.get(pkIndex));
        pkVectors.add(originalVectors.get(pkIndex));
      }

      try (VectorSchemaRoot pkOnlyRoot = new VectorSchemaRoot(pkFields, pkVectors, 1)) {
        // 步骤 ⅲ: 对这个 record batch 进行 arrow ipc 序列化，最后 base64 编码
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // TODO：如果开启字典编码，ArrowStreamWriter 需要一个 DictionaryProvider
        try (ArrowStreamWriter writer = new ArrowStreamWriter(pkOnlyRoot, null, out)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        } catch (IOException e) {
          throw new RuntimeException("Failed to write Arrow IPC stream.", e);
        }

        byte[] ipcBytes = out.toByteArray();

        return Base64.getEncoder().encodeToString(ipcBytes);
      }
    }
  }
}
