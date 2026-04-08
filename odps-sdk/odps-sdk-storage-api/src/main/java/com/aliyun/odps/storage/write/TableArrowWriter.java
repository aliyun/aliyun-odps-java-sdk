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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
import com.aliyun.odps.storage.internal.Constants;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.io.RawArrowRequestBody;
import com.aliyun.odps.storage.internal.models.BlobWriteItem;
import com.aliyun.odps.storage.internal.models.BlobWriteResponse;
import com.aliyun.odps.storage.internal.models.CloseWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.internal.models.WriteSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.table.arrow.compression.OdpsZstdCompressionCodec;
import com.aliyun.odps.table.arrow.writers.ArrowCompressVectorUnloader;
import com.aliyun.odps.table.utils.SchemaUtils;

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
 * Note: This class is not thread-safe for concurrent write operations.
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
  private List<byte[]> flushingBatches;
  private volatile Schema schema;
  private final long bufferSize;
  private long cachedSize;
  private long bytesWritten;
  private long recordCount;
  protected final BufferAllocator allocator;
  private final boolean autoFlushEnabled;
  private final ExecutorService executorService;
  private final ReentrantLock flushLock;
  private Future<Void> pendingFlushFuture;
  private volatile MaxStorageException lastAsyncException;

  protected final List<Integer> primaryKeyColumnIndices;

  protected final WriteMode writeMode;
  protected final String streamingTableId;
  protected final Long streamingSchemaVersion;

  private String routeToken;

  /** Last request ID from writeTable response, for client logging. */
  private volatile String lastRequestId;

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
    this.flushingBatches = new ArrayList<>();
    if (this.executorService != null) {
      this.flushLock = new ReentrantLock();
    } else {
      this.flushLock = null;
    }
    this.pendingFlushFuture = null;
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
  }

  @Override
  public void writeBatch(VectorSchemaRoot root) {
    if (root == null || root.getRowCount() == 0) {
      return;
    }

    if (!primaryKeyColumnIndices.isEmpty()) {
      validateAndSetOperationColumn(root);
    }
    // Check for any pending async exceptions
    checkLastAsyncException();

    // If there's a pending async flush that might have failed, check its status
    if (executorService != null && pendingFlushFuture != null && pendingFlushFuture.isDone()) {
      try {
        pendingFlushFuture.get();
        // Only clear exception after successful completion
        lastAsyncException = null;
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        lastAsyncException =
          (cause instanceof MaxStorageException) ? (MaxStorageException) cause
                                                 : new ClientException(cause.getMessage(), cause);
        checkLastAsyncException();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ClientException("Async flush interrupted", e);
      }
    }

    writeInternal(root);
    if (this.autoFlushEnabled && this.cachedSize >= this.bufferSize) {
      if (executorService != null) {
        asyncFlush();
      } else {
        flush();
      }
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
    // If there's a pending async flush, wait for it to complete first
    if (executorService != null && pendingFlushFuture != null) {
      try {
        pendingFlushFuture.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof MaxStorageException) {
          throw (MaxStorageException) cause;
        } else {
          throw new ClientException("Failed to complete pending flush", cause);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ClientException("Flush interrupted", e);
      }
    }

    // swap to avoid concurrent modify
    List<byte[]> swap = cachedBatches;
    cachedBatches = flushingBatches;
    flushingBatches = swap;

    this.cachedSize = 0;
    this.recordCount = 0;

    // Flush any remaining data in the current buffer
    if (!flushingBatches.isEmpty()) {
      flushInternal(flushingBatches, schema, recordCount);
      this.flushingBatches.clear();
    }
  }

  /**
   * 检查并抛出异步链路发生的异常
   */
  private void checkLastAsyncException() throws MaxStorageException {
    if (lastAsyncException != null) {
      MaxStorageException e = lastAsyncException;
      lastAsyncException = null; // 消费后清除
      throw e;
    }
  }

  /**
   * 等待异步 Flush 任务完成，并提取其中的异常
   */
  private void waitForPendingFlush() throws MaxStorageException {
    if (pendingFlushFuture != null) {
      try {
        pendingFlushFuture.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof MaxStorageException) {
          throw (MaxStorageException) cause;
        }
        throw new ClientException("Async flush execution failed", cause);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ClientException("Interrupted while waiting for async flush", e);
      } finally {
        pendingFlushFuture = null;
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
   * @return a list of {@link Blob} references in the same order as the input list
   * @throws ClientException if called on a Delta Table or if the server response is inconsistent
   */
  public List<Blob> batchUploadBlob(long columnId, List<byte[]> dataList) {
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
    // Flush any remaining data
    flush();
    waitForPendingFlush();

    // In streaming mode, closeWriteStream is not supported
    if (writeMode == WriteMode.STREAMING || Constants.AUTO_COMMIT_DEFAULT_STREAM_ID.equals(streamId)) {
      return;
    }

    // Close the write stream
    CloseWriteStreamRequest closeWriteStreamRequest =
      CloseWriteStreamRequest.newBuilder().
      withSessionId(sessionId).
      withStreamId(streamId).
      withStreamVersion(streamVersion).
      build();
    storageStub.closeWriteStream(tableId, closeWriteStreamRequest, routeToken);
  }

  public WriteSchema getWriteSchema() {
    return tableSchema;
  }
  /**
   * Asynchronously flushes the current buffer when double buffering is enabled.
   * This method swaps buffers and schedules the flush operation on a background thread,
   * allowing write operations to continue immediately.
   */
  private void asyncFlush() {
    if (executorService == null || flushLock == null) {
      return;
    }

    flushLock.lock();
    try {
      // 1. Wait for previous async flush to complete
      waitForPendingFlush();

      if (cachedBatches.isEmpty()) {
        return;
      }

      // 2. Prepare data for flushing
      final Schema schemaToFlush = this.schema;
      final long recordCountToFlush = this.recordCount;

      // 3. Swap buffers: cachedBatches <-> secondaryBatches
      // After swap: cachedBatches gets the empty list (cleared by previous task's finally),
      // secondaryBatches gets the current data to flush
      List<byte[]> batchesToFlush = cachedBatches;
      cachedBatches = flushingBatches;
      flushingBatches = batchesToFlush;

      // Reset counters for the new active buffer
      this.cachedSize = 0;
      this.recordCount = 0;

      // 4. Schedule async flush
      pendingFlushFuture = executorService.submit(() -> {
        try {
          flushInternal(batchesToFlush, schemaToFlush, recordCountToFlush);
        } catch (MaxStorageException e) {
          log.error("Async flush failed", e);
          lastAsyncException = e;
          throw e;
        } catch (Exception oe) {
          log.error("Async flush failed", oe);
          lastAsyncException = new ClientException(oe);
          throw lastAsyncException;
        } finally {
          // Critical: Clear the list to release memory immediately after flush
          batchesToFlush.clear();
        }
        return null;
      });

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

    HttpResponse response = storageStub.writeTable(tableId, sessionId, streamId, streamVersion, flushRecordCount,
                             arrowStreamBody, routeToken, streamingTableId, streamingSchemaVersion);
    // Extract route token from response headers for next flush
    String newToken = response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER);
    if (newToken != null) {
      this.routeToken = newToken;
    }
    String rid = response.getRequestId();
    if (rid != null && !rid.isEmpty()) {
      this.lastRequestId = rid;
    }
  }

  /**
   * Returns the request ID of the last successful write (flush). For client-side logging.
   */
  public String getLastRequestId() {
    return lastRequestId;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public RecordWriter getAsRecordWriter(long rowCountPerBatch) {
    boolean hasOperationColumn = tableSchema.getSystemColumns().stream()
      .map(Column::getName)
      .anyMatch(Constants.OPERATION_COLUMN_NAME::equals);

    if (hasOperationColumn) {
      return new DeltaTableRecordWriter(this, rowCountPerBatch);
    } else {
      return new AppendTableRecordWriter(this, rowCountPerBatch);
    }
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
