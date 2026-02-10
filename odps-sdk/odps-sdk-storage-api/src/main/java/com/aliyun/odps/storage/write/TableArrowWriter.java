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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;
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
import com.aliyun.odps.storage.internal.models.BlobWriteResponse;
import com.aliyun.odps.storage.internal.models.CloseWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.WriteSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.table.arrow.compression.OdpsZstdCompressionCodec;
import com.aliyun.odps.table.arrow.writers.ArrowCompressVectorUnloader;
import com.aliyun.odps.table.utils.SchemaUtils;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;

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
  private List<byte[]> secondaryBatches;
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
    if (this.executorService != null) {
      this.secondaryBatches = new ArrayList<>();
      this.flushLock = new ReentrantLock();
    } else {
      this.flushLock = null;
    }
    this.pendingFlushFuture = null;
    this.lastAsyncException = null;
    this.bytesWritten = 0;
    this.cachedSize = 0;
    this.recordCount = 0;
  }

  @Override
  public void writeBatch(VectorSchemaRoot root) {
    if (root == null || root.getRowCount() == 0) {
      return;
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

    // Flush any remaining data in the current buffer
    if (!cachedBatches.isEmpty()) {
      flushInternal(cachedBatches, schema, recordCount);
      clearCache();
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
    BlobWriteResponse
      blobWriteResponse =
      storageStub.tableWriteBlob(tableId, sessionId, streamId, streamVersion, staticPartitionSpec,
                                 columnId, data);
    return Blob.fromReference(blobWriteResponse.getBlobReference());
  }

  @Override
  public void close() {
    // Flush any remaining data
    flush();
    waitForPendingFlush();

    // Close the write stream
    CloseWriteStreamRequest closeWriteStreamRequest =
      CloseWriteStreamRequest.newBuilder().
      withSessionId(sessionId).
      withStreamId(streamId).
      withStreamVersion(streamVersion).
      build();
    storageStub.closeWriteStream(tableId, closeWriteStreamRequest);
  }

  public WriteSchema getWriteSchema() {
    return tableSchema;
  }

  private void clearCache() {
    this.cachedBatches.clear();
    this.cachedSize = 0;
    this.recordCount = 0;
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
      cachedBatches = secondaryBatches;
      secondaryBatches = batchesToFlush;

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
    RequestBody arrowStreamBody = new RequestBody() {
      @Override
      public MediaType contentType() {
        return MediaType.parse("application/vnd.apache.arrow.stream");
      }

      @Override
      public void writeTo(@NotNull BufferedSink sink) throws IOException {
        try (WriteChannel channel = new WriteChannel(Channels.newChannel(sink.outputStream()))) {
          MessageSerializer.serialize(channel, flushSchema, ipcOption);
          for (byte[] batchBytes : batches) {
            sink.write(batchBytes);
          }
          if (!ipcOption.write_legacy_ipc_format) {
            channel.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
          }
          channel.writeIntLittleEndian(0);
        }
      }
    };

    storageStub.writeTable(tableId, sessionId, streamId, streamVersion, flushRecordCount,
                           arrowStreamBody);
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
}
