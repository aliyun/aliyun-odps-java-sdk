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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.internal.io.StreamingUpload;
import com.aliyun.odps.table.arrow.ArrowWriterFactory;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.Column;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.BatchCompatibleSessionResponse;
import com.aliyun.odps.storage.internal.models.BatchCompatibleWriteResponse;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.table.utils.SchemaUtils;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Arrow writer for one block in {@link WriteMode#BATCH_COMPATIBLE}.
 *
 * <p>A block with rows streams a single Arrow IPC request with bounded buffering.
 * An empty block completes locally.
 * {@link #flush()} sends pending bytes without committing the block. Failed uploads cannot
 * be replayed; create a new writer with a new attempt number. {@link #abort()} cancels an
 * in-flight request; the session is not committed and the quota reservation expires.
 */
public final class TableBlockWriter implements ArrowWriter {

  private final StorageStub storageStub;
  private final TableIdentifier tableId;
  private final String sessionId;
  private final int blockNumber;
  private final int attemptNumber;
  private final String routeToken;
  private final Schema arrowSchema;
  private final BufferAllocator allocator;
  private final boolean enhanceWriteCheck;
  private final int chunkSize;
  private final int uploadBufferSize;
  private final CompressionCodec compressionCodec;
  private volatile StreamingUpload upload;
  private OutputStream output;
  private ArrowWriter writer;
  private IOException failure;

  private volatile boolean closed;
  private long recordCount;
  private BlockWriteResult result;

  TableBlockWriter(StorageStub storageStub, TableIdentifier tableId, String sessionId,
      int blockNumber, int attemptNumber, String routeToken,
      BatchCompatibleSessionResponse.DataSchema dataSchema, boolean enhanceWriteCheck,
      BufferAllocator allocator, ArrowOptions arrowOptions, int chunkSize, int uploadBufferSize,
      CompressionCodec compressionCodec) {
    this.compressionCodec = java.util.Objects.requireNonNull(compressionCodec, "compressionCodec");
    this.storageStub = storageStub;
    this.tableId = tableId;
    this.sessionId = sessionId;
    this.blockNumber = blockNumber;
    this.attemptNumber = attemptNumber;
    this.routeToken = routeToken;
    this.arrowSchema = toArrowSchema(dataSchema, arrowOptions);
    this.enhanceWriteCheck = enhanceWriteCheck;
    this.allocator = allocator;
    this.chunkSize = chunkSize;
    this.uploadBufferSize = uploadBufferSize;
  }

  public VectorSchemaRoot createVectorSchemaRoot() {
    return VectorSchemaRoot.create(arrowSchema, allocator);
  }

  public Schema getSchema() {
    return arrowSchema;
  }

  @Override
  public void writeBatch(VectorSchemaRoot root) throws IOException {
    ensureOpen();
    if (root == null || root.getRowCount() == 0) {
      return;
    }
    if (!arrowSchema.equals(root.getSchema())) {
      throw new IOException("Arrow schema does not match the batch-compatible session schema");
    }

    try {
      ensureStarted();
      writer.writeBatch(root);
      recordCount += root.getRowCount();
    } catch (IOException | RuntimeException e) {
      throw fail(e);
    } catch (Error e) {
      abort();
      throw e;
    }
  }

  private void ensureStarted() throws IOException {
    if (writer == null) {
      CreateWriteStreamRequest request = new CreateWriteStreamRequest();
      request.setStreamId("block-" + blockNumber + "-attempt-" + attemptNumber);
      request.setStreamVersion(1);
      CreateWriteStreamResponse reservation = storageStub.createTableWriteStream(
          tableId, sessionId, request, routeToken, WriteMode.BATCH_COMPATIBLE);
      if (reservation == null || reservation.getQuotaToken() == null
          || reservation.getQuotaToken().isEmpty()) {
        throw new ClientException("Batch-compatible quota reservation returned no quota token");
      }
      if (reservation.getRouteToken() == null || reservation.getRouteToken().isEmpty()) {
        throw new ClientException("Batch-compatible quota reservation returned no route token");
      }
      ensureOpen();
      upload = storageStub.openBatchCompatibleBlock(tableId, sessionId, blockNumber,
          attemptNumber, reservation.getRouteToken(), reservation.getQuotaToken(), chunkSize);
      if (closed) {
        upload.close();
        throw new IOException("Block writer was aborted");
      }
      upload.configureBufferSize(uploadBufferSize);
      output = upload.outputStream();
      writer = ArrowWriterFactory.newBuilder(output).withCompression(compressionCodec).build();
    }
  }

  /** Waits until all bytes written so far have been flushed to the network. */
  public void flush() throws IOException {
    ensureOpen();
    try {
      if (output != null) {
        output.flush();
      }
    } catch (IOException | RuntimeException e) {
      throw fail(e);
    } catch (Error e) {
      abort();
      throw e;
    }
  }

  /** Returns a session commit result, including a local result for an empty block. */
  public BlockWriteResult commit() throws IOException {
    close();
    if (result == null) {
      throw new IOException("Block writer was aborted and cannot be committed");
    }
    return result;
  }

  /** Cancels the upload and unblocks concurrent write/flush/close calls. */
  public void abort() {
    closed = true;
    StreamingUpload active = upload;
    if (active != null && result == null) {
      active.close();
    }
  }

  @Override
  public void close() throws IOException {
    if (failure != null) {
      throw failure;
    }
    if (closed) {
      return;
    }
    try {
      if (recordCount == 0) {
        result = new BlockWriteResult(sessionId, blockNumber, attemptNumber, 0, null);
        return;
      }
      writer.close();
      BatchCompatibleWriteResponse response = storageStub.getGson().fromJson(
          upload.finish().getBody(), BatchCompatibleWriteResponse.class);
      validateResponse(response);
      result = new BlockWriteResult(sessionId, blockNumber, attemptNumber,
          response.getRecordCount(), response.getCommitMessage());
    } catch (IOException | RuntimeException e) {
      throw fail(e);
    } catch (Error e) {
      abort();
      throw e;
    } finally {
      closed = true;
    }
  }

  private IOException fail(Exception error) {
    if (failure == null) {
      failure = error instanceof IOException ? (IOException) error
          : new IOException("Failed to write block " + blockNumber + " attempt " + attemptNumber
              + ": " + error, error);
    }
    abort();
    return failure;
  }

  @Override
  public long bytesWritten() {
    return writer == null ? 0 : writer.bytesWritten();
  }

  public int getBlockNumber() {
    return blockNumber;
  }

  public int getAttemptNumber() {
    return attemptNumber;
  }

  public long getRecordCount() {
    return recordCount;
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("Block writer is already closed");
    }
  }

  private void validateResponse(BatchCompatibleWriteResponse response) throws IOException {
    if (response == null) {
      throw new IOException("Batch-compatible write returned an empty response");
    }
    if (response.getRecordCount() != recordCount) {
      throw new IOException(
          "Unexpected record count, expected " + recordCount + " but got "
              + response.getRecordCount());
    }
    if (response.getCommitMessage() == null) {
      throw new IOException("Batch-compatible write did not return a commit message");
    }
    if (!enhanceWriteCheck) {
      return;
    }

    try {
      JsonObject message = JsonParser.parseString(response.getCommitMessage()).getAsJsonObject();
      if (message.has("BlockNumber")
          && message.get("BlockNumber").getAsInt() != blockNumber) {
        throw new IOException("Commit message block number does not match the writer");
      }
      if (message.has("AttemptNumber")
          && message.get("AttemptNumber").getAsInt() != attemptNumber) {
        throw new IOException("Commit message attempt number does not match the writer");
      }
      if (message.has("WriterStats")) {
        JsonObject stats = message.getAsJsonObject("WriterStats");
        if (stats.has("RecordNum") && stats.get("RecordNum").getAsLong() != recordCount) {
          throw new IOException("Commit message record count does not match the writer");
        }
      }
    } catch (IOException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new IOException("Cannot validate the batch-compatible commit message", e);
    }
  }

  private static Schema toArrowSchema(BatchCompatibleSessionResponse.DataSchema dataSchema,
      ArrowOptions arrowOptions) {
    List<Column> columns = new ArrayList<>();
    if (dataSchema != null) {
      addColumns(columns, dataSchema.getDataColumns());
      addColumns(columns, dataSchema.getPartitionColumns());
    }
    return SchemaUtils.toArrowSchema(columns, arrowOptions);
  }

  private static void addColumns(
      List<Column> target,
      List<BatchCompatibleSessionResponse.Column> source) {
    List<BatchCompatibleSessionResponse.Column> columns = source == null
        ? Collections.<BatchCompatibleSessionResponse.Column>emptyList()
        : source;
    for (BatchCompatibleSessionResponse.Column column : columns) {
      TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(column.getType());
      Column converted = new Column(column.getName(), typeInfo, column.getComment());
      converted.setNullable(column.isNullable());
      target.add(converted);
    }
  }
}
