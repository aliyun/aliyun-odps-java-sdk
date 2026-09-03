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
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import com.aliyun.odps.Column;
import com.aliyun.odps.storage.MaxStorageException;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.io.RawArrowRequestBody;
import com.aliyun.odps.storage.internal.models.BatchCompatibleSessionResponse;
import com.aliyun.odps.storage.internal.models.BatchCompatibleWriteResponse;
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
 * <p>The block protocol accepts one Arrow IPC stream per block/attempt pair. Batches are
 * therefore buffered until {@link #commit()} or {@link #close()} finalizes the writer.
 * Prefer {@link #commit()} when the returned {@link BlockWriteResult} will be used directly.
 * Calling {@link #abort()} discards buffered batches without creating a remote writer result;
 * the service releases the unused quota reservation after it expires.
 */
public final class TableBlockWriter implements ArrowWriter {

  private final StorageStub storageStub;
  private final TableIdentifier tableId;
  private final String sessionId;
  private final int blockNumber;
  private final int attemptNumber;
  private final String routeToken;
  private final String quotaToken;
  private final Schema arrowSchema;
  private final BufferAllocator allocator;
  private final boolean enhanceWriteCheck;
  private final IpcOption ipcOption = new IpcOption();
  private final List<byte[]> cachedBatches = new ArrayList<>();

  private boolean closed;
  private long cachedSize;
  private long recordCount;
  private long bytesWritten;
  private BlockWriteResult result;

  TableBlockWriter(StorageStub storageStub,
                   TableIdentifier tableId,
                   String sessionId,
                   int blockNumber,
                   int attemptNumber,
                   String routeToken,
                   String quotaToken,
                   BatchCompatibleSessionResponse.DataSchema dataSchema,
                   boolean enhanceWriteCheck,
                   BufferAllocator allocator) {
    this.storageStub = storageStub;
    this.tableId = tableId;
    this.sessionId = sessionId;
    this.blockNumber = blockNumber;
    this.attemptNumber = attemptNumber;
    this.routeToken = routeToken;
    this.quotaToken = quotaToken;
    this.arrowSchema = toArrowSchema(dataSchema);
    this.enhanceWriteCheck = enhanceWriteCheck;
    this.allocator = allocator;
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

    try (ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch();
         ByteArrayOutputStream output = new ByteArrayOutputStream();
         WriteChannel channel = new WriteChannel(Channels.newChannel(output))) {
      MessageSerializer.serialize(channel, batch, ipcOption);
      byte[] bytes = output.toByteArray();
      cachedBatches.add(bytes);
      cachedSize += bytes.length;
      recordCount += root.getRowCount();
    }
  }

  /** Finalizes the block upload and returns the result required by the session commit. */
  public BlockWriteResult commit() throws IOException {
    close();
    if (result == null) {
      throw new IOException("Block writer was aborted and cannot be committed");
    }
    return result;
  }

  /** Discards locally buffered data without sending it to the service. */
  public void abort() {
    if (!closed) {
      cachedBatches.clear();
      cachedSize = 0;
      closed = true;
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      RawArrowRequestBody body = new RawArrowRequestBody(cachedBatches, arrowSchema, ipcOption);
      BatchCompatibleWriteResponse response = storageStub.writeBatchCompatibleBlock(
          tableId, sessionId, blockNumber, attemptNumber, body, routeToken, quotaToken);
      validateResponse(response);
      bytesWritten += cachedSize;
      result = new BlockWriteResult(
          sessionId,
          blockNumber,
          attemptNumber,
          response.getRecordCount(),
          response.getCommitMessage());
    } catch (MaxStorageException e) {
      throw new IOException(
          "Failed to write block " + blockNumber + " attempt " + attemptNumber, e);
    } finally {
      cachedBatches.clear();
      cachedSize = 0;
      closed = true;
    }
  }

  @Override
  public long bytesWritten() {
    return bytesWritten + cachedSize;
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

  private static Schema toArrowSchema(BatchCompatibleSessionResponse.DataSchema dataSchema) {
    List<Column> columns = new ArrayList<>();
    if (dataSchema != null) {
      addColumns(columns, dataSchema.getDataColumns());
      addColumns(columns, dataSchema.getPartitionColumns());
    }
    return SchemaUtils.toArrowSchema(columns);
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
