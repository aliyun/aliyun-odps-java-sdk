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

import org.apache.arrow.memory.BufferAllocator;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.internal.Constants;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.BatchCompatibleCreateSessionRequest;
import com.aliyun.odps.storage.internal.models.BatchCompatibleSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateTableWriteSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateTableWriteSessionResponse;
import com.aliyun.odps.storage.internal.models.GetTableWriteSessionResponse;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.utils.StringUtils;

/**
 * <p>This builder allows configuration of various write session settings including:
 * <ul>
 *   <li>Partition specifications for writing data to specific partitions</li>
 *   <li>Overwrite mode for replacing existing data</li>
 *   <li>Write mode (Batch, BatchCompatible, or Streaming)</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
 * TableWriteSessionBuilder builder = client.createWriteSessionBuilder(tableId)
 *     .withPartition(new PartitionSpec("pt='20250101'"))
 *     .withOverwrite(true);
 * TableWriteSession session = builder.build();
 * }</pre>
 *
 * <p>Streaming mode example:
 * <pre>{@code
 * TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
 * TableWriteSessionBuilder builder = client.createWriteSessionBuilder(tableId)
 *     .withWriteMode(WriteMode.STREAMING);
 * TableWriteSession session = builder.build();
 * }</pre>
 *
 * <p>Batch-compatible mode example:
 * <pre>{@code
 * TableWriteSession session = client.createWriteSessionBuilder(tableId)
 *     .withWriteMode(WriteMode.BATCH_COMPATIBLE)
 *     .withBatchCompatibleOptions(BatchCompatibleOptions.newBuilder()
 *         .withEnhanceWriteCheck(true)
 *         .build())
 *     .build();
 * }</pre>
 */
public class TableWriteSessionBuilder {

  private final CreateTableWriteSessionRequest
    createTableWriteSessionRequest =
    new CreateTableWriteSessionRequest();
  private final StorageStub storageStub;

  private final TableIdentifier table;
  private final BufferAllocator allocator;
  private PartitionSpec partitionSpec;
  private String sessionId;
  private WriteMode writeMode = WriteMode.BATCH;
  private boolean overwrite;
  private BatchCompatibleOptions batchCompatibleOptions = BatchCompatibleOptions.createDefault();
  private boolean batchCompatibleOptionsConfigured;

  /**
   * Constructs a new TableWriteSessionBuilder with the provided parameters.
   *
   * @param storageStub The storage stub for communicating with the MaxCompute service
   * @param allocator   The buffer allocator for Arrow memory management
   * @param table       The identifier of the table to write to
   * @throws IllegalArgumentException if table is null
   */
  public TableWriteSessionBuilder(StorageStub storageStub,
                                  BufferAllocator allocator,
                                  TableIdentifier table) {
    Preconditions.checkNotNull(table, "Table identifier cannot be null");
    this.table = table;
    this.allocator = allocator;
    this.storageStub = storageStub;
  }

  /**
   * Set the sessionId to reload write session.
   *
   * @param sessionId The session id of write session already created.
   * @return This builder instance for method chaining
   */
  public TableWriteSessionBuilder withSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  /**
   * Sets the partition specification for writing data to a specific partition.
   *
   * <p>This allows writing data to a specific partition value, which is useful
   * for partitioned tables.
   *
   * @param partitionSpec The partition specification to write to
   * @return This builder instance for method chaining
   */
  public TableWriteSessionBuilder withPartition(
    PartitionSpec partitionSpec) {
    this.partitionSpec = partitionSpec;
    this.createTableWriteSessionRequest.setPartialPartitionSpec(partitionSpec);
    return this;
  }

  /**
   * Sets whether to overwrite existing data when writing to the table.
   *
   * <p>When overwrite is enabled, existing data in the table or specified partitions
   * will be replaced with the new data being written.
   *
   * @param overwrite Whether to overwrite existing data
   * @return This builder instance for method chaining
   */
  public TableWriteSessionBuilder withOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
    this.createTableWriteSessionRequest.getFlags().put("overwrite", String.valueOf(overwrite));
    return this;
  }

  /**
   * Sets advanced options used only by {@link WriteMode#BATCH_COMPATIBLE}.
   *
   * @param options batch-compatible block protocol settings
   * @return this builder
   */
  public TableWriteSessionBuilder withBatchCompatibleOptions(BatchCompatibleOptions options) {
    this.batchCompatibleOptions = Preconditions.checkNotNull(
        options, "Batch-compatible options cannot be null");
    this.batchCompatibleOptionsConfigured = true;
    return this;
  }

  /**
   * Sets the write mode for the session.
   *
   * <p>In BATCH mode (default), data becomes visible only after the session is committed.
   * BATCH_COMPATIBLE uses block and attempt numbers and commits typed block results.
   * In STREAMING mode, data becomes visible immediately after flush, without requiring
   * explicit commit. Streaming mode uses a default session ID and does not require
   * explicit session creation.
   *
   * @param writeMode The write mode to use
   * @return This builder instance for method chaining
   */
  public TableWriteSessionBuilder withWriteMode(WriteMode writeMode) {
    this.writeMode = writeMode != null ? writeMode : WriteMode.BATCH;
    return this;
  }

  /**
   * Builds and returns a new TableWriteSession instance with the configured settings.
   *
   * <p>For BATCH and BATCH_COMPATIBLE modes, this method makes an API call to the MaxCompute
   * service to create a write session with the specified configuration.
   *
   * <p>For STREAMING mode, no session creation API call is made. The session uses a
   * default session ID ("default") and data becomes visible immediately after flush.
   *
   * @return A new TableWriteSession instance
   */
  public TableWriteSession build() {
    if (batchCompatibleOptionsConfigured && writeMode != WriteMode.BATCH_COMPATIBLE) {
      throw new IllegalStateException(
          "BatchCompatibleOptions require WriteMode.BATCH_COMPATIBLE");
    }

    if (writeMode.isStreaming()) {
      // Streaming mode: use default session ID without creating session
      return new TableWriteSession(storageStub, table, partitionSpec, allocator,
                                   Constants.AUTO_COMMIT_SESSION_ID, writeMode, null);
    }

    if (writeMode == WriteMode.BATCH_COMPATIBLE) {
      return buildBatchCompatibleSession();
    }

    if (StringUtils.isNotBlank(sessionId)) {
      String routeToken = null;
      if (!Constants.AUTO_COMMIT_SESSION_ID.equals(sessionId)) {
        GetTableWriteSessionResponse resp =
            storageStub.getTableWriteSession(table, sessionId, null, writeMode);
        routeToken = resp.getRouteToken();
      }
      return new TableWriteSession(storageStub, table, partitionSpec, allocator,
                                   sessionId, writeMode, routeToken);
    } else {
      CreateTableWriteSessionResponse createTableWriteSessionResponse =
        storageStub.createTableWriteSession(table, createTableWriteSessionRequest, writeMode);
      this.sessionId = createTableWriteSessionResponse.getSessionId();
      return new TableWriteSession(storageStub, table, partitionSpec, allocator,
              sessionId, writeMode,
              createTableWriteSessionResponse.getRouteToken());
    }
  }

  private TableWriteSession buildBatchCompatibleSession() {
    BatchCompatibleSessionResponse response;
    if (StringUtils.isNotBlank(sessionId)) {
      response = storageStub.getBatchCompatibleSession(table, sessionId, null);
    } else {
      BatchCompatibleCreateSessionRequest request =
          new BatchCompatibleCreateSessionRequest();
      request.setPartitionSpec(
          partitionSpec == null ? "" : partitionSpec.toString(false, true));
      request.setOverwrite(overwrite);
      request.setDynamicPartitionOptions(
          new BatchCompatibleCreateSessionRequest.DynamicPartitionOptions(
              batchCompatibleOptions.getDynamicPartitionLimit()));
      request.setArrowOptions(ArrowOptions.createDefault());
      request.setMaxFieldSize(batchCompatibleOptions.getMaxFieldSize());
      request.setEnhanceWriteCheck(batchCompatibleOptions.isEnhanceWriteCheck());
      response = storageStub.createBatchCompatibleSession(table, request);
      if (response == null || StringUtils.isBlank(response.getSessionId())) {
        throw new ClientException(
            "Create batch-compatible session returned no session identifier");
      }
      sessionId = response.getSessionId();
    }

    if (response == null) {
      throw new ClientException("Get batch-compatible session returned an empty response");
    }
    if (response.getDataSchema() == null) {
      throw new ClientException(
          "Batch-compatible session response did not contain DataSchema");
    }

    return new TableWriteSession(
        storageStub,
        table,
        partitionSpec,
        allocator,
        sessionId,
        writeMode,
        response.getRouteToken(),
        response.getMaxBlockNumber(),
        response.getDataSchema(),
        response.isEnhanceWriteCheck());
  }
}
