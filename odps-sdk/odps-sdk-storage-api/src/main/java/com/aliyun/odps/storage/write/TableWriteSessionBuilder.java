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
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateTableWriteSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateTableWriteSessionResponse;
import com.aliyun.odps.storage.internal.models.GetTableWriteSessionResponse;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.utils.StringUtils;

/**
 * <p>This builder allows configuration of various write session settings including:
 * <ul>
 *   <li>Partition specifications for writing data to specific partitions</li>
 *   <li>Overwrite mode for replacing existing data</li>
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
    this.createTableWriteSessionRequest.getFlags().put("overwrite", String.valueOf(overwrite));
    return this;
  }

  /**
   * Builds and returns a new TableWriteSession instance with the configured settings.
   *
   * <p>This method makes an API call to the MaxCompute service to create a write session
   * with the specified configuration. The session can then be used to write data to
   * the table in a distributed manner.
   *
   * @return A new TableWriteSession instance
   */
  public TableWriteSession build() {
    if (StringUtils.isNotBlank(sessionId)) {
      GetTableWriteSessionResponse getTableWriteSessionResponse =
        storageStub.getTableWriteSession(table, sessionId);
      return new TableWriteSession(storageStub, table, partitionSpec, allocator,
                                   sessionId);
    } else {
      CreateTableWriteSessionResponse createTableWriteSessionResponse =
        storageStub.createTableWriteSession(table, createTableWriteSessionRequest);
      this.sessionId = createTableWriteSessionResponse.getSessionId();
    }
    return new TableWriteSession(storageStub, table, partitionSpec, allocator,
                                 sessionId);
  }
}