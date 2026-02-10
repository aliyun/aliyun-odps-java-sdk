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

package com.aliyun.odps.storage.read;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.storage.models.SessionStatus;
import com.aliyun.odps.storage.settings.IncrementalReadOptions;
import com.aliyun.odps.storage.settings.SplitOptions;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.utils.StringUtils;

/**
 * Implementation of {@link TableReadSessionBuilder} for the MaxCompute Storage API.
 *
 * <p>This builder allows configuration of various read session settings including:
 * <ul>
 *   <li>Data and partition column selection</li>
 *   <li>Partition and bucket filtering</li>
 *   <li>Split options for parallel processing</li>
 *   <li>Arrow-specific options</li>
 *   <li>Filter predicates for data filtering</li>
 *   <li>Incremental read options</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
 * TableReadSessionBuilder builder = client.createReadSessionBuilder(tableId)
 *     .withRequiredDataColumns(Arrays.asList("col1", "col2"))
 *     .withRequiredPartitions(Arrays.asList("pt='20250101'"))
 *     .withSplitOptions(SplitOptions.newBuilder().build());
 * TableReadSession session = builder.build();
 * }</pre>
 */
public class TableReadSessionBuilder {

  private StorageStub storageStub;

  private final CreateTableReadSessionRequest
    tableReadSessionRequest =
    new CreateTableReadSessionRequest();

  private final TableIdentifier table;
  private final BufferAllocator allocator;
  private SplitOptions splitOptions;
  private String sessionId;
  private long sessionReadyTimeoutSeconds = 3600;

  /**
   * Constructs a new TableReadSessionBuilder with the provided parameters.
   *
   * @param storageStub The storage stub for communicating with the MaxCompute service
   * @param allocator   The buffer allocator for Arrow memory management
   * @param table       The identifier of the table to read from
   * @throws IllegalArgumentException if table is null
   */
  public TableReadSessionBuilder(StorageStub storageStub,
                                 BufferAllocator allocator,
                                 TableIdentifier table) {
    Preconditions.checkNotNull(table, "Table identifier cannot be null");
    this.table = table;
    this.allocator = allocator;
    this.storageStub = storageStub;
  }

  public TableReadSessionBuilder withSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  /**
   * Sets the required data columns to read from the table.
   *
   * <p>By specifying only the columns needed, network traffic and processing
   * overhead can be reduced.
   *
   * @param requiredDataColumns The list of data column names to read
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withColumns(
    List<String> requiredDataColumns) {
    this.tableReadSessionRequest.setRequiredDataColumns(requiredDataColumns);
    return this;
  }

  /**
   * Sets the required partition columns to read from the table.
   *
   * @param requiredPartitionColumns The list of partition column names to read
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withPartitionColumns(
    List<String> requiredPartitionColumns) {
    this.tableReadSessionRequest.setRequiredPartitionColumns(requiredPartitionColumns);
    return this;
  }

  /**
   * Sets the required partitions to read from the table.
   *
   * <p>This allows filtering data by specific partition values, reducing
   * the amount of data that needs to be processed.
   *
   * @param requiredPartitions The list of partition specifications to read (e.g., "pt='20250101'")
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withPartitions(
    List<PartitionSpec> requiredPartitions) {
    List<String> partitions = requiredPartitions.stream()
      .map(p -> p.toString(false, true))
      .collect(Collectors.toList());
    this.tableReadSessionRequest.setRequiredPartitions(partitions);
    return this;
  }

  /**
   * Sets the required bucket IDs to read from the table.
   *
   * <p>This allows filtering data by specific bucket IDs, which is useful
   * for clustered tables.
   *
   * @param requiredBucketIds The list of bucket IDs to read
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withBucketIds(
    List<Integer> requiredBucketIds) {
    this.tableReadSessionRequest.setRequiredBucketIds(requiredBucketIds);
    return this;
  }

  /**
   * Sets the split options for parallel processing.
   *
   * <p>These options control how the data is split into chunks for parallel
   * processing, including split size and other parameters.
   *
   * @param splitOptions The split options configuration
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withSplitOptions(
    SplitOptions splitOptions) {
    this.splitOptions = splitOptions;
    this.tableReadSessionRequest.setSplitOptions(splitOptions);
    return this;
  }

  /**
   * Sets the Arrow options for data processing.
   *
   * <p>These options control Arrow-specific behavior such as memory allocation
   * and serialization settings.
   *
   * @param arrowOptions The Arrow options configuration
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withArrowOptions(
    ArrowOptions arrowOptions) {
    this.tableReadSessionRequest.setArrowOptions(arrowOptions);
    return this;
  }

  /**
   * Sets whether to fallback to no filter if specific filtering fails.
   *
   * @param enable Whether to fallback to no filter
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder enableFilterFallback(
    boolean enable) {
    this.tableReadSessionRequest.setFilterPredicateFallback(enable);
    return this;
  }

  /**
   * Sets the filter for server-side data filtering.
   *
   * <p>This allows filtering data at the server side, reducing the amount
   * of data transferred over the network.
   *
   * @param filterPredicate The filter predicate expression
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withFilter(String filterPredicate) {
    this.tableReadSessionRequest.setFilterPredicate(filterPredicate);
    return this;
  }

  /**
   * Sets the maximum number of files per split for reading.
   *
   * @param maxFilesPerSplit The maximum number of files per split
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withMaxFilesPerSplit(int maxFilesPerSplit) {
    this.tableReadSessionRequest.setSplitMaxFileNum(maxFilesPerSplit);
    return this;
  }

  /**
   * Sets the incremental read options for the read session.
   *
   * @param incrementalReadOptions The incremental read options configuration
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withIncrementalReadOptions(
    IncrementalReadOptions incrementalReadOptions) {
    this.tableReadSessionRequest.setIncrementalReadOptions(incrementalReadOptions);
    return this;
  }

  /**
   * Sets whether to enable incremental read mode for the session.
   *
   * @param incrementalRead Whether to enable incremental read mode
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withIncrementalReadEnabled(boolean incrementalRead) {
    this.tableReadSessionRequest.setIncrementalRead(incrementalRead);
    return this;
  }

  /**
   * Sets the maximum time to wait for the session to be ready.
   *
   * <p>When creating a session, it may take some time for the session to transition
   * from INIT to NORMAL state. This method controls the maximum time to wait for
   * the session to become ready.
   *
   * @param timeoutSeconds The maximum time to wait in seconds (default is 3600 seconds / 1 hour)
   * @return This builder instance for method chaining
   */
  public TableReadSessionBuilder withSessionReadyTimeout(long timeoutSeconds) {
    if (timeoutSeconds <= 0) {
      throw new IllegalArgumentException("Timeout must be positive, but was: " + timeoutSeconds);
    }
    this.sessionReadyTimeoutSeconds = timeoutSeconds;
    return this;
  }

  /**
   * Builds and returns a new TableReadSession instance with the configured settings.
   *
   * <p>This method makes an API call to the MaxCompute service to create a read session
   * with the specified configuration. The session can then be used to read data from
   * the table in a distributed manner.
   *
   * <p>If the session is in INIT state, this method will poll the session status
   * until it becomes NORMAL or the timeout is reached. The default wait time is
   * 3600 seconds (1 hour), but can be customized using
   * {@link #withSessionReadyTimeout(long)}.
   *
   * @return A new TableReadSession instance
   * @throws ClientException if the session creation fails or times out
   * @throws MaxStorageException if the session cannot be created due to server-side errors
   */
  public TableReadSession build() {
    CreateTableReadSessionResponse createTableReadSessionResponse;
    if (StringUtils.isNotBlank(sessionId)) {
      createTableReadSessionResponse =
        storageStub.getTableReadSession(table, sessionId, false);
    } else {
      createTableReadSessionResponse =
        storageStub.createTableReadSession(table, tableReadSessionRequest);
    }
    
    long waitedSeconds = 0;
    SessionStatus status = SessionStatus.fromString(createTableReadSessionResponse.getSessionStatus());
    
    while (status == SessionStatus.INIT && waitedSeconds < sessionReadyTimeoutSeconds) {
      try {
        TimeUnit.SECONDS.sleep(1);
        waitedSeconds++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ClientException("Interrupt when waiting for session to be ready.", e);
      }
      createTableReadSessionResponse =
        storageStub.getTableReadSession(table, createTableReadSessionResponse.getSessionId(),
                                        false);
      status = SessionStatus.fromString(createTableReadSessionResponse.getSessionStatus());
    }
    
    if (status == SessionStatus.INIT) {
      throw new ClientException("Session creation timeout after " + sessionReadyTimeoutSeconds + " seconds");
    }
    
    if (status != SessionStatus.NORMAL) {
      throw new ClientException("Session is not ready. Current status: " + status);
    }
    
    return new TableReadSession(storageStub, table, allocator, createTableReadSessionResponse, this);
  }

  /**
   * Gets the table identifier for this builder.
   *
   * @return The table identifier.
   */
  public TableIdentifier getTable() {
    return table;
  }

  public SplitOptions getSplitOptions() {
    return splitOptions;
  }
}