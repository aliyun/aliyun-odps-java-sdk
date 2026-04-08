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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.memory.BufferAllocator;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.BlobWriteItem;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.GetWriteStreamRequest;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowWriter;

/**
 * <p>This builder creates Arrow writers that can write data to MaxCompute tables
 * using the Arrow columnar format. The writer supports various compression codecs
 * for efficient data transfer.
 *
 * @see TableArrowWriter
 */
public class TableWriterBuilder {

  private final CreateWriteStreamRequest request = new CreateWriteStreamRequest();

  private final TableIdentifier tableId;

  private final PartitionSpec staticPartitionSpec;

  private final String sessionId;

  private final StorageStub storageStub;

  private final BufferAllocator allocator;

  private long bufferSize = 64 * 1024 * 1024;

  private final String streamId;

  private final Long streamVersion;

  private boolean batchBlobUploadEnabled;

  private BlobWriteItem.ChecksumType blobChecksumType = BlobWriteItem.ChecksumType.None;

  private boolean autoFlushEnabled = true;

  private boolean resume = false;

  private ExecutorService executorService;

  private final WriteMode writeMode;

  private String routeToken;

  /**
   * Constructs a new TableWriterBuilder with the provided parameters.
   *
   * @param storageStub The storage stub for communicating with the MaxCompute service
   * @param tableId     The identifier of the table to write to
   * @param allocator   The buffer allocator for Arrow memory management
   * @param sessionId   The session ID for this write session
   * @param streamId    The stream ID for this write stream
   * @param streamVersion The stream version for this write stream
   * @param writeMode   The write mode (BATCH or STREAMING)
   */
  TableWriterBuilder(StorageStub storageStub,
                     TableIdentifier tableId,
                     PartitionSpec staticPartitionSpec,
                     BufferAllocator allocator,
                     String sessionId,
                     String streamId,
                     long streamVersion,
                     WriteMode writeMode,
                     String routeToken) {
    this.storageStub = storageStub;
    this.tableId = tableId;
    this.staticPartitionSpec = staticPartitionSpec;
    this.sessionId = sessionId;
    this.allocator = allocator;

    this.streamId = streamId;
    this.request.setStreamId(streamId);

    this.streamVersion = streamVersion;
    this.request.setStreamVersion(streamVersion);

    this.writeMode = writeMode != null ? writeMode : WriteMode.BATCH;
    this.routeToken = routeToken;
  }

  public TableWriterBuilder withBufferSize(long bufferSize) {
    this.bufferSize = bufferSize;
    return this;
  }

  public TableWriterBuilder withBatchBlobUploadEnabled(boolean enabled) {
    this.batchBlobUploadEnabled = enabled;
    return this;
  }

  public TableWriterBuilder withBlobChecksumType(BlobWriteItem.ChecksumType blobChecksumType) {
    this.blobChecksumType = blobChecksumType;
    return this;
  }

  public TableWriterBuilder withAutoFlushEnabled(boolean enabled) {
    this.autoFlushEnabled = enabled;
    return this;
  }

  public TableWriterBuilder withResume(boolean resume) {
    this.resume = resume;
    return this;
  }

  public TableWriterBuilder withExecutorService(ExecutorService executorService) {
    this.executorService = executorService;
    return this;
  }

  /**
   * Builds and returns a new ArrowWriter instance with the configured settings.
   *
   * <p>This method makes an API call to the MaxCompute service to create a write stream
   * with the specified configuration. The writer can then be used to write Arrow data
   * to the table.
   *
   * @return A new ArrowWriter instance
   * @throws com.aliyun.odps.storage.MaxStorageException if unable to create the writer
   */
  public ArrowWriter build() {
    CreateWriteStreamResponse response = null;
    if (resume) {
      response = storageStub.getWriteStream(
              GetWriteStreamRequest.newBuilder()
                      .withSessionId(sessionId)
                      .withStreamId(streamId)
                      .withStreamVersion(streamVersion)
                      .withTableIdentifier(tableId)
                      .build(), routeToken);
    } else {
      response = storageStub.createTableWriteStream(tableId, sessionId, request, routeToken);
    }
    if (batchBlobUploadEnabled) {
      return new TableArrowBatchBlobWriter(this, response);
    } else {
      return new TableArrowWriter(this, response);
    }
  }

  public TableIdentifier getTableId() {
    return tableId;
  }

  public PartitionSpec getStaticPartitionSpec() {
    return staticPartitionSpec;
  }

  public String getSessionId() {
    return sessionId;
  }

  public StorageStub getStorageStub() {
    return storageStub;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public long getBufferSize() {
    return bufferSize;
  }

  public String getStreamId() {
    return streamId;
  }

  public long getStreamVersion() {
    return streamVersion;
  }

  public BlobWriteItem.ChecksumType getBlobChecksumType() {
    return blobChecksumType;
  }

  public boolean isAutoFlushEnabled() {
    return autoFlushEnabled;
  }

  public ExecutorService getExecutorService() {
    if (executorService == null) {
      // Create default single-threaded executor if not provided
      executorService = Executors.newSingleThreadExecutor();
    }
    return executorService;
  }

  public WriteMode getWriteMode() {
    return writeMode;
  }
}