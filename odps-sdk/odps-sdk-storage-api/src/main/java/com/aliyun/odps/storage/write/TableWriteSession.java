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

import java.util.List;

import com.aliyun.odps.storage.internal.models.GetTableWriteSessionResponse;
import org.apache.arrow.memory.BufferAllocator;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.Constants;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.table.TableIdentifier;

/**
 * <p>This class represents a write session for a MaxCompute table, allowing clients
 * to write data in a distributed manner using Arrow format. Each session is associated
 * with a specific table and provides methods to create writers, commit or abort
 * the write operation.
 *
 * <p>Example usage (Batch mode):
 * <pre>{@code
 * TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
 * try (TableWriteSession session = client.createWriteSessionBuilder(tableId).build()) {
 *     TableArrowWriter writer = session.createWriterBuilder("stream1", 1).build();
 *     // Write data using writer
 *     writer.close();
 *     session.commit();
 * }
 * }</pre>
 *
 * <p>Example usage (Streaming mode):
 * <pre>{@code
 * TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
 * try (TableWriteSession session = client.createWriteSessionBuilder(tableId)
 *         .withWriteMode(WriteMode.STREAMING).build()) {
 *     TableArrowWriter writer = session.createWriterBuilder("stream1", 1).build();
 *     // Write data using writer
 *     writer.writeBatch(root);
 *     writer.flush(); // Data becomes visible immediately
 *     writer.close();
 *     // No commit needed for streaming mode
 * }
 * }</pre>
 *
 * <p>The session must be either committed or aborted to properly clean up resources
 * in BATCH mode. In STREAMING mode, data is visible after flush and no commit is needed.
 * If {@link TableWriteSessionBuilder#withPartition} is set, STREAMING still skips an explicit
 * commit, but the client creates a write session first so {@code PartialPartitionSpec} is sent.
 */
public class TableWriteSession implements AutoCloseable {

  private final BufferAllocator allocator;
  boolean closed = false;
  boolean committed = false;
  private final StorageStub storageStub;
  private final String id;
  private final TableIdentifier tableId;
  private final PartitionSpec staticPartitionSpec;
  private final WriteMode writeMode;
  private String routeToken;

  /**
   * Constructs a new TableWriteSession with the provided parameters.
   *
   * @param storageStub The storage stub for communicating with the MaxCompute service
   * @param tableId     The identifier of the table to write to
   * @param allocator   The buffer allocator for Arrow memory management
   * @param sessionId   The sessionId of Write Session
   * @param writeMode   The write mode (BATCH or STREAMING)
   * @param routeToken  The route token for routing the write request
   */
  TableWriteSession(StorageStub storageStub,
                           TableIdentifier tableId,
                           PartitionSpec staticPartitionSpec,
                           BufferAllocator allocator,
                           String sessionId,
                    WriteMode writeMode,
                    String routeToken) {
    this.storageStub = storageStub;
    this.tableId = tableId;
    this.allocator = allocator;
    this.staticPartitionSpec = staticPartitionSpec;
    this.id = sessionId;
    this.writeMode = writeMode != null ? writeMode : WriteMode.BATCH;
    this.routeToken = routeToken;
  }

  /**
   * Updates the route token if it has not been set yet. This is called when a writer is
   * created for a streaming session, where the initial route token is {@code null} and
   * is obtained from the {@code createTableWriteStream} response.
   *
   * @param token the route token returned by the server
   */
  void updateRouteToken(String token) {
    if (this.routeToken == null && token != null) {
      this.routeToken = token;
    }
  }
  /**
   * Gets the unique identifier of this write session.
   *
   * @return The session ID
   */
  public String getId() {
    return id;
  }

  /**
   * Gets the write mode of this session.
   *
   * @return The write mode
   */
  public WriteMode getWriteMode() {
    return writeMode;
  }


  /**
   * Queries the server for the minimum uncommitted staging id of this streaming write session.
   *
   * <p>The returned value is the smallest staging id currently held by the streaming
   * auto-committer (i.e. the next staging batch to be committed). It is useful for reasoning
   * about async visibility progress of streaming writes.
   *
   * <p>Only valid in STREAMING mode. In BATCH mode, throws
   * {@link UnsupportedOperationException}.
   *
   * @return the min uncommitted staging id, may be {@code null} if the server has no staging
   *     batch pending (e.g. all flushed data already committed)
   * @throws ClientException if the server returns an error
   * @throws UnsupportedOperationException if not called on a streaming write session
   */
  public String getMinUncommittedStagingId() {
    if (!writeMode.isStreaming() || !Constants.AUTO_COMMIT_SESSION_ID.equals(id)) {
      throw new UnsupportedOperationException(
        "getMinUncommittedStagingId is only supported on streaming write with default session");
    }
    try {
      GetTableWriteSessionResponse resp =
          storageStub.getTableWriteSession(tableId, id, routeToken, writeMode);
      return resp.getMinUncommittedStagingId();
    } catch (ServiceException e) {
      // Session not found means the auto-committer session has expired on the server side.
      // This typically indicates that all staging data has already been committed and the
      // session was cleaned up. Treat it as "no uncommitted staging" (return null).
      if (e.getHttpStatus() == 404) {
        return null;
      }
      throw e;
    }
  }


  /**
   * Creates a new builder for a writer for this write session.
   *
   * <p>This method initializes a writer builder that can be configured with
   * various options before creating the actual writer. The writer allows
   * writing data to the table in Arrow format.
   *
   * @param streamId The unique identifier for the write stream
   * @param streamVersion The version of the write stream (must be >= 1)
   * @return A new TableWriterBuilder instance to configure the writer
   * @throws IllegalArgumentException if streamVersion is less than 1
   */
  public TableWriterBuilder createWriterBuilder(String streamId, long streamVersion) {
    if (streamVersion < 1) {
      throw new IllegalArgumentException("streamVersion must be >= 1, but was: " + streamVersion);
    }
    return new TableWriterBuilder(storageStub, tableId, staticPartitionSpec, allocator, id,
                                  streamId, streamVersion, writeMode, routeToken, this);
  }

  /**
   * Creates a new builder for a writer for this write session.
   *
   * <p>This method is intended for Exactly-Once mode where streamVersion is not required.
   * The streamVersion will be set to 0 internally.
   *
   * @param streamId The unique identifier for the write stream
   * @return A new TableWriterBuilder instance to configure the writer
   */
  public TableWriterBuilder createWriterBuilder(String streamId) {
    return new TableWriterBuilder(storageStub, tableId, staticPartitionSpec, allocator, id,
                                  streamId, 0, writeMode, routeToken, this);
  }

  /**
   * Commits the write session, making all written data available in the table.
   *
   * <p>This method makes an API call to the MaxCompute service to commit the write session.
   * After this call, all data written through the writers associated with this session
   * will be visible in the table. The session is automatically closed after committing.
   *
   * <p>Note: For legacy unpartitioned streaming (session id {@code default}), this is a no-op.
   * For STREAMING with an explicit write session, this calls {@code TableCommitWriteSession}.
   *
   * @throws IllegalStateException if the session is already closed
   */
  public void commit() {
    commit(null, null);
  }

  /**
   * Commits a batch write session. Pass closed stream ids and versions (same as used for
   * {@link TableWriteSession#createWriterBuilder(String, long)}) when the service requires
   * them in the commit body; otherwise use {@link #commit()}.
   */
  public void commit(List<String> streamIds, List<Long> streamVersions) {
    checkNotClosed();
    if (writeMode.isStreaming()
        && Constants.AUTO_COMMIT_SESSION_ID.equals(id)) {
      // Unpartitioned streaming: no TableCreateWriteSession; data visible after flush.
      committed = true;
      closed = true;
      return;
    }
    storageStub.commitTableWriteSession(tableId, id, routeToken, streamIds, streamVersions, writeMode);
    committed = true;
    closed = true;
  }

  /**
   * Aborts the write session, discarding all written data.
   *
   * <p>This method makes an API call to the MaxCompute service to abort the write session.
   * After this call, all data written through the writers associated with this session
   * will be discarded. The session is automatically closed after aborting.
   *
   * <p>Note: Legacy {@code default}-session streaming skips the HTTP abort; explicit sessions
   * call {@code TableAbortWriteSession}.
   */
  public void abort() {
    if (closed) {
      return;
    }
    if (writeMode.isStreaming()
        && Constants.AUTO_COMMIT_SESSION_ID.equals(id)) {
      closed = true;
      return;
    }
    storageStub.abortTableWriteSession(tableId, id, routeToken, writeMode);
    closed = true;
  }

  /**
   * Closes the write session, automatically aborting if not already committed.
   *
   * <p>This method ensures proper cleanup of resources. If the session has not been
   * explicitly committed, it will be automatically aborted to discard any written data.
   * This behavior is intentional to prevent data inconsistency when a session is closed
   * without explicit commitment.
   *
   * <p>In STREAMING mode, this method simply marks the session as closed without
   * additional cleanup since data is already visible after flush.
   *
   * <p>If you want to explicitly close the session without aborting, call {@link #commit()}
   * before calling this method.
   *
   * <p>This method is safe to call multiple times.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }

    if (!committed) {
      abort();
    }
  }

  /**
   * Checks that the session is not closed, throwing an exception if it is.
   *
   * @throws ClientException if the session is already closed
   */
  private void checkNotClosed() {
    if (closed) {
      throw new ClientException("Session is already closed");
    }
  }
}