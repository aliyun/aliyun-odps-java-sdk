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
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.table.TableIdentifier;

/**
 * <p>This class represents a write session for a MaxCompute table, allowing clients
 * to write data in a distributed manner using Arrow format. Each session is associated
 * with a specific table and provides methods to create writers, commit or abort
 * the write operation.
 *
 * <p>Example usage:
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
 * <p>The session must be either committed or aborted to properly clean up resources.
 * If neither commit() nor abort() is called before closing the session, abort() 
 * will be called automatically.
 */
public class TableWriteSession implements AutoCloseable {

  private final BufferAllocator allocator;
  boolean closed = false;
  boolean committed = false;
  private final StorageStub storageStub;
  private final String id;
  private final TableIdentifier tableId;
  private final PartitionSpec staticPartitionSpec;

  /**
   * Constructs a new TableWriteSession with the provided parameters.
   *
   * @param storageStub The storage stub for communicating with the MaxCompute service
   * @param tableId     The identifier of the table to write to
   * @param allocator   The buffer allocator for Arrow memory management
   * @param sessionId   The sessionId of Write Session
   */
  TableWriteSession(StorageStub storageStub,
                           TableIdentifier tableId,
                           PartitionSpec staticPartitionSpec,
                           BufferAllocator allocator,
                           String sessionId) {
    this.storageStub = storageStub;
    this.tableId = tableId;
    this.allocator = allocator;
    this.staticPartitionSpec = staticPartitionSpec;

    this.id = sessionId;
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
    return new TableWriterBuilder(storageStub, tableId, staticPartitionSpec, allocator, id, streamId, streamVersion);
  }

  /**
   * Commits the write session, making all written data available in the table.
   *
   * <p>This method makes an API call to the MaxCompute service to commit the write session.
   * After this call, all data written through the writers associated with this session
   * will be visible in the table. The session is automatically closed after committing.
   *
   * @throws IllegalStateException if the session is already closed
   */
  public void commit() {
    checkNotClosed();
    storageStub.commitTableWriteSession(tableId, id);
    committed = true;
    closed = true;
  }

  /**
   * Aborts the write session, discarding all written data.
   *
   * <p>This method makes an API call to the MaxCompute service to abort the write session.
   * After this call, all data written through the writers associated with this session
   * will be discarded. The session is automatically closed after aborting.
   */
  public void abort() {
    if (closed) {
      return;
    }
    storageStub.abortTableWriteSession(tableId, id);
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