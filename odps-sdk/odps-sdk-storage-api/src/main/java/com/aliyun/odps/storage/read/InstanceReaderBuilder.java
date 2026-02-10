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

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateInstanceReadStreamRequest;
import com.aliyun.odps.table.InstanceIdentifier;
import com.aliyun.odps.table.arrow.ArrowReader;

/**
 * <p>This builder allows configuration of various instance read options including:
 * <ul>
 *   <li>Buffer and batch size options</li>
 *   <li>Data column selection</li>
 *   <li>Row skip counts</li>
 *   <li>Compression settings</li>
 *   <li>Data format preferences</li>
 * </ul>
 */
public class InstanceReaderBuilder {

  private static final Logger log = LoggerFactory.getLogger(InstanceReaderBuilder.class);

  private final StorageStub storageStub;
  private final InstanceIdentifier instanceId;
  private final TableSchema tableSchema;
  private final BufferAllocator allocator;
  private final String sessionId;

  private final CreateInstanceReadStreamRequest request = new CreateInstanceReadStreamRequest();
  private Long offset = null;
  private Long count = null;
  private boolean async = false;
  private BlockingQueue<Object> asyncQueue;

  public InstanceReaderBuilder(StorageStub storageStub,
                               InstanceIdentifier instanceId,
                               String sessionId,
                               TableSchema tableSchema,
                               BufferAllocator allocator) {
    this.storageStub = storageStub;
    this.instanceId = instanceId;
    this.tableSchema = tableSchema;
    this.allocator = allocator;
    this.sessionId = sessionId;
  }

  public InstanceReaderBuilder withOffset(long offset) {
    this.offset = offset;
    return this;
  }

  public InstanceReaderBuilder withCount(long count) {
    this.count = count;
    return this;
  }

  public InstanceReaderBuilder withColumns(List<String> columns) {
    this.request.setColumns(columns);
    return this;
  }

  public InstanceReaderBuilder withTaskName(String taskName) {
    this.request.setTaskName(taskName);
    return this;
  }

  public InstanceReaderBuilder withQueryId(long queryId) {
    this.request.setQueryId(queryId);
    return this;
  }

  public InstanceReaderBuilder withEnableLimit(boolean enableLimit) {
    this.request.setEnableLimit(enableLimit);
    return this;
  }


  public InstanceReaderBuilder withAsync(boolean async) {
    this.async = async;
    return this;
  }

  public InstanceReaderBuilder withAsyncQueue(BlockingQueue<Object> asyncQueue) {
    this.asyncQueue = asyncQueue;
    return this;
  }


  public BufferAllocator getAllocator() {
    return allocator;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public boolean isAsync() {
    return async;
  }

  public BlockingQueue<Object> getAsyncQueue() {
    return asyncQueue;
  }

  /**
   * Build a new MaxInstanceArrowReader with current configurations.
   */
  public ArrowReader build() {
    if (request.getTaskName() == null) {
      log.warn("The taskName of instance reader have not set, use 'AnonymousSQLTask' as default value.");
      request.setTaskName("AnonymousSQLTask");
    }
    InputStream response =
      storageStub.createInstanceReadStream(instanceId, sessionId, count, offset, request);

    return new ArrowReaderImpl(this, response);
  }
}