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

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateTableReadStreamRequest;
import com.aliyun.odps.storage.models.DataFormat;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.read.split.InputSplit;


public class TableReaderBuilder {

  private final CreateTableReadStreamRequest request = new CreateTableReadStreamRequest();

  private final TableIdentifier tableId;

  private final String sessionId;

  private final StorageStub storageStub;

  private final InputSplit inputSplit;

  private final BufferAllocator allocator;

  private boolean async = false;

  private BlockingQueue<Object> asyncQueue;

  private TableSchema tableSchema;


  TableReaderBuilder(StorageStub storageStub,
                     TableIdentifier tableId,
                     TableSchema tableSchema,
                     BufferAllocator allocator,
                     InputSplit inputSplit) {
    this.storageStub = storageStub;
    this.tableId = tableId;
    this.sessionId = inputSplit.getSessionId();
    this.tableSchema = tableSchema;
    this.inputSplit = inputSplit;
    this.allocator = allocator;
  }

  public TableReaderBuilder withMaxBatchRows(long maxBatchRows) {
    this.request.setMaxBatchRows(maxBatchRows);
    return this;
  }

  public TableReaderBuilder withSkipRowNum(long skipRowNum) {
    this.request.setSkipRowNum(skipRowNum);
    return this;
  }

  public TableReaderBuilder withMaxBatchRawSize(long maxBatchRawSize) {
    this.request.setMaxBatchRawSize(maxBatchRawSize);
    return this;
  }

  public TableReaderBuilder withDataFormat(
    DataFormat dataFormat) {
    this.request.setDataFormat(dataFormat);
    return this;
  }

  public TableReaderBuilder withDataColumns(List<String> dataColumns) {
    this.request.setDataColumns(dataColumns);
    return this;
  }

  public TableReaderBuilder withDataColumnsUnordered(boolean dataColumnsUnordered) {
    this.request.setDataColumnsUnordered(dataColumnsUnordered);
    return this;
  }

  public TableReaderBuilder withAsync(boolean async) {
    this.async = async;
    return this;
  }

  public TableReaderBuilder withAsyncQueue(BlockingQueue<Object> asyncQueue) {
    this.asyncQueue = asyncQueue;
    return this;
  }

  public TableIdentifier getTableId() {
    return tableId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public StorageStub getStorageStub() {
    return storageStub;
  }

  public InputSplit getInputSplit() {
    return inputSplit;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public boolean isAsync() {
    return async;
  }

  public BlockingQueue<Object> getAsyncQueue() {
    return asyncQueue;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public ArrowReader build() {
    InputStream response = storageStub.createTableReadStream(tableId, inputSplit, request);

    return new ArrowReaderImpl(this, response);
  }
}