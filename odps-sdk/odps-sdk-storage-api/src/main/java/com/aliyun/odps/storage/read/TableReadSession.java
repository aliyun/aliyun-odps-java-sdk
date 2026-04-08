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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.storage.models.SplitMode;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.aliyun.odps.table.utils.SchemaUtils;

/**
 * Implementation of {@link TableReadSession} for the MaxCompute Storage API.
 *
 * <p>This class represents a read session for a MaxCompute table, allowing clients
 * to read data in a distributed manner using input splits. Each session is associated
 * with a specific table and provides methods to get the session ID, Arrow schema,
 * and input splits for parallel processing.
 *
 * <p>Example usage:
 * <pre>{@code
 * TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
 * try (TableReadSession session = client.createReadSessionBuilder(tableId).build()) {
 *     List<InputSplit> splits = session.getSplits();
 *     for (InputSplit split : splits) {
 *         try (ArrowReader reader = session.createArrowReaderBuilder(split).build()) {
 *             // Process data from reader
 *         }
 *     }
 * }
 * }</pre>
 */
public class TableReadSession implements AutoCloseable {

  private final TableSchema schema;
  private final SplitMode splitMode;
  private final String id;
  private final BufferAllocator allocator;
  private final StorageStub storageStub;
  private final TableIdentifier tableId;
  private final List<InputSplit> splits;
  private String routeToken;

  /**
   * Constructs a new TableReadSession with the provided parameters.
   *
   * @param storageStub The storage stub for communicating with the MaxCompute service
   * @param tableId     The identifier of the table to read from
   * @param allocator   The buffer allocator for Arrow memory management
   * @param response    The response from the create table read session API call
   */
  public TableReadSession(StorageStub storageStub,
                          TableIdentifier tableId,
                          BufferAllocator allocator,
                          CreateTableReadSessionResponse response,
                          TableReadSessionBuilder builder) {
    this.storageStub = storageStub;
    this.tableId = tableId;
    this.allocator = allocator;

    this.id = response.getSessionId();
    this.schema = response.getDataSchema();
    this.splitMode = response.getSplitMode();
    this.routeToken = response.getRouteToken();

    switch (splitMode) {
      case SIZE:
        int splitsCount = response.getSplitsCount();
        splits = IntStream.range(0, splitsCount)
          .mapToObj(i -> new IndexedInputSplit(id, i))
          .collect(Collectors.toList());
        break;
      case ROW_OFFSET:
        splits = new ArrayList<>();
        long recordCount = response.getRecordCount();
        for (long i = 0; i < recordCount; i += builder.getSplitOptions().getSplitNumber()) {
          splits.add(new RowRangeInputSplit(id, i,
                                            Math.min(builder.getSplitOptions().getSplitNumber(),
                                                     recordCount - i)));
        }
        break;
      default:
        // TODO
        throw new ClientException("Unsupported split mode: " + splitMode);
    }
  }

  /**
   * Gets the unique identifier of this read session.
   *
   * @return The session ID
   */
  public String getId() {
    return id;
  }

  /**
   * Gets the Arrow schema for the data in this read session.
   *
   * <p>This method converts the MaxCompute table schema to an Arrow schema,
   * which is used for efficient data processing and columnar format support.
   *
   * @return The Arrow schema representation of the table data
   */
  public Schema getArrowSchema() {
    return SchemaUtils.toArrowSchema(schema.getAllColumns());
  }


  public TableSchema getTableSchema() {
    return schema;
  }

  /**
   * Gets the input splits for this read session.
   *
   * <p>Input splits are used to divide the data into chunks that can be processed
   * in parallel. Each split represents a portion of the data that can be read
   * independently using an Arrow reader.
   *
   * @return A list of input splits for parallel processing
   */
  public List<InputSplit> getSplits() {
    return splits;
  }

  /**
   * Creates a new builder for an Arrow reader for the specified input split.
   *
   * <p>This method initializes an Arrow reader builder that can be configured with
   * various options before creating the actual Arrow reader. The reader allows
   * reading data from a specific input split in Arrow format.
   *
   * @param split The input split to read data from
   * @return A new TableReaderBuilder instance to configure the Arrow reader
   */
  public TableReaderBuilder createReaderBuilder(InputSplit split) {
    return new TableReaderBuilder(storageStub, tableId, schema, allocator, split, routeToken);
  }

  public void close() {
    // do nothing
  }
}