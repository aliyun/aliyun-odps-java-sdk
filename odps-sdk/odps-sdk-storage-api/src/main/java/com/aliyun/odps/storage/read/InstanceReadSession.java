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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateInstanceReadSessionResponse;
import com.aliyun.odps.table.InstanceIdentifier;
import com.aliyun.odps.table.utils.SchemaUtils;

/**
 * Implementation of a read session for the MaxCompute Storage API for Instances.
 *
 * <p>This class represents a read session for a MaxCompute Instance result, allowing clients
 * to read data in a distributed manner using input splits. Each session is associated
 * with a specific instance and provides methods to get the session ID, Arrow schema,
 * and input splits for parallel processing.
 *
 * <p>Example usage:
 * <pre>{@code
 * InstanceIdentifier instanceId = InstanceIdentifier.of("my_project", "my_instance_id");
 * try (InstanceReadSession session = client.createInstanceReadSession(instanceId)) {
 *     List<InputSplit> splits = session.getSplits();
 *     for (InputSplit split : splits) {
 *         try (MaxInstanceArrowReader reader = session.createArrowReaderBuilder(split).build()) {
 *             // Process data from reader
 *         }
 *     }
 * }
 * }</pre>
 */
public class InstanceReadSession {

  private final TableSchema schema;
  private final String id;
  private final BufferAllocator allocator;
  private final StorageStub storageStub;
  private final InstanceIdentifier instanceId;
  private final long recordCount;

  /**
   * Constructs a new InstanceReadSession with the provided parameters.
   *
   * @param storageStub The storage stub for communicating with the MaxCompute service
   * @param instanceId  The identifier of the instance to read from
   * @param allocator   The buffer allocator for Arrow memory management
   * @param response    The response from the create instance read session API call
   */
  public InstanceReadSession(StorageStub storageStub,
                             InstanceIdentifier instanceId,
                             BufferAllocator allocator,
                             CreateInstanceReadSessionResponse response) {
    this.storageStub = storageStub;
    this.instanceId = instanceId;
    this.allocator = allocator;

    this.id = response.getDownloadId();
    this.schema = response.getSchema();
    this.recordCount = response.getRecordCount();
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
   * @return The Arrow schema representation of the instance data
   */
  public Schema getArrowSchema() {
    return SchemaUtils.toArrowSchema(schema.getAllColumns());
  }

  public TableSchema getTableSchema() {
    return schema;
  }

  public long getRecordCount() {
    return recordCount;
  }

  /**
   * Creates a new builder for an Arrow reader for the specified input split.
   *
   * <p>This method initializes an Arrow reader builder that can be configured with
   * various options before creating the actual Arrow reader. The reader allows
   * reading data from a specific input split in Arrow format.
   *
   * @return A new InstanceReaderBuilder instance to configure the Arrow reader
   */
  public InstanceReaderBuilder createReaderBuilder() {
    return new InstanceReaderBuilder(storageStub, instanceId, id, schema, allocator);
  }
}