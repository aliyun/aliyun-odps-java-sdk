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

import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateInstanceReadSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateInstanceReadSessionResponse;
import com.aliyun.odps.table.InstanceIdentifier;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.utils.StringUtils;

/**
 * Implementation of a builder for Instance Read Session in the MaxCompute Storage API.
 *
 * <p>This builder allows configuration of instance read session settings including:
 * <ul>
 *   <li>Data column selection</li>
 *   <li>Split options for parallel processing</li>
 *   <li>Arrow-specific options</li>
 *   <li>Filter predicates for data filtering</li>
 * </ul>
 */
public class InstanceReadSessionBuilder {

  private StorageStub storageStub;

  private final CreateInstanceReadSessionRequest
    instanceReadSessionRequest = new CreateInstanceReadSessionRequest();

  private final InstanceIdentifier instance;
  private final BufferAllocator allocator;

  private boolean enableLimit;
  private String sessionId;

  /**
   * Constructs a new InstanceReadSessionBuilder with the provided parameters.
   *
   * @param storageStub The storage stub for communicating with the MaxCompute service
   * @param allocator   The buffer allocator for Arrow memory management
   * @param instance    The identifier of the instance to read from
   * @throws IllegalArgumentException if instance is null
   */
  public InstanceReadSessionBuilder(StorageStub storageStub,
                                    BufferAllocator allocator,
                                    InstanceIdentifier instance) {
    Preconditions.checkNotNull(instance, "Instance identifier cannot be null");
    this.instance = instance;
    this.allocator = allocator;
    this.storageStub = storageStub;
  }

  public InstanceReadSessionBuilder withSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public InstanceReadSessionBuilder withEnableLimit(boolean enableLimit) {
    this.instanceReadSessionRequest.setEnableLimit(enableLimit);
    return this;
  }


  /**
   * Builds and returns a new InstanceReadSession instance with the configured settings.
   *
   * <p>This method makes an API call to the MaxCompute service to create a read session
   * with the specified configuration. The session can then be used to read data from
   * the instance in a distributed manner.
   *
   * @return A new InstanceReadSession instance
   */
  public InstanceReadSession build() {
    CreateInstanceReadSessionResponse createInstanceReadSessionResponse;
    if (StringUtils.isNotBlank(sessionId)) {
      createInstanceReadSessionResponse =
        storageStub.getInstanceReadSession(instance, sessionId);
    } else {
      createInstanceReadSessionResponse =
        storageStub.createInstanceReadSession(instance, instanceReadSessionRequest);
    }

    return new InstanceReadSession(storageStub, instance, allocator,
                                   createInstanceReadSessionResponse);
  }

  /**
   * Gets the instance identifier for this builder.
   *
   * @return The instance identifier.
   */
  public InstanceIdentifier getInstance() {
    return instance;
  }
}