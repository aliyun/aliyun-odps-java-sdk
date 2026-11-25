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

package com.aliyun.odps.storage.models;

import com.aliyun.odps.storage.internal.SessionDescriptorCodec;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;

/**
 * Immutable, versioned description of a Storage API session, independent of credentials and
 * clients.
 *
 * <p>Export with a native session's getDescriptor(), transfer toJson(), then parse with fromJson().
 * Bind to a worker using its native session builder's withSessionDescriptor(). Restoration does not
 * create or reload a remote session. A restored write session has only block-writing authority.
 * Descriptions contain session identity and opaque routing data; do not log or publicly share them.
 */
public final class SessionDescriptor {
  /** Session purpose, independent of the Java implementation class. */
  public enum Kind {
    BATCH_READ,
    INCREMENTAL_READ,
    BATCH_WRITE
  }

  private final SessionDescriptorCodec codec;

  private SessionDescriptor(SessionDescriptorCodec codec) {
    this.codec = codec;
  }

  /** Parses a versioned description without network access or process-local configuration. */
  public static SessionDescriptor fromJson(String json) {
    return new SessionDescriptor(SessionDescriptorCodec.parse(json));
  }

  public String toJson() {
    return codec.toJson();
  }

  public Kind getKind() {
    switch (codec.kind()) {
      case SessionDescriptorCodec.READ:
        return Kind.BATCH_READ;
      case SessionDescriptorCodec.INCREMENTAL:
        return Kind.INCREMENTAL_READ;
      case SessionDescriptorCodec.WRITE:
        return Kind.BATCH_WRITE;
      default:
        throw new IllegalStateException("Unsupported session kind");
    }
  }

  public TableIdentifier getTableIdentifier() {
    return codec.table();
  }

  public String getSessionId() {
    return codec.metadata().get("SessionId").getAsString();
  }

  /** Returns a fresh copy of write encoding units; read descriptions do not carry these options. */
  public ArrowOptions getArrowOptions() {
    if (getKind() != Kind.BATCH_WRITE) {
      throw new IllegalStateException(
          "Arrow encoding units are only available for write descriptors");
    }
    return codec.arrowOptions();
  }
}
