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

package com.aliyun.odps.table.impl.v2;

import com.aliyun.odps.storage.internal.SessionDescriptorCodec;
import com.aliyun.odps.table.Session;
import com.aliyun.odps.table.read.impl.v2.TableBatchReadSessionV2Impl;
import com.aliyun.odps.table.write.impl.v2.TableBatchWriteSessionV2Impl;

/**
 * Versioned JSON descriptions of v2 sessions, without credentials or process-local resources.
 * Restored sessions expose offline metadata and accept worker settings through reader/writer
 * options. They cannot perform global commit; explicitly reload a session with settings for
 * coordination. Descriptions contain opaque routing information and should not be logged or
 * publicly shared.
 */
final class SessionSerialization {
  private SessionSerialization() {}

  public static String toJson(Session session) {
    if (session instanceof TableBatchReadSessionV2Impl)
      return ((TableBatchReadSessionV2Impl) session).toJson();
    if (session instanceof TableBatchWriteSessionV2Impl)
      return ((TableBatchWriteSessionV2Impl) session).toDescriptorJson();
    throw new IllegalArgumentException("Versioned session serialization supports v2 sessions only");
  }

  public static Session fromJson(String json) {
    try {
      SessionDescriptorCodec descriptor = SessionDescriptorCodec.parse(json);
      if (SessionDescriptorCodec.WRITE.equals(descriptor.kind()))
        return TableBatchWriteSessionV2Impl.fromDescriptor(descriptor);
      return TableBatchReadSessionV2Impl.fromDescriptor(descriptor);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Cannot restore v2 session descriptor", e);
    }
  }
}
