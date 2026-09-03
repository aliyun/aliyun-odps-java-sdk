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

/**
 * Enum representing the write mode for table write sessions.
 *
 * <p>BATCH mode: Default mode. Data is written to the table and becomes visible
 * only after the session is committed.
 *
 * <p>BATCH_COMPATIBLE mode: Compatibility mode for clients that need block-number and
 * attempt-number write semantics. Each block returns a typed result that must be supplied
 * when committing the session. Data becomes visible only after the session is committed.
 *
 * <p>STREAMING mode: Data becomes visible immediately after flush, without requiring
 * explicit commit. The session uses a default session ID and does not require
 * explicit session creation.
 *
 * <p>STREAMING_REALTIME mode: Realtime streaming write mode. Data becomes visible
 * immediately after flush with lowest latency. Behaves like STREAMING on the client
 * side (no explicit session creation, no commit required), but the server uses
 * a realtime-optimized pipeline.
 */
public enum WriteMode {

  /**
   * Batch write mode. Data becomes visible only after session commit.
   */
  BATCH("Batch"),

  /**
   * Batch compatibility mode. Writers are identified by block and attempt numbers, and
   * successful block results are supplied when committing the session.
   */
  BATCH_COMPATIBLE("BatchCompatible"),

  /**
   * Streaming write mode. Data becomes visible immediately after flush.
   * Without a static partition, the client uses session id {@code default} and skips
   * {@code TableCreateWriteSession}. With a static partition, a write session is created first.
   */
  STREAMING("Streaming"),

  /**
   * Realtime streaming write mode. Data becomes visible immediately after flush
   * with lowest latency. Client-side behavior is the same as STREAMING.
   */
  STREAMING_REALTIME("StreamingRealtime");

  private final String value;

  WriteMode(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  /**
   * Returns whether this write mode is a streaming mode (STREAMING or STREAMING_REALTIME).
   *
   * @return true if this is a streaming write mode
   */
  public boolean isStreaming() {
    return this == STREAMING || this == STREAMING_REALTIME;
  }

  public static WriteMode fromValue(String value) {
    if (value == null) {
      return BATCH;
    }
    for (WriteMode mode : WriteMode.values()) {
      if (mode.value.equalsIgnoreCase(value)) {
        return mode;
      }
    }
    return BATCH;
  }
}
