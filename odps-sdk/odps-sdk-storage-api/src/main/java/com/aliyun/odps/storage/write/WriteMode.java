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
 * <p>STREAMING mode: Data becomes visible immediately after flush, without requiring
 * explicit commit. The session uses a default session ID and does not require
 * explicit session creation.
 */
public enum WriteMode {

  /**
   * Batch write mode. Data becomes visible only after session commit.
   */
  BATCH("Batch"),

  /**
   * Streaming write mode. Data becomes visible immediately after flush.
   * No explicit session creation required, uses default session ID.
   */
  STREAMING("Streaming");

  private final String value;

  WriteMode(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
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
