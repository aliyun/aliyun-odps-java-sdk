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

package com.aliyun.odps.retry;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Identifies one actual request in an SDK retry sequence.
 *
 * <p>The trace ID remains stable for the sequence, while {@code retryIndex} is zero-based and
 * increases for every subsequent request. Calls to {@link #next()} share a thread-safe sequence
 * and reserve distinct subsequent indexes. A context is one request snapshot and must not be
 * reused as the first request of multiple retry loops.</p>
 */
public final class RetryContext {

  private final String traceId;
  private final int retryIndex;
  private final AtomicInteger sequence;

  private RetryContext(String traceId, int retryIndex, AtomicInteger sequence) {
    this.traceId = traceId;
    this.retryIndex = retryIndex;
    this.sequence = sequence;
  }

  /** Creates a new retry sequence whose first request has index {@code 0}. */
  public static RetryContext create() {
    return create(UUID.randomUUID().toString(), 0);
  }

  /**
   * Creates a retry sequence starting from the supplied request identity.
   *
   * @param traceId stable, non-empty sequence identifier
   * @param retryIndex zero-based request index
   */
  public static RetryContext create(String traceId, int retryIndex) {
    if (traceId == null || traceId.trim().isEmpty()) {
      throw new IllegalArgumentException("Retry trace ID must not be blank");
    }
    if (retryIndex < 0) {
      throw new IllegalArgumentException("Retry index must not be negative");
    }
    return new RetryContext(traceId, retryIndex, new AtomicInteger(retryIndex));
  }

  public String getTraceId() {
    return traceId;
  }

  /** Returns the zero-based index of this actual request. */
  public int getRetryIndex() {
    return retryIndex;
  }

  /** Returns a context for the next actual request in the same sequence. */
  public RetryContext next() {
    return new RetryContext(traceId, sequence.incrementAndGet(), sequence);
  }

  /** Adds this request identity to the supplied mutable header map. */
  public void injectHeaders(Map<String, String> headers) {
    Objects.requireNonNull(headers, "headers");
    headers.put(RetryHeaders.TRACE_ID, traceId);
    headers.put(RetryHeaders.RETRY_INDEX, String.valueOf(retryIndex));
  }
}
