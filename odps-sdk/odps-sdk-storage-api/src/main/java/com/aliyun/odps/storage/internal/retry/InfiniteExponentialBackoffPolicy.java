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

package com.aliyun.odps.storage.internal.retry;

import java.util.concurrent.TimeUnit;

/**
 * A retry policy with infinite exponential backoff and jitter.
 * <p>
 * This policy retries indefinitely with exponential backoff up to a maximum wait time,
 * and adds random jitter to prevent thundering herd problems when multiple clients
 * retry simultaneously.
 * <p>
 * This policy is suitable for transient errors like rate limiting (429) or temporary
 * service unavailability.
 * <p>
 * Wait time calculation: base_wait = 2^(attempt-1) seconds (capped at 64 seconds)
 * With jitter: final_wait = base_wait + (base_wait * 0.1 * random)
 * Max wait time: 64 seconds
 */
public class InfiniteExponentialBackoffPolicy implements RetryPolicy {

  private static final long MAX_WAIT_TIME_MS = TimeUnit.SECONDS.toMillis(64);
  private static final double JITTER_FACTOR = 0.1;

  public static final InfiniteExponentialBackoffPolicy INSTANCE = new InfiniteExponentialBackoffPolicy();

  private InfiniteExponentialBackoffPolicy() {
  }

  @Override
  public boolean shouldRetry(Exception e, int attempt) {
    return true;
  }

  @Override
  public long getRetryWaitTime(int attempt) {
    long baseWaitMs;
    if (attempt < 7) {
      baseWaitMs = (long) Math.pow(2, attempt - 1) * 1000;
    } else {
      baseWaitMs = MAX_WAIT_TIME_MS;
    }
    long jitterMs = (long) (baseWaitMs * JITTER_FACTOR * Math.random());
    return baseWaitMs + jitterMs;
  }
}