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
 * Retry policy interface for determining whether and how to retry failed operations.
 */
public interface RetryPolicy {

  /**
   * Determines whether an operation should be retried based on the exception and attempt count.
   *
   * @param e The exception that occurred
   * @param attempt The current attempt number (1-based)
   * @return true if the operation should be retried, false otherwise
   */
  boolean shouldRetry(Exception e, int attempt);

  /**
   * Calculates the wait time before the next retry attempt.
   *
   * @param attempt The current attempt number (1-based)
   * @return The wait time in milliseconds
   */
  long getRetryWaitTime(int attempt);

  /**
   * Waits for the configured retry delay before the next attempt.
   *
   * @param attempt The current attempt number (1-based)
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  default void waitForNextRetry(int attempt) throws InterruptedException {
    TimeUnit.MILLISECONDS.sleep(getRetryWaitTime(attempt));
  }
}