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

/**
 * A retry policy that never retries failed operations.
 * <p>
 * This is the default policy for client errors (4xx) and other non-retryable exceptions.
 */
public class NoRetryPolicy implements RetryPolicy {

  public static final NoRetryPolicy INSTANCE = new NoRetryPolicy();

  private NoRetryPolicy() {
  }

  @Override
  public boolean shouldRetry(Exception e, int attempt) {
    return false;
  }

  @Override
  public long getRetryWaitTime(int attempt) {
    return 0;
  }

  @Override
  public void waitForNextRetry(int attempt) {
  }
}