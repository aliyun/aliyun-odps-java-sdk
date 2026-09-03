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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.aliyun.odps.retry.RetryContext;

class RetryHandlerTest {

  @Test
  void retryContextUsesStableTraceAndZeroBasedRequestIndexes() throws Exception {
    List<Integer> policyAttempts = new ArrayList<>();
    RetryPolicy retryTwice = new RetryPolicy() {
      @Override
      public boolean shouldRetry(Exception exception, int attempt) {
        policyAttempts.add(attempt);
        return attempt < 3;
      }

      @Override
      public long getRetryWaitTime(int attempt) {
        return 0;
      }
    };
    RetryHandler handler = new RetryHandler(retryTwice, null);
    AtomicInteger calls = new AtomicInteger();
    List<String> traceIds = new ArrayList<>();
    List<Integer> retryIndexes = new ArrayList<>();

    String result = handler.executeWithRetry(
        RetryContext.create("stable-trace", 0), context -> {
          traceIds.add(context.getTraceId());
          retryIndexes.add(context.getRetryIndex());
          if (calls.incrementAndGet() < 3) {
            throw new Exception("retry");
          }
          return "ok";
        });

    assertEquals("ok", result);
    assertEquals(Arrays.asList("stable-trace", "stable-trace", "stable-trace"), traceIds);
    assertEquals(Arrays.asList(0, 1, 2), retryIndexes);
    assertEquals(Arrays.asList(1, 2), policyAttempts);
  }
}
