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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

public class RetryContextTest {

  @Test
  public void testCreateUsesUuidAndZeroIndex() {
    RetryContext context = RetryContext.create();

    assertEquals(UUID.fromString(context.getTraceId()).toString(), context.getTraceId());
    assertEquals(0, context.getRetryIndex());
  }

  @Test
  public void testNextSharesOneMonotonicSequence() {
    RetryContext root = RetryContext.create("test-trace", 3);

    RetryContext firstBranch = root.next();
    RetryContext secondBranch = root.next();
    RetryContext firstBranchNext = firstBranch.next();

    assertEquals("test-trace", firstBranch.getTraceId());
    assertEquals("test-trace", secondBranch.getTraceId());
    assertEquals("test-trace", firstBranchNext.getTraceId());
    assertEquals(4, firstBranch.getRetryIndex());
    assertEquals(5, secondBranch.getRetryIndex());
    assertEquals(6, firstBranchNext.getRetryIndex());
  }

  @Test
  public void testConcurrentNextIndexesAreUnique() throws Exception {
    int threadCount = 8;
    int contextsPerThread = 100;
    RetryContext root = RetryContext.create("concurrent-trace", 0);
    Set<Integer> indexes = Collections.newSetFromMap(
        new ConcurrentHashMap<Integer, Boolean>());
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<Future<?>> futures = new ArrayList<>();

    try {
      for (int thread = 0; thread < threadCount; thread++) {
        futures.add(executor.submit(() -> {
          for (int index = 0; index < contextsPerThread; index++) {
            RetryContext context = root.next();
            assertEquals("concurrent-trace", context.getTraceId());
            assertTrue(indexes.add(context.getRetryIndex()));
          }
        }));
      }
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }

    int total = threadCount * contextsPerThread;
    assertEquals(total, indexes.size());
    for (int index = 1; index <= total; index++) {
      assertTrue("missing retry index " + index, indexes.contains(index));
    }
  }

  @Test
  public void testInjectHeadersUsesProtocolNames() {
    assertEquals("odps-tunnel-retry-trace-id", RetryHeaders.TRACE_ID);
    assertEquals("odps-tunnel-retry-index", RetryHeaders.RETRY_INDEX);

    RetryContext context = RetryContext.create("header-trace", 9);
    Map<String, String> headers = new HashMap<>();
    context.injectHeaders(headers);

    assertEquals("header-trace", headers.get("odps-tunnel-retry-trace-id"));
    assertEquals("9", headers.get("odps-tunnel-retry-index"));
  }

  @Test
  public void testCreateRejectsInvalidIdentity() {
    assertIllegalArgument(() -> RetryContext.create(null, 0));
    assertIllegalArgument(() -> RetryContext.create("", 0));
    assertIllegalArgument(() -> RetryContext.create(" \t", 0));
    assertIllegalArgument(() -> RetryContext.create("trace", -1));
  }

  private static void assertIllegalArgument(Runnable action) {
    try {
      action.run();
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }
}
