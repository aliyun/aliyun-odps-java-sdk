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

package com.aliyun.odps.tunnel.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Test;

import com.aliyun.odps.retry.RetryContext;
import com.aliyun.odps.tunnel.TunnelException;

public class TunnelRetryHandlerTest {

  @Test
  public void testRetryContextIsStableAndIndexIsZeroBased() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(2);
    TunnelRetryHandler handler = new TunnelRetryHandler(policy, null);
    List<RetryContext> contexts = new ArrayList<>();

    String result = handler.executeWithRetry(ctx -> {
      contexts.add(ctx);
      if (ctx.getRetryIndex() < 2) {
        throw new TunnelException("retryable failure");
      }
      return "success";
    });

    assertEquals("success", result);
    assertEquals(3, contexts.size());
    String traceId = contexts.get(0).getTraceId();
    assertNotNull(traceId);
    assertEquals(traceId, contexts.get(1).getTraceId());
    assertEquals(traceId, contexts.get(2).getTraceId());
    assertEquals(0, contexts.get(0).getRetryIndex());
    assertEquals(1, contexts.get(1).getRetryIndex());
    assertEquals(2, contexts.get(2).getRetryIndex());
    assertEquals(Arrays.asList(1, 2), policy.getAttempts());
  }

  @Test
  public void testExistingInitialContextContinuesItsSharedSequence() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(1);
    TunnelRetryHandler handler = new TunnelRetryHandler(policy, null);
    RetryContext root = RetryContext.create("existing-trace", 4);
    RetryContext nestedStart = root.next();
    List<RetryContext> contexts = new ArrayList<>();

    handler.executeWithRetry(nestedStart, ctx -> {
      contexts.add(ctx);
      if (contexts.size() == 1) {
        throw new TunnelException("retry once");
      }
      return null;
    });

    assertEquals(2, contexts.size());
    assertEquals("existing-trace", contexts.get(0).getTraceId());
    assertEquals("existing-trace", contexts.get(1).getTraceId());
    assertEquals(5, contexts.get(0).getRetryIndex());
    assertEquals(6, contexts.get(1).getRetryIndex());
    assertEquals(7, root.next().getRetryIndex());
    assertEquals(Arrays.asList(1), policy.getAttempts());
  }

  @Test
  public void testCallableOverloadStillWorks() throws Exception {
    TunnelRetryHandler handler = new TunnelRetryHandler();
    Callable<String> action = () -> "callable-result";

    assertEquals("callable-result", handler.executeWithRetry(action));
  }

  @Test(expected = TunnelException.class)
  public void testNoRetryPolicyThrowsImmediately() throws Exception {
    TunnelRetryHandler handler = new TunnelRetryHandler();

    handler.executeWithRetry(ctx -> {
      throw new TunnelException("fail");
    });
  }

  private static final class RecordingRetryPolicy implements TunnelRetryHandler.RetryPolicy {

    private final int retryLimit;
    private final List<Integer> attempts = new ArrayList<>();

    private RecordingRetryPolicy(int retryLimit) {
      this.retryLimit = retryLimit;
    }

    @Override
    public boolean shouldRetry(Exception e, int attempt) {
      attempts.add(attempt);
      return attempt <= retryLimit;
    }

    @Override
    public long getRetryWaitTime(int attempt) {
      return 0;
    }

    @Override
    public void waitForNextRetry(int attempt) {
      // Tests deliberately avoid the production exponential backoff.
    }

    private List<Integer> getAttempts() {
      return attempts;
    }
  }
}
