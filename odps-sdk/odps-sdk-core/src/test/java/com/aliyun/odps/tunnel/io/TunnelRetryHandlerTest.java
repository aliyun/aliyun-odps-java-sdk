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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.aliyun.odps.retry.RetryContext;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.TunnelConstants;
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

  @Test
  public void testThrottlingDoesNotConsumeDefaultRetriesOrBackoff() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(3);
    List<Long> loggedAttempts = new ArrayList<>();
    List<Long> loggedWaits = new ArrayList<>();
    NoWaitRetryHandler handler = new NoWaitRetryHandler(policy, new RestClient.RetryLogger() {
      @Override
      public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
        loggedAttempts.add(retryCount);
        loggedWaits.add(retrySleepTime);
      }
    });
    List<RetryContext> contexts = new ArrayList<>();
    TunnelException localError = new TunnelException("network failure");

    try {
      handler.executeWithRetry(ctx -> {
        contexts.add(ctx);
        if (contexts.size() <= 100) {
          throw statusError(429);
        }
        throw localError;
      });
      fail("Expected the fourth local error to exhaust the default retry budget");
    } catch (TunnelException e) {
      assertSame(localError, e);
    }

    assertEquals(TunnelConstants.LOCAL_ERROR_CODE, localError.getErrorCode());
    assertEquals(104, contexts.size());
    assertEquals(Arrays.asList(1, 2, 3, 4), policy.getAttempts());
    assertEquals(Arrays.asList(1, 2, 3), policy.waitAttempts);
    assertEquals(Arrays.asList(1, 2, 3), policy.waitTimeAttempts);
    assertEquals(Arrays.asList(101L, 102L, 103L), loggedAttempts.subList(100, 103));
    assertEquals(Arrays.asList(10L, 20L, 30L), loggedWaits.subList(100, 103));
    for (int i = 0; i < contexts.size(); i++) {
      assertEquals(i, contexts.get(i).getRetryIndex());
      assertEquals(contexts.get(0).getTraceId(), contexts.get(i).getTraceId());
    }
  }

  @Test
  public void testAlternatingErrorsDoNotResetDefaultBudget() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(3);
    NoWaitRetryHandler handler = new NoWaitRetryHandler(policy, null);
    int[] requests = {0};

    try {
      handler.executeWithRetry((Callable<Void>) () -> {
        if (requests[0]++ % 2 == 0) {
          throw statusError(429);
        }
        throw new IOException("network failure");
      });
      fail("Alternating errors must not refresh the default retry budget");
    } catch (IOException expected) {
      assertEquals(8, requests[0]);
    }

    assertEquals(Arrays.asList(1, 2, 3, 4), policy.getAttempts());
    assertEquals(Arrays.asList(1, 2, 3, 4), handler.throttling.attempts);
  }

  @Test
  public void testServerErrorsShareTheirOwnBudgetAfterThrottling() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(3);
    NoWaitRetryHandler handler = new NoWaitRetryHandler(policy, null);
    int[] requests = {0};
    int[] statuses = {500, 502, 503, 504};

    try {
      handler.executeWithRetry((Callable<Void>) () -> {
        int request = requests[0]++;
        throw statusError(request < 100 ? 429 : statuses[(request - 100) % statuses.length]);
      });
      fail("Expected the eighth server error to exhaust the server retry budget");
    } catch (TunnelException expected) {
      assertEquals(504, expected.getStatus().intValue());
    }

    assertEquals(108, requests[0]);
    assertTrue(policy.getAttempts().isEmpty());
    assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), handler.serverErrors.attempts);
    assertEquals(Arrays.asList(1000L, 2000L, 4000L, 8000L, 16000L, 32000L, 64000L),
        handler.serverErrors.waits);
  }

  @Test
  public void testSlotReassignmentAndThrottlingKeepTheirSharedBackoff() throws Exception {
    NoWaitRetryHandler handler = new NoWaitRetryHandler(new RecordingRetryPolicy(0), null);
    int[] requests = {0};
    handler.executeWithRetry((Callable<Void>) () -> {
      int request = requests[0]++;
      if (request < 8) {
        throw statusError(request % 2 == 0 ? 308 : 429);
      }
      return null;
    });

    assertEquals(9, requests[0]);
    assertEquals(Arrays.asList(1000L, 2000L, 4000L, 8000L, 16000L, 32000L, 64000L, 64000L),
        handler.throttling.waits);
  }

  @Test
  public void testLocalErrorDoesNotInvokeStatusCodeHandler() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(1);
    NoWaitRetryHandler handler = new NoWaitRetryHandler(policy, null);
    List<Integer> handledStatuses = new ArrayList<>();
    int[] requests = {0};

    handler.executeWithRetry(ctx -> {
      switch (requests[0]++) {
        case 0:
          throw statusError(429);
        case 1:
          throw new TunnelException("network failure");
        default:
          return null;
      }
    }, handledStatuses::add);

    assertEquals(Arrays.asList(429), handledStatuses);
    assertEquals(Arrays.asList(1), policy.getAttempts());
  }

  @Test
  public void testAllDefaultErrorsShareBudgetAcrossServerErrors() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(3);
    NoWaitRetryHandler handler = new NoWaitRetryHandler(policy, null);
    Exception[] errors = {new IOException("network"), statusError(500), statusError(400),
        statusError(503), new TunnelException("local error"), new IOException("network again")};
    int[] requests = {0};
    try {
      handler.executeWithRetry((Callable<Void>) () -> {
        throw errors[requests[0]++];
      });
      fail("All errors using the default policy must share its retry budget");
    } catch (IOException expected) {
      assertSame(errors[5], expected);
    }

    assertEquals(6, requests[0]);
    assertEquals(Arrays.asList(1, 2, 3, 4), policy.getAttempts());
    assertEquals(Arrays.asList(1, 2), handler.serverErrors.attempts);
  }

  @Test
  public void testLegacyOnFailurePreservesCallerSuppliedAttempts() {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(3);
    TunnelRetryHandler handler = new TunnelRetryHandler(policy, null);
    assertTrue(handler.onFailure(new IOException("retry"), 3));
    assertFalse(handler.onFailure(new IOException("stop"), 4));
    assertEquals(Arrays.asList(3, 4), policy.getAttempts());
    assertEquals(Arrays.asList(3), policy.waitAttempts);
  }

  @Test
  public void testOverriddenPolicySharesBudgetAcrossHttpGroups() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(1);
    TunnelRetryHandler handler = new TunnelRetryHandler(policy, null) {
      @Override
      protected RetryPolicy getRetryPolicy(Integer status) {
        return defaultRetryPolicy;
      }
    };
    IOException localError = new IOException("connect timed out");
    int[] requests = {0};
    try {
      handler.executeWithRetry((Callable<Void>) () -> {
        switch (requests[0]++) {
          case 0:
            throw statusError(500);
          case 1:
            throw localError;
          default:
            return null;
        }
      });
      fail("The same selected policy must not get a second retry budget");
    } catch (IOException expected) {
      assertSame(localError, expected);
    }
    assertEquals(2, requests[0]);
    assertEquals(Arrays.asList(1, 2), policy.getAttempts());
  }

  @Test
  public void testDistinctInstancesOfSamePolicyClassHaveIndependentBudgets() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(1);
    RecordingRetryPolicy throttling = new RecordingRetryPolicy(1);
    TunnelRetryHandler handler = new TunnelRetryHandler(policy, null) {
      @Override
      protected RetryPolicy getRetryPolicy(Integer status) {
        return status != null && status == 429 ? throttling : defaultRetryPolicy;
      }
    };
    IOException localError = new IOException("connect timed out");
    int[] requests = {0};
    try {
      handler.executeWithRetry((Callable<Void>) () -> {
        switch (requests[0]++) {
          case 0:
            throw statusError(429);
          case 1:
            throw statusError(500);
          case 2:
            throw localError;
          default:
            return null;
        }
      });
      fail("Only the selected policy's budget may be consumed");
    } catch (IOException expected) {
      assertSame(localError, expected);
    }
    assertEquals(3, requests[0]);
    assertEquals(Arrays.asList(1), throttling.getAttempts());
    assertEquals(Arrays.asList(1, 2), policy.getAttempts());
  }

  @Test
  public void testStatefulOnFailureUsesTheSelectedPolicyBudget() {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(1);
    TunnelRetryHandler handler = new TunnelRetryHandler(policy, null) {
      @Override
      protected RetryPolicy getRetryPolicy(Integer status) {
        return defaultRetryPolicy;
      }
    };
    TunnelRetryHandler.RetryState state = new TunnelRetryHandler.RetryState();
    assertTrue(handler.onFailure(statusError(500), 0, state));
    assertFalse(handler.onFailure(new IOException("connect timed out"), 1, state));
    assertEquals(Arrays.asList(1, 2), policy.getAttempts());
  }

  @Test
  public void testInterruptedWaitStopsWithoutAnotherRequest() throws Exception {
    IOException failure = new IOException("network failure");
    TunnelRetryHandler handler = new TunnelRetryHandler(new TunnelRetryHandler.RetryPolicy() {
      @Override
      public boolean shouldRetry(Exception e, int attempt) {
        return true;
      }

      @Override
      public long getRetryWaitTime(int attempt) {
        return 0;
      }

      @Override
      public void waitForNextRetry(int attempt) throws InterruptedException {
        throw new InterruptedException("cancel retry");
      }
    }, null);
    int[] requests = {0};
    try {
      handler.executeWithRetry((Callable<Void>) () -> {
        requests[0]++;
        throw failure;
      });
      fail("Interrupted retry must propagate the original failure");
    } catch (IOException expected) {
      assertSame(failure, expected);
      assertEquals(1, requests[0]);
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testSequentialOperationsOnSameHandlerHaveIndependentBudgets() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(1);
    TunnelRetryHandler handler = new TunnelRetryHandler(policy, null);
    for (int operation = 0; operation < 2; operation++) {
      handler.executeWithRetry(ctx -> {
        if (ctx.getRetryIndex() == 0) {
          throw new IOException("retry once");
        }
        return null;
      });
    }

    assertEquals(Arrays.asList(1, 1), policy.getAttempts());
  }

  @Test(timeout = 10000)
  public void testConcurrentOperationsOnSameHandlerHaveIndependentBudgets() throws Exception {
    RecordingRetryPolicy policy = new RecordingRetryPolicy(2);
    NoWaitRetryHandler handler = new NoWaitRetryHandler(policy, null);
    CyclicBarrier barrier = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Callable<String> operation = () -> handler.executeWithRetry(ctx -> {
      if (ctx.getRetryIndex() < 100) {
        throw statusError(429);
      }
      if (ctx.getRetryIndex() == 100) {
        barrier.await(5, TimeUnit.SECONDS);
      }
      if (ctx.getRetryIndex() < 102) {
        throw new IOException("network failure");
      }
      return ctx.getTraceId();
    });

    try {
      Future<String> first = executor.submit(operation);
      Future<String> second = executor.submit(operation);
      assertTrue(!first.get(5, TimeUnit.SECONDS).equals(second.get(5, TimeUnit.SECONDS)));
      List<Integer> attempts = new ArrayList<>(policy.getAttempts());
      Collections.sort(attempts);
      assertEquals(Arrays.asList(1, 1, 2, 2), attempts);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static TunnelException statusError(int status) {
    TunnelException error = new TunnelException("HTTP " + status);
    error.setStatus(status);
    return error;
  }

  private static final class NoWaitRetryHandler extends TunnelRetryHandler {
    private final NoWaitPolicy throttling;
    private final NoWaitPolicy serverErrors;

    private NoWaitRetryHandler(RetryPolicy policy, RestClient.RetryLogger logger) {
      super(policy, logger);
      throttling = new NoWaitPolicy(super.getRetryPolicy(429));
      serverErrors = new NoWaitPolicy(super.getRetryPolicy(500));
    }

    @Override
    protected RetryPolicy getRetryPolicy(Integer status) {
      if (status != null && (status == 429 || status == 308)) {
        return throttling;
      }
      if (status != null && status >= 500 && status < 600) {
        return serverErrors;
      }
      return super.getRetryPolicy(status);
    }
  }

  private static final class NoWaitPolicy implements TunnelRetryHandler.RetryPolicy {
    private final TunnelRetryHandler.RetryPolicy delegate;
    private final List<Integer> attempts = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> waits = Collections.synchronizedList(new ArrayList<>());

    private NoWaitPolicy(TunnelRetryHandler.RetryPolicy delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean shouldRetry(Exception e, int attempt) {
      attempts.add(attempt);
      return delegate.shouldRetry(e, attempt);
    }

    @Override
    public long getRetryWaitTime(int attempt) {
      return delegate.getRetryWaitTime(attempt);
    }

    @Override
    public void waitForNextRetry(int attempt) {
      waits.add(getRetryWaitTime(attempt));
    }
  }

  private static final class RecordingRetryPolicy implements TunnelRetryHandler.RetryPolicy {

    private final int retryLimit;
    private final List<Integer> attempts = Collections.synchronizedList(new ArrayList<>());
    private final List<Integer> waitAttempts = Collections.synchronizedList(new ArrayList<>());
    private final List<Integer> waitTimeAttempts = Collections.synchronizedList(new ArrayList<>());

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
      waitTimeAttempts.add(attempt);
      return attempt * 10L;
    }

    @Override
    public void waitForNextRetry(int attempt) {
      // Tests deliberately avoid the production exponential backoff.
      waitAttempts.add(attempt);
    }

    private List<Integer> getAttempts() {
      return attempts;
    }
  }
}
