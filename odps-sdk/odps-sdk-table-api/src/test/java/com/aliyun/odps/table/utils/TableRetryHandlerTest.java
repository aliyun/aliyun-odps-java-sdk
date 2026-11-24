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

package com.aliyun.odps.table.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.TunnelException;

public class TableRetryHandlerTest {

  @Test
  public void testServerAndLocalTimeoutsShareConfiguredRetryLimit() throws Exception {
    assertSharedTimeoutBudget(false);
  }

  @Test
  public void testLocalAndServerTimeoutsShareConfiguredRetryLimit() throws Exception {
    assertSharedTimeoutBudget(true);
  }

  private void assertSharedTimeoutBudget(boolean localFirst) throws Exception {
    NoWaitHandler handler = new NoWaitHandler();
    Exception first = localFirst ? new IOException("connect timed out") : statusError(500);
    Exception second = localFirst ? statusError(503) : new IOException("connect timed out");
    int[] requests = {0};
    try {
      handler.executeWithRetry((Callable<Void>) () -> {
        switch (requests[0]++) {
          case 0:
            throw first;
          case 1:
            throw second;
          default:
            return null;
        }
      });
      fail("retryTimes=1 must stop on the second timeout, regardless of HTTP status");
    } catch (Exception expected) {
      assertSame(second, expected);
    }
    assertEquals(2, requests[0]);
    assertEquals(Arrays.asList(1, 2), handler.defaultAttempts);
  }

  @Test
  public void testThrottlingAndTimeoutsKeepSeparatePolicyBudgets() throws Exception {
    NoWaitHandler handler = new NoWaitHandler();
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
      fail("The second default-policy timeout must exhaust its retry budget");
    } catch (IOException expected) {
      assertSame(localError, expected);
    }
    assertEquals(3, requests[0]);
    assertEquals(Arrays.asList(1), handler.throttlingAttempts);
    assertEquals(Arrays.asList(1, 2), handler.defaultAttempts);
  }

  private static TunnelException statusError(int status) {
    TunnelException error = new TunnelException("connect timed out");
    error.setStatus(status);
    return error;
  }

  private static RestClient newRestClient() {
    RestClient client = new Odps(new AliyunAccount("test-id", "test-key")).getRestClient();
    client.setRetryTimes(1);
    return client;
  }

  private static final class NoWaitHandler extends TableRetryHandler {
    private final List<Integer> defaultAttempts = new ArrayList<>();
    private final List<Integer> throttlingAttempts = new ArrayList<>();
    private RetryPolicy throttling;

    private NoWaitHandler() {
      super(newRestClient());
      defaultRetryPolicy = recordingPolicy(defaultRetryPolicy, defaultAttempts);
    }

    @Override
    protected RetryPolicy getRetryPolicy(Integer status) {
      RetryPolicy selected = super.getRetryPolicy(status);
      if (selected == defaultRetryPolicy) {
        return selected;
      }
      if (throttling == null) {
        throttling = recordingPolicy(selected, throttlingAttempts);
      }
      return throttling;
    }

    private RetryPolicy recordingPolicy(RetryPolicy delegate, List<Integer> attempts) {
      return new RetryPolicy() {
        @Override
        public boolean shouldRetry(Exception error, int attempt) {
          attempts.add(attempt);
          return delegate.shouldRetry(error, attempt);
        }

        @Override
        public long getRetryWaitTime(int attempt) {
          return 0;
        }
      };
    }
  }
}
