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

import java.util.concurrent.Callable;
import java.util.function.IntConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.storage.ServiceException;

/**
 * Retry handler for executing operations with automatic retry logic.
 * <p>
 * This handler provides retry capabilities based on HTTP status codes:
 * <ul>
 *   <li>429 (Too Many Requests): infinite exponential retry with jitter</li>
 *   <li>502, 503, 504, 500: limited exponential retry (7 times) with jitter</li>
 *   <li>408 (Request Timeout): limited exponential retry (7 times) with jitter</li>
 *   <li>Other 4xx: no retry</li>
 *   <li>Other 5xx: limited exponential retry (7 times) with jitter</li>
 * </ul>
 * <p>
 * All retry strategies include jitter to prevent thundering herd problems when
 * multiple clients retry simultaneously.
 */
public class RetryHandler {

  private static final Logger log = LoggerFactory.getLogger(RetryHandler.class);

  private final RetryPolicy defaultRetryPolicy;
  private final RetryLogger retryLogger;

  public RetryHandler() {
    this(NoRetryPolicy.INSTANCE, null);
  }

  public RetryHandler(RetryPolicy defaultRetryPolicy, RetryLogger retryLogger) {
    this.defaultRetryPolicy = defaultRetryPolicy != null ? defaultRetryPolicy : NoRetryPolicy.INSTANCE;
    this.retryLogger = retryLogger;
  }

  public RetryHandler(RetryLogger retryLogger) {
    this(NoRetryPolicy.INSTANCE, retryLogger);
  }

  /**
   * Executes an action with retry logic.
   *
   * @param action The action to execute
   * @param <T> The return type of the action
   * @return The result of the action
   * @throws Exception if the action fails after all retries
   */
  public <T> T executeWithRetry(Callable<T> action) throws Exception {
    return executeWithRetry(action, null);
  }

  /**
   * Executes an action with retry logic and error code handler.
   *
   * @param action The action to execute
   * @param errorCodeHandler Handler for error codes (called before each retry)
   * @param <T> The return type of the action
   * @return The result of the action
   * @throws Exception if the action fails after all retries
   */
  public <T> T executeWithRetry(Callable<T> action, IntConsumer errorCodeHandler)
      throws Exception {
    int attempt = 1;
    long startTime = 0;
    while (true) {
      try {
        startTime = System.currentTimeMillis();
        return action.call();
      } catch (Exception e) {
        RetryPolicy policy = getRetryPolicy(e);
        
        if (errorCodeHandler != null && e instanceof ServiceException) {
          errorCodeHandler.accept(((ServiceException) e).getHttpStatus());
        }
        
        if (!policy.shouldRetry(e, attempt)) {
          throw e;
        }
        logRetryAttempt(e, attempt, policy, System.currentTimeMillis() - startTime);
        if (retryLogger != null) {
          retryLogger.onRetryLog(e, attempt, policy.getRetryWaitTime(attempt));
        }
        
        try {
          policy.waitForNextRetry(attempt);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
        
        attempt++;
      }
    }
  }

  private void logRetryAttempt(Exception e, int attempt, RetryPolicy policy, long failTime) {
    int httpStatus = 0;
    String errorCode = "N/A";
    String requestId = "N/A";
    if (e instanceof ServiceException) {
      httpStatus = ((ServiceException) e).getHttpStatus();
      errorCode = ((ServiceException) e).getErrorCode();
      requestId = ((ServiceException) e).getRequestId();
    }
    long waitTimeMs = policy.getRetryWaitTime(attempt);

    log.warn("Request failed (attempt {}), will retry after {}ms. HTTP: {}, Error: {}, RequestId: {}, Waste {}ms",
             attempt, waitTimeMs, httpStatus, errorCode, requestId, failTime + waitTimeMs);
  }

  private String getPolicyName(RetryPolicy policy) {
    if (policy instanceof NoRetryPolicy) {
      return "NoRetry";
    } else if (policy instanceof ExponentialBackoffPolicy) {
      return "ExponentialBackoff";
    } else if (policy instanceof InfiniteExponentialBackoffPolicy) {
      return "InfiniteExponentialBackoff";
    } else {
      return policy.getClass().getSimpleName();
    }
  }

  /**
   * Determines the retry policy based on the exception.
   *
   * @param e The exception
   * @return The appropriate retry policy
   */
  protected RetryPolicy getRetryPolicy(Exception e) {
    if (e instanceof ServiceException) {
      return getRetryPolicy(((ServiceException) e).getHttpStatus());
    }
    return defaultRetryPolicy;
  }

  /**
   * Determines the retry policy based on the HTTP status code.
   *
   * @param statusCode The HTTP status code
   * @return The appropriate retry policy
   */
  protected RetryPolicy getRetryPolicy(int statusCode) {
    switch (statusCode) {
      case 429:
        return InfiniteExponentialBackoffPolicy.INSTANCE;
      case 408:
      case 500:
      case 502:
      case 503:
      case 504:
        return ExponentialBackoffPolicy.INSTANCE;
      default:
        if (statusCode >= 500 && statusCode < 600) {
          return ExponentialBackoffPolicy.INSTANCE;
        }
        return defaultRetryPolicy;
    }
  }

  /**
   * Functional interface for logging retry attempts.
   */
  @FunctionalInterface
  public interface RetryLogger {
    /**
     * Called when a retry attempt is about to be made.
     *
     * @param e The exception that caused the retry
     * @param attempt The current attempt number (1-based)
     * @param waitTimeMs The wait time before the next retry in milliseconds
     */
    void onRetryLog(Exception e, int attempt, long waitTimeMs);
  }
}