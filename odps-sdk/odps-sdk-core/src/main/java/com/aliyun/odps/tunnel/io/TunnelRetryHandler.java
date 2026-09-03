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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import com.aliyun.odps.commons.transport.HttpStatus;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.retry.RetryContext;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * retry strategy for table tunnel
 * 429: Server-side current limiting strategy, infinite exponential retry (max 64s)
 * 4xx: Client-side error, default not retry
 * 502, 504: should trigger reload session and retry exponential (1s - 64s)
 * 5xx: Server-side error,  retry exponential (1s - 64s)
 * 308: only upsert session flush use, update slot map and infinite exponential retry (max 64s)
 */
public class TunnelRetryHandler {

    protected RetryPolicy defaultRetryPolicy;
    private RestClient.RetryLogger retryLogger;

    public TunnelRetryHandler() {
        this(NoRetryPolicy.INSTANCE, null);
    }

    public TunnelRetryHandler(Configuration configuration) {
        this(configuration.getRetryPolicy(), configuration.getRetryLogger());
    }

    public TunnelRetryHandler(RetryPolicy defaultRetryPolicy, RestClient.RetryLogger retryLogger) {
        if (defaultRetryPolicy == null) {
            this.defaultRetryPolicy = NoRetryPolicy.INSTANCE;
        } else {
            this.defaultRetryPolicy = defaultRetryPolicy;
        }
        this.retryLogger = retryLogger;
    }

    public boolean onFailure(Exception e, int attempt) {
        RetryPolicy policy;
        if (e instanceof TunnelException) {
            policy = getRetryPolicy(((TunnelException) e).getStatus());
        } else {
            policy = defaultRetryPolicy;
        }
        if (policy.shouldRetry(e, attempt)) {
            if (retryLogger != null) {
                retryLogger.onRetryLog(e, attempt, policy.getRetryWaitTime(attempt));
            }
            try {
                policy.waitForNextRetry(attempt);
            } catch (InterruptedException i) {
                Thread.currentThread().interrupt();
            }
            return true;
        } else {
            return false;
        }
    }

    protected RetryPolicy getRetryPolicy(Integer errorCode) {
        if (errorCode == null) {
            return defaultRetryPolicy;
        }
        if (errorCode == HttpStatus.SLOT_REASSIGNMENT || errorCode == HttpStatus.FLOW_EXCEEDED) {
            return InfiniteExponentialWaitRetryPolicy.INSTANCE;
        } else if (HttpStatus.isServerError(errorCode)) {
            return ExponentialWaitRetryPolicy.INSTANCE;
        } else {
            return defaultRetryPolicy;
        }
    }

    public <T> T executeWithRetry(Callable<T> action) throws Exception {
        return executeWithRetry(action, null);
    }

    public <T> T executeWithRetry(Callable<T> action, IntConsumer errorCodeHandler)
        throws Exception {
        return executeWithRetry(ctx -> action.call(), errorCodeHandler);
    }

    public <T> T executeWithRetry(RetryableAction<T> action) throws Exception {
        return executeWithRetry(action, null);
    }

    public <T> T executeWithRetry(RetryableAction<T> action,
                                  IntConsumer errorCodeHandler)
        throws Exception {
        return executeWithRetry(RetryContext.create(), action, errorCodeHandler);
    }

    /**
     * Executes an action using an existing retry sequence.
     *
     * <p>This overload allows nested retry loops to share one trace ID and monotonically
     * increasing zero-based request indexes. Retry policies still receive their historical
     * one-based attempt number. The supplied context represents the first actual request and
     * must not concurrently start another retry loop.</p>
     */
    public <T> T executeWithRetry(RetryContext initialContext,
                                  RetryableAction<T> action) throws Exception {
        return executeWithRetry(initialContext, action, null);
    }

    public <T> T executeWithRetry(RetryContext initialContext,
                                  RetryableAction<T> action,
                                  IntConsumer errorCodeHandler) throws Exception {
        RetryContext context = initialContext;
        int policyAttempt = 1;
        while (true) {
            try {
                return action.call(context);
            } catch (Exception e) {
                RetryPolicy policy;
                if (e instanceof TunnelException) {
                    policy = getRetryPolicy(((TunnelException) e).getStatus());
                } else {
                    policy = defaultRetryPolicy;
                }
                if (errorCodeHandler != null && e instanceof TunnelException) {
                    errorCodeHandler.accept(((TunnelException) e).getStatus());
                }
                if (!policy.shouldRetry(e, policyAttempt)) {
                    throw e;
                }
                if (retryLogger != null) {
                    retryLogger.onRetryLog(
                        e, policyAttempt, policy.getRetryWaitTime(policyAttempt));
                }
                try {
                    policy.waitForNextRetry(policyAttempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                context = context.next();
                policyAttempt++;
            }
        }
    }

    @FunctionalInterface
    public interface RetryableAction<T> {
        /** Executes one actual request with its zero-based retry context. */
        T call(RetryContext ctx) throws Exception;
    }

    public interface RetryPolicy {

        boolean shouldRetry(Exception e, int attempt);

        long getRetryWaitTime(int attempt);

        default void waitForNextRetry(int attempt) throws InterruptedException {
            TimeUnit.MILLISECONDS.sleep(getRetryWaitTime(attempt));
        }
    }

    static class NoRetryPolicy implements RetryPolicy {

        static final NoRetryPolicy INSTANCE = new NoRetryPolicy();

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
            // No wait needed
        }
    }

    static class InfiniteExponentialWaitRetryPolicy implements RetryPolicy {

        static final InfiniteExponentialWaitRetryPolicy
            INSTANCE =
            new InfiniteExponentialWaitRetryPolicy();

        @Override
        public boolean shouldRetry(Exception e, int attempt) {
            return true;
        }

        @Override
        public long getRetryWaitTime(int attempt) {
            if (attempt < 7) {
                int waitTime = (int) Math.pow(2, attempt - 1);
                return TimeUnit.SECONDS.toMillis(waitTime);
            } else {
                return TimeUnit.SECONDS.toMillis(64);
            }
        }
    }

    static class ExponentialWaitRetryPolicy implements RetryPolicy {

        static final ExponentialWaitRetryPolicy INSTANCE = new ExponentialWaitRetryPolicy();

        @Override
        public boolean shouldRetry(Exception e, int attempt) {
            return attempt <= 7; // max retry 7 t 7 times, 1s, 2s, 4s, 8s, 16s, 32s, 64s
        }

        @Override
        public long getRetryWaitTime(int attempt) {
            int waitTime = (int) Math.pow(2, attempt - 1);
            return TimeUnit.SECONDS.toMillis(waitTime);
        }
    }

}
