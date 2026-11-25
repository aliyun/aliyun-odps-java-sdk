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

import java.util.IdentityHashMap;
import java.util.Map;
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
    private final boolean legacyOnFailureOverridden;

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
        this.legacyOnFailureOverridden = hasLegacyOnFailureOverride();
    }

    public boolean onFailure(Exception e, int attempt) {
        return handleFailure(e, attempt, null);
    }

    private boolean hasLegacyOnFailureOverride() {
        try {
            return getClass().getMethod("onFailure", Exception.class, int.class)
                .getDeclaringClass() != TunnelRetryHandler.class;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Missing legacy retry callback", e);
        }
    }

    /**
     * Handles a failure with counters belonging to one retry operation.
     * Existing two-argument overrides retain control and receive the caller's original attempt.
     *
     * @param attempt overall retry count used for logging, with the caller's existing indexing
     * @param retryState operation-local state; {@code null} preserves the legacy attempt semantics
     */
    public boolean onFailure(Exception e, int attempt, RetryState retryState) {
        if (legacyOnFailureOverridden) {
            return onFailure(e, attempt);
        }
        return handleFailure(e, attempt, retryState);
    }

    private boolean handleFailure(Exception e, int attempt, RetryState retryState) {
        RetryPolicy policy;
        if (e instanceof TunnelException) {
            policy = getRetryPolicy(((TunnelException) e).getStatus());
        } else {
            policy = defaultRetryPolicy;
        }
        int policyAttempt = retryState == null ? attempt : retryState.nextAttempt(policy);
        if (policy.shouldRetry(e, policyAttempt)) {
            if (retryLogger != null) {
                retryLogger.onRetryLog(e, attempt, policy.getRetryWaitTime(policyAttempt));
            }
            try {
                policy.waitForNextRetry(policyAttempt);
            } catch (InterruptedException i) {
                Thread.currentThread().interrupt();
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Selects the retry policy for an HTTP status code.
     *
     * <p>Managed retry loops count failures and compute backoff separately for each returned
     * policy instance, using object identity rather than {@link Object#equals(Object)}.
     * Overrides must return the same instance for errors that share a retry budget. Returning
     * a new policy or wrapper on every call starts a new counter at one and may prevent the
     * retry limit or backoff progression from taking effect.</p>
     *
     * <p>Reuse policy instances stored in handler fields, including wrappers around policies
     * returned by {@code super.getRetryPolicy(errorCode)}. Normal customization through
     * {@link Configuration.Builder#withRetryPolicy(TunnelRetryHandler.RetryPolicy)} supplies
     * a fixed default policy and does not require overriding this method.</p>
     *
     * @param errorCode HTTP status code, or {@code null} for the default policy
     * @return the policy instance for the selected retry group
     */
    protected RetryPolicy getRetryPolicy(Integer errorCode) {
        if (errorCode == null) {
            return defaultRetryPolicy;
        }
        if (errorCode == HttpStatus.SLOT_REASSIGNMENT || errorCode == HttpStatus.FLOW_EXCEEDED) {
            return InfiniteExponentialWaitRetryPolicy.INSTANCE;
        } else if (HttpStatus.isServerError(errorCode)) {
            return ExponentialWaitRetryPolicy.INSTANCE;
        }
        return defaultRetryPolicy;
    }

    /**
     * Retry counters for a single operation, or a single bucket in one Upsert flush.
     *
     * <p>Counters follow the selected policy's identity, including subclass policy mappings.
     * Policies must be reused for errors that share a retry budget. This state
     * is confined to the retry loop's calling thread and must not be shared between independent
     * operations. Policies and handlers do not retain it.</p>
     */
    public static final class RetryState {
        private final Map<RetryPolicy, Integer> attempts = new IdentityHashMap<>();

        private int nextAttempt(RetryPolicy policy) {
            Integer previous = attempts.get(policy);
            int attempt = previous == null ? 1
                : previous == Integer.MAX_VALUE ? previous : previous + 1;
            attempts.put(policy, attempt);
            return attempt;
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
     * increasing zero-based request indexes. Retry policies receive a one-based attempt number
     * within their own retry group. The supplied context represents the first actual request and
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
        RetryState retryState = new RetryState();
        int attempt = 1;
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
                if (errorCodeHandler != null && e instanceof TunnelException
                    && ((TunnelException) e).getStatus() != null) {
                    errorCodeHandler.accept(((TunnelException) e).getStatus());
                }
                int policyAttempt = retryState.nextAttempt(policy);
                if (!policy.shouldRetry(e, policyAttempt)) {
                    throw e;
                }
                if (retryLogger != null) {
                    retryLogger.onRetryLog(
                        e, attempt, policy.getRetryWaitTime(policyAttempt));
                }
                try {
                    policy.waitForNextRetry(policyAttempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                context = context.next();
                attempt++;
            }
        }
    }

    @FunctionalInterface
    public interface RetryableAction<T> {
        /** Executes one actual request with its zero-based retry context. */
        T call(RetryContext ctx) throws Exception;
    }

    public interface RetryPolicy {

        /** Managed retry loops pass a one-based attempt within the selected retry group. */
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
