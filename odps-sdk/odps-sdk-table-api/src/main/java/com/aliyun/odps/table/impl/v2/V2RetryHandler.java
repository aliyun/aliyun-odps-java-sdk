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

package com.aliyun.odps.table.impl.v2;

import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.retry.RetryHandler;
import com.aliyun.odps.storage.internal.retry.RetryPolicy;
import com.aliyun.odps.table.configuration.RestOptions;
import java.io.IOException;

/** Applies explicit table-api retry limits without retrying permanent service errors. */
final class V2RetryHandler extends RetryHandler {

  private final Integer retryTimes;
  private final Long retryWaitMillis;

  V2RetryHandler(RestOptions options) {
    retryTimes = options.getRetryTimes().orElse(null);
    retryWaitMillis =
        options.getRetryWaitTimeInSeconds().map(value -> value.longValue() * 1000).orElse(null);
    if (retryTimes != null && retryTimes < 0 || retryWaitMillis != null && retryWaitMillis < 0) {
      throw new IllegalArgumentException("Retry count and wait time must be non-negative");
    }
  }

  @Override
  protected RetryPolicy getRetryPolicy(Exception exception) {
    RetryPolicy nativePolicy = super.getRetryPolicy(exception);
    boolean serviceRetryable =
        exception instanceof ServiceException
            && (((ServiceException) exception).getHttpStatus() == 429
                || ((ServiceException) exception).getHttpStatus() == 408
                || ((ServiceException) exception).getHttpStatus() >= 500
                    && ((ServiceException) exception).getHttpStatus() < 600);
    boolean ioFailure = !(exception instanceof ServiceException) && hasIoCause(exception);
    return new RetryPolicy() {
      @Override
      public boolean shouldRetry(Exception error, int attempt) {
        if (retryTimes != null) {
          return attempt <= retryTimes && (serviceRetryable || ioFailure);
        }
        return nativePolicy.shouldRetry(error, attempt);
      }

      @Override
      public long getRetryWaitTime(int attempt) {
        return retryWaitMillis == null ? nativePolicy.getRetryWaitTime(attempt) : retryWaitMillis;
      }
    };
  }

  private static boolean hasIoCause(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof IOException) {
        return true;
      }
    }
    return false;
  }
}
