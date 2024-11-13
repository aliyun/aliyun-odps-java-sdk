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

package com.aliyun.odps.table.configuration;

import java.io.Serializable;
import java.util.Optional;

public class RestOptions implements Serializable {

    private String userAgent;

    private Integer connectTimeout;

    private Integer readTimeout;

    private Integer retryTimes;

    private Boolean ignoreCerts;

    private Long asyncIntervalInMills;

    private Integer asyncTimeoutInSeconds;

    private Integer upsertConcurrentNum;

    private Integer upsertNetworkNum;

    private Integer retryWaitTimeInSeconds;

    private RestOptions() {
    }

    public static Builder newBuilder() {
        return new RestOptions.Builder();
    }

    public Optional<Integer> getReadTimeout() {
        return Optional.ofNullable(readTimeout);
    }

    public Optional<Integer> getConnectTimeout() {
        return Optional.ofNullable(connectTimeout);
    }

    public Optional<String> getUserAgent() {
        return Optional.ofNullable(userAgent);
    }

    public Optional<Integer> getRetryTimes() {
        return Optional.ofNullable(retryTimes);
    }

    public Optional<Boolean> isIgnoreCerts() {
        return Optional.ofNullable(ignoreCerts);
    }

    public Optional<Integer> getUpsertConcurrentNum() {
        return Optional.ofNullable(upsertConcurrentNum);
    }

    public Optional<Integer> getUpsertNetworkNum() {
        return Optional.ofNullable(upsertNetworkNum);
    }

    public Optional<Long> getAsyncIntervalInMills() {
        return Optional.ofNullable(asyncIntervalInMills);
    }

    public Optional<Integer> getAsyncTimeoutInSeconds() {
        return Optional.ofNullable(asyncTimeoutInSeconds);
    }

    public Optional<Integer> getRetryWaitTimeInSeconds() {
        return Optional.ofNullable(retryWaitTimeInSeconds);
    }

    public static class Builder {

        private final RestOptions restOptions;

        public Builder() {
            restOptions = new RestOptions();
        }

        public Builder witUserAgent(String userAgent) {
            this.restOptions.userAgent = userAgent;
            return this;
        }

        public Builder withConnectTimeout(int connectTimeout) {
            this.restOptions.connectTimeout = connectTimeout;
            return this;
        }

        public Builder withReadTimeout(int readTimeout) {
            this.restOptions.readTimeout = readTimeout;
            return this;
        }

        public Builder withRetryTimes(int retryTimes) {
            this.restOptions.retryTimes = retryTimes;
            return this;
        }

        public Builder withIgnoreCerts(boolean ignoreCerts) {
            this.restOptions.ignoreCerts = ignoreCerts;
            return this;
        }

        public Builder withAsyncIntervalInMillis(long asyncIntervalInMills) {
            this.restOptions.asyncIntervalInMills = asyncIntervalInMills;
            return this;
        }

        public Builder withAsyncTimeoutInSeconds(int asyncTimeoutInSeconds) {
            this.restOptions.asyncTimeoutInSeconds = asyncTimeoutInSeconds;
            return this;
        }

        public Builder withUpsertConcurrentNum(int upsertConcurrentNum) {
            this.restOptions.upsertConcurrentNum = upsertConcurrentNum;
            return this;
        }

        public Builder withUpsertNetworkNum(int upsertNetworkNum) {
            this.restOptions.upsertNetworkNum = upsertNetworkNum;
            return this;
        }

        public Builder withRetryWaitTimeInSeconds(int retryWaitTimeInSeconds) {
            this.restOptions.retryWaitTimeInSeconds = retryWaitTimeInSeconds;
            return this;
        }

        public RestOptions build() {
            return this.restOptions;
        }
    }
}
