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

import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;

public class TableRetryHandler extends TunnelRetryHandler {

    public TableRetryHandler(RestClient restClient) {
        super(new TableRetryPolicy(restClient), restClient.getRetryLogger());
    }

    static class TableRetryPolicy implements TunnelRetryHandler.RetryPolicy {

        private final int maxRetryTimes;
        private final long retryWaitTimeInMills;

        public TableRetryPolicy(RestClient restClient) {
            this.maxRetryTimes = restClient.getRetryTimes();
            this.retryWaitTimeInMills = (restClient.getRetryWaitTime() > 0 ?
                    restClient.getRetryWaitTime() :
                    restClient.getConnectTimeout() + restClient.getReadTimeout()) * 1000L;
        }

        @Override
        public boolean shouldRetry(Exception e, int attempt) {
            if (ExceptionUtils.findThrowableWithMessage(e, "connect timed out").isPresent()
                    && attempt <= maxRetryTimes) {
                return true;
            }
            return false;
        }

        @Override
        public long getRetryWaitTime(int attempt) {
            return retryWaitTimeInMills;
        }
    }
}
