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

import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.commons.util.backoff.BackOffStrategy;
import com.aliyun.odps.tunnel.TunnelException;

public class TunnelRetryStrategy extends RetryStrategy {
    private final static int limit = 6;
    private final static int interval = 4;

    public TunnelRetryStrategy() {
        super(limit, interval, RetryStrategy.BackoffStrategy.EXPONENTIAL_BACKOFF);
    }

    public TunnelRetryStrategy(int limit, BackOffStrategy strategy) {
        super(limit, strategy);
    }

    @Override
    protected boolean needRetry(Exception e) {
        // not retry on specific error code, current none
        return true;
    }
}
