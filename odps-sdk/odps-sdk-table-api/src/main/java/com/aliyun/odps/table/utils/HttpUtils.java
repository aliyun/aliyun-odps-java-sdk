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

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HttpUtils {

    public static Map<String, String> createCommonHeader() {
        Map<String, String> headers = new HashMap<>();
        headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));
        return headers;
    }

    public static long getAsyncIntervalInMills(EnvironmentSettings settings) {
        Optional<RestOptions> restOptions = settings.getRestOptions();
        return restOptions.map(options -> options.getAsyncIntervalInMills()
                .orElse(ConfigConstants.DEFAULT_ASYNC_INTERVAL_IN_MILLS))
                .orElse(ConfigConstants.DEFAULT_ASYNC_INTERVAL_IN_MILLS);
    }

    public static int getAsyncTimeoutInSeconds(EnvironmentSettings settings) {
        Optional<RestOptions> restOptions = settings.getRestOptions();
        return restOptions.map(options -> options.getAsyncTimeoutInSeconds()
                .orElse(ConfigConstants.DEFAULT_ASYNC_TIMEOUT_IN_SECONDS))
                .orElse(ConfigConstants.DEFAULT_ASYNC_TIMEOUT_IN_SECONDS);
    }

}
