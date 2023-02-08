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

package com.aliyun.odps;

import com.aliyun.odps.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

public class TunnelEndpointLocalCache {
    private long cacheSize = 10000;
    private int durationSeconds = 900;
    private Cache<String, String> tunnelCache = null;

    public TunnelEndpointLocalCache(long size, int duration)
    {
        cacheSize = size;
        durationSeconds = duration;
        tunnelCache = CacheBuilder.newBuilder()
                .expireAfterAccess(durationSeconds, TimeUnit.SECONDS)
                .maximumSize(cacheSize).// cache size
                build();
    }

    public String getTunnelEndpointFromLocalCache(Odps odps, String tunnelQuotaName) throws ExecutionException
    {
        String key = odps.getEndpoint() + odps.getDefaultProject();
        return tunnelCache.get(key, new Callable<String>() {
            public String call() throws Exception {
                String routerEndpoint = odps.projects().get(odps.getDefaultProject()).getTunnelEndpoint(tunnelQuotaName);
                return routerEndpoint;
            }
        });
    }

    public void putTunnelEndpointIntoLocalCache(Odps odps, String tunnelEndpoint)
    {
        String key = odps.getEndpoint() + odps.getDefaultProject();
        tunnelCache.put(key,tunnelEndpoint);
    }

    public Cache<String, String> getTunnelCache()
    {
        return tunnelCache;
    }
}