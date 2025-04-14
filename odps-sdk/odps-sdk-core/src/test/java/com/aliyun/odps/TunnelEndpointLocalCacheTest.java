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

import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.ApsaraAccount;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class TunnelEndpointLocalCacheTest  extends TestBase {
    private final static String endPoint = "http://service.odps.aliyun.com/api";
    private final static String project1 = "localCacheTestProject1";
    private final static String project2 = "localCacheTestProject2";
    private final static String project3 = "localCacheTestProject3";
    private final static String project4 = "localCacheTestProject4";
    private final static String project5 = "localCacheTestProject5";
    private final static String project6 = "localCacheTestProject6";

    private final static String tunnelEndpoint = "http://dt.cn-shanghai.maxcompute.aliyun.com";
    private final static String accessId = "this is an accessId";
    private final static String accessKey = "this is an accessKey";
    private final static long cacheSize = 5;
    private final static int duration = 5;
    private final static TunnelEndpointLocalCache cache = new TunnelEndpointLocalCache(cacheSize,duration);

    @Test
    public void testSetGetFromLocalCache() throws IOException, OdpsException {
        Account account = new ApsaraAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setDefaultProject(project1);
        odps.setEndpoint(endPoint);

        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);
        try {
            assertEquals(cache.getTunnelEndpointFromLocalCache(odps, null), tunnelEndpoint);
        }
        catch (ExecutionException e)
        {
            //should not access here
        }
    }

    @Test
    public void testSetExceedSize() throws IOException, OdpsException {
        Account account = new ApsaraAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setDefaultProject(project1);
        odps.setEndpoint(endPoint);
        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);

        odps.setDefaultProject(project2);
        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);

        odps.setDefaultProject(project3);
        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);

        odps.setDefaultProject(project4);
        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);

        odps.setDefaultProject(project5);
        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);

        odps.setDefaultProject(project6);
        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);

        odps.setDefaultProject(project3);
        assertEquals(cache.getTunnelCache().getIfPresent(odps.getEndpoint()+odps.getDefaultProject()),tunnelEndpoint);
        odps.setDefaultProject(project1);
        assertEquals(cache.getTunnelCache().getIfPresent(odps.getEndpoint()+odps.getDefaultProject()),null);
    }

    @Test
    public void testSetExpireTime() throws IOException, OdpsException {
        Account account = new ApsaraAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setDefaultProject(project1);
        odps.setEndpoint(endPoint);
        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);

        odps.setDefaultProject(project2);
        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        odps.setDefaultProject(project6);
        cache.putTunnelEndpointIntoLocalCache(odps,tunnelEndpoint);

        assertEquals(cache.getTunnelCache().getIfPresent(odps.getEndpoint()+odps.getDefaultProject()),tunnelEndpoint);
        odps.setDefaultProject(project1);
        assertEquals(cache.getTunnelCache().getIfPresent(odps.getEndpoint()+odps.getDefaultProject()),null);
    }
}