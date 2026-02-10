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

package com.aliyun.odps.storage.internal.utils;


import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import com.aliyun.odps.ProxyConfig;

public class OkHttpProxyHelper {

  public static ProxySelector createProxySelector(ProxyConfig config) {
    final List<Proxy> httpResult = config.getProxy(ProxyConfig.Type.HTTP)
      .map(Collections::singletonList)
      .orElseGet(() -> precomputeSocksFallback(config));

    final List<Proxy> httpsResult = config.getProxy(ProxyConfig.Type.HTTPS)
      .map(Collections::singletonList)
      .orElseGet(() -> precomputeSocksFallback(config));

    final List<Proxy> defaultResult = precomputeSocksFallback(config);

    return new ProxySelector() {
      @Override
      public List<Proxy> select(URI uri) {
        String scheme = uri.getScheme();

        if (scheme == null) return defaultResult;

        switch (scheme.toLowerCase()) {
          case "https": return httpsResult;
          case "http":  return httpResult;
          default:      return defaultResult;
        }
      }

      @Override
      public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
      }
    };
  }

  private static List<Proxy> precomputeSocksFallback(ProxyConfig config) {
    return config.getProxy(ProxyConfig.Type.SOCKS5)
      .map(Collections::singletonList)
      .orElseGet(() -> config.getProxy(ProxyConfig.Type.SOCKS4)
        .map(Collections::singletonList)
        .orElse(Collections.singletonList(Proxy.NO_PROXY)));
  }
}

