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
import com.aliyun.odps.ProxyCredential;

import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

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

  /**
   * Builds an OkHttp proxy authenticator for the credentials carried by {@code config}.
   *
   * <p>Returns {@code null} when no HTTP/HTTPS credential is configured, so the caller can leave
   * {@code OkHttpClient.Builder.proxyAuthenticator(...)} untouched and keep the previous behaviour
   * (OkHttp's default {@code Authenticator.NONE}: a {@code 407} is surfaced to the caller instead of
   * being answered).
   *
   * <p>Selection mirrors {@link #createProxySelector(ProxyConfig)}: a request to an {@code https://}
   * URL is tunnelled by the proxy configured for {@link ProxyConfig.Type#HTTPS}, everything else by
   * {@link ProxyConfig.Type#HTTP}. Credentials are therefore never mixed between the two entries.
   *
   * @param config proxy configuration, may be null
   * @return the authenticator, or null when there is nothing to authenticate with
   */
  public static Authenticator createProxyAuthenticator(ProxyConfig config) {
    if (config == null || !config.hasProxyCredentials()) {
      return null;
    }
    final ProxyCredential httpCredential =
        config.getProxyCredential(ProxyConfig.Type.HTTP).orElse(null);
    final ProxyCredential httpsCredential =
        config.getProxyCredential(ProxyConfig.Type.HTTPS).orElse(null);
    if (httpCredential == null && httpsCredential == null) {
      return null;
    }
    return new Authenticator() {
      @Override
      public Request authenticate(Route route, Response response) {
        if (response.code() != 407) {
          // Not a proxy challenge: let the normal (server) authenticator handle it.
          return null;
        }
        Request failed = response.request();
        if (failed.header("Proxy-Authorization") != null) {
          // Our credential was already sent and refused. Retrying would loop until OkHttp gives
          // up, so surface the 407 to the caller instead.
          return null;
        }
        ProxyCredential credential = failed.url().isHttps() ? httpsCredential : httpCredential;
        if (credential == null) {
          return null;
        }
        return failed.newBuilder()
            .header("Proxy-Authorization", credential.proxyAuthorizationHeader())
            .build();
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

