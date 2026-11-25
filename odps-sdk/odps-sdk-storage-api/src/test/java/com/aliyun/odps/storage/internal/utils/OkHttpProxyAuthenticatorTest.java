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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.ProxyCredential;

import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * The proxy authenticator the storage-api egress installs.
 *
 * <p>Written as a JUnit&nbsp;5 test on purpose: this module runs on the junit-platform provider and
 * the JUnit&nbsp;4 sibling {@code OkHttpProxyHelperTest} in the same directory is compiled but never
 * executed (no vintage engine), see the work item record for that finding.
 */
class OkHttpProxyAuthenticatorTest {

  private static Response challenge(String url, int code, String proxyAuthorization) {
    Request.Builder builder = new Request.Builder().url(url);
    if (proxyAuthorization != null) {
      builder.header("Proxy-Authorization", proxyAuthorization);
    }
    return new Response.Builder()
        .request(builder.build())
        .protocol(Protocol.HTTP_1_1)
        .code(code)
        .message(code == 407 ? "Proxy Authentication Required" : "OK")
        .body(ResponseBody.create(null, new byte[0]))
        .addHeader("Proxy-Authenticate", "Basic realm=\"odps-proxy-auth-test\"")
        .build();
  }

  @Test
  void noCredentialMeansNoAuthenticator() {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://192.0.2.1:8080")
        .withHttpsProxy("http://192.0.2.2:8443")
        .build();

    assertNull(OkHttpProxyHelper.createProxyAuthenticator(config));
    assertNull(OkHttpProxyHelper.createProxyAuthenticator(null));
    // Without credentials the client keeps OkHttp's own default, i.e. previous behaviour untouched.
    assertEquals(Authenticator.NONE, new OkHttpClient.Builder().build().proxyAuthenticator());
  }

  @Test
  void httpAndHttpsChallengesUseTheirOwnCredential() throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://u-http:p-http@192.0.2.1:8080")
        .withHttpsProxy("http://u-https:p-https@192.0.2.2:8443")
        .build();
    Authenticator authenticator = OkHttpProxyHelper.createProxyAuthenticator(config);
    assertNotNull(authenticator);

    Request http = authenticator.authenticate(null, challenge("http://odps.aliyun.com", 407, null));
    assertEquals(Credentials.basic("u-http", "p-http"), http.header("Proxy-Authorization"));

    Request https = authenticator.authenticate(null,
        challenge("https://odps.aliyun.com/api", 407, null));
    assertEquals(Credentials.basic("u-https", "p-https"), https.header("Proxy-Authorization"));

    // The same split still decides which proxy is used at all, credential or not.
    ProxySelector selector = OkHttpProxyHelper.createProxySelector(config);
    List<Proxy> httpsProxies = selector.select(new URI("https://odps.aliyun.com/api"));
    InetSocketAddress address = (InetSocketAddress) httpsProxies.get(0).address();
    assertEquals("192.0.2.2", address.getHostString());
    assertEquals(8443, address.getPort());
  }

  @Test
  void socksCredentialsNeverAnswerAnHttpChallenge() {
    ProxyConfig config = ProxyConfig.builder()
        .withSocks5Proxy("socks5://u-socks:p-socks@192.0.2.3:1080")
        .build();

    assertNull(OkHttpProxyHelper.createProxyAuthenticator(config));
  }

  @Test
  void missingCredentialForThatSchemeIsNotAnswered() throws IOException {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://u-http:p-http@192.0.2.1:8080")
        .build();
    Authenticator authenticator = OkHttpProxyHelper.createProxyAuthenticator(config);
    assertNotNull(authenticator);

    // https requests are tunnelled by the Type.HTTPS proxy, which carries no credential here: the
    // 407 must be surfaced rather than answered with the HTTP proxy's credential.
    assertNull(authenticator.authenticate(null, challenge("https://odps.aliyun.com", 407, null)));
  }

  @Test
  void alreadyAuthenticatedRequestIsNotRetried() throws IOException {
    ProxyConfig config = ProxyConfig.builder().withHttpProxy("http://u:p@192.0.2.1:8080").build();
    Authenticator authenticator = OkHttpProxyHelper.createProxyAuthenticator(config);
    assertNotNull(authenticator);

    String sent = Credentials.basic("u", "p");
    assertNull(authenticator.authenticate(null, challenge("http://odps.aliyun.com", 407, sent)));
    // Only 407 is a proxy challenge; a 401 belongs to the service authenticator.
    assertNull(authenticator.authenticate(null, challenge("http://odps.aliyun.com", 401, null)));
  }

  @Test
  void headerValueIsTheMeasuredBasicShape() {
    ProxyCredential credential = new ProxyCredential("alice", "p@ss:100%off");

    // Credentials.basic is what the probe that produced the reference matrix sent on the wire.
    assertEquals(Credentials.basic("alice", "p@ss:100%off"), credential.proxyAuthorizationHeader());
    assertEquals("Basic " + Base64.getEncoder().encodeToString(
        "alice:p@ss:100%off".getBytes(StandardCharsets.UTF_8)),
        credential.proxyAuthorizationHeader());
  }
}
