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

package com.aliyun.odps.table.enviroment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Test;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.transport.TableTransport;
import com.aliyun.odps.rest.RestClient;

/**
 * Offline check of what {@code EnvironmentSettings.withHttpProxy/withHttpsProxy} actually does to
 * the clients this module builds. No network and no cloud identity are involved: the assertion is
 * about the object graph, and the behaviour it locks in is the one {@code OkHttpConnection} reads.
 *
 * <p>Before the proxy settings were wired, {@code RestClient.getProxy()} stayed {@code null} no
 * matter what the caller configured, so {@code OkHttpConnection} never installed a
 * {@code proxyAuthenticator}: an authenticating proxy could not be used from this client at all.
 */
public class ExecutionEnvironmentProxyWiringTest {

  private static final String PASSWORD = "test-proxy-password";

  private static EnvironmentSettings.Builder settings() {
    return EnvironmentSettings.newBuilder()
        .withCredentials(new Credentials.Builder()
            .withAccount(new AliyunAccount("access", "secret"))
            .build())
        .withDefaultProject("a_project")
        .withServiceEndpoint("http://service.example.com/api")
        .withTunnelEndpoint("http://tunnel.example.com");
  }

  private static RestClient clientWithEndpoint(String endpoint) {
    RestClient restClient = new RestClient(new TableTransport(null));
    restClient.setEndpoint(endpoint);
    return restClient;
  }

  @Test
  public void noProxyConfiguredKeepsTheClientUntouched() {
    ExecutionEnvironment env = new LocalEnvironment(settings().build());
    RestClient restClient = clientWithEndpoint("http://tunnel.example.com");

    env.applyProxy(restClient);

    assertNull("default must not install a proxy", restClient.getProxy());
    assertNull("default must not install a credential", restClient.getProxyCredential());
  }

  @Test
  public void userinfoCredentialReachesTheClient() {
    ExecutionEnvironment env = new LocalEnvironment(settings()
        .withHttpProxy("http://tony%2Fproxy:" + PASSWORD + "@127.0.0.1:18911")
        .build());
    RestClient restClient = clientWithEndpoint("http://tunnel.example.com");

    env.applyProxy(restClient);

    Proxy proxy = restClient.getProxy();
    assertNotNull("the configured proxy must be installed", proxy);
    assertEquals(Proxy.Type.HTTP, proxy.type());
    assertEquals("127.0.0.1", ((InetSocketAddress) proxy.address()).getHostString());
    assertEquals(18911, ((InetSocketAddress) proxy.address()).getPort());
    assertNotNull("the parsed credential must travel with it", restClient.getProxyCredential());
    assertEquals("tony/proxy", restClient.getProxyCredential().getUsername());
    assertEquals(PASSWORD, restClient.getProxyCredential().getPassword());
    assertTrue("the credential must not be printed in the clear",
        !String.valueOf(restClient.getProxyCredential()).contains(PASSWORD));
  }

  @Test
  public void addressOnlyProxyInstallsNoCredential() {
    ExecutionEnvironment env = new LocalEnvironment(settings()
        .withHttpProxy("http://127.0.0.1:18911")
        .build());
    RestClient restClient = clientWithEndpoint("http://tunnel.example.com");

    env.applyProxy(restClient);

    assertNotNull("the address itself is still a proxy", restClient.getProxy());
    assertNull("no credential may be invented", restClient.getProxyCredential());
  }

  @Test
  public void httpsEndpointUsesTheHttpsProxy() {
    ExecutionEnvironment env = new LocalEnvironment(settings()
        .withHttpProxy("http://http-user:http-pass@127.0.0.1:18911")
        .withHttpsProxy("http://https-user:https-pass@127.0.0.1:18912")
        .build());

    RestClient http = clientWithEndpoint("http://tunnel.example.com");
    RestClient https = clientWithEndpoint("https://tunnel.example.com");
    env.applyProxy(http);
    env.applyProxy(https);

    assertNotNull(http.getProxyCredential());
    assertNotNull(https.getProxyCredential());
    assertEquals("http-user", http.getProxyCredential().getUsername());
    assertEquals("https-user", https.getProxyCredential().getUsername());
    assertEquals(18911, ((InetSocketAddress) http.getProxy().address()).getPort());
    assertEquals(18912, ((InetSocketAddress) https.getProxy().address()).getPort());
  }

  @Test
  public void onlyHttpsProxyConfiguredDoesNotProxyAnHttpEndpointClient() {
    // The two settings are independent on purpose: a caller that only ever talks to an https
    // service configures one string. Nothing may be applied to an http client that was not asked.
    ExecutionEnvironment env = new LocalEnvironment(settings()
        .withHttpsProxy("http://127.0.0.1:18912")
        .build());

    RestClient https = clientWithEndpoint("https://tunnel.example.com");
    env.applyProxy(https);
    assertNotNull(https.getProxy());

    RestClient http = clientWithEndpoint("http://tunnel.example.com");
    env.applyProxy(http);
    assertNull("an http client must not silently inherit the https proxy", http.getProxy());
  }

  @Test
  public void tunnelDiscoveryUsesAuthenticatedProxyWithoutAWriteTimeout() throws Exception {
    MockWebServer proxy = new MockWebServer();
    proxy.enqueue(new MockResponse().setResponseCode(200).setBody("tunnel.example.com:1234"));
    proxy.start();
    try {
      EnvironmentSettings settings = EnvironmentSettings.newBuilder()
          .inRemoteMode()
          .withCredentials(new Credentials.Builder()
              .withAccount(new AliyunAccount("access", "secret")).build())
          .withServiceEndpoint("http://service.example.com")
          .withHttpProxy("http://proxy-user:proxy-pass@127.0.0.1:" + proxy.getPort())
          .build();

      RestClient client = ExecutionEnvironment.create(settings).createHttpClient("a_project");
      RecordedRequest discovery = proxy.takeRequest(2, TimeUnit.SECONDS);

      assertNotNull("tunnel endpoint discovery must reach the configured proxy", discovery);
      assertEquals("Basic cHJveHktdXNlcjpwcm94eS1wYXNz",
          discovery.getHeader("Proxy-Authorization"));
      assertEquals("http://tunnel.example.com:1234", client.getEndpoint());
    } finally {
      proxy.shutdown();
    }
  }
}
