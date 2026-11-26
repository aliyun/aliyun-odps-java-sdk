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

package com.aliyun.odps.table.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.ProxyCredential;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;

/**
 * What {@code OkHttpConnection} puts on the wire when a proxy demands authentication. Everything is
 * local (MockWebServer plays the proxy), no cloud identity: the assertion is about the header.
 *
 * <p>The case that matters is a POST whose body is streamed. The body is handed to the call as it
 * is written, so by the time the proxy has answered anything, the bytes are gone: a follow-up
 * request produced from the same call cannot carry them again. That is why this egress has to
 * attach {@code Proxy-Authorization} to the first attempt (like the netty Upsert path does) instead
 * of only answering a challenge, and why the counter-case below still expects a single unanswered
 * {@code 407} when no credential was configured.
 */
public class OkHttpProxyAuthTest {

  private static final String USER = "tony/proxy";
  private static final String PASSWORD = "test-proxy-password";

  private static RestClient clientWithProxy(MockWebServer server, ProxyCredential credential) {
    return clientWithProxy(server, credential, true);
  }

  private static RestClient clientWithProxy(MockWebServer server, ProxyCredential credential,
                                            boolean explicitWriteTimeout) {
    com.aliyun.odps.table.configuration.RestOptions.Builder options =
        com.aliyun.odps.table.configuration.RestOptions.newBuilder().withRetryTimes(0);
    if (explicitWriteTimeout) {
      options.withWriteTimeout(5);
    }
    EnvironmentSettings settings = EnvironmentSettings.newBuilder()
        .inRemoteMode()
        .withCredentials(new Credentials.Builder()
            .withAccount(new AliyunAccount("access-id", "access-key"))
            .build())
        .withServiceEndpoint(server.url("/").toString())
        .withTunnelEndpoint(server.url("/").toString())
        .withRestOptions(options.build())
        .build();
    RestClient restClient = ExecutionEnvironment.create(settings).createHttpClient("a_project");
    restClient.setProxy(new Proxy(Proxy.Type.HTTP,
        new InetSocketAddress("127.0.0.1", server.getPort())));
    if (credential != null) {
      restClient.setProxyCredential(credential);
    }
    return restClient;
  }

  @Test
  public void credentialedProxyWorksWithoutExplicitWriteTimeout() throws Exception {
    MockWebServer proxy = new MockWebServer();
    proxy.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));
    proxy.start();
    try {
      RestClient client = clientWithProxy(proxy, new ProxyCredential(USER, PASSWORD), false);
      Request request = new Request(client);
      request.setURI(URI.create(proxy.url("/read").toString()));
      request.setMethod(Request.Method.GET);

      Response response = client.getTransport().request(request);
      RecordedRequest recorded = proxy.takeRequest(2, TimeUnit.SECONDS);

      assertNotNull(recorded);
      assertEquals("Basic " + java.util.Base64.getEncoder().encodeToString(
              (USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)),
          recorded.getHeader("Proxy-Authorization"));
      assertTrue(response.isOK());
    } finally {
      proxy.shutdown();
    }
  }

  private static Request postRequest(RestClient restClient, MockWebServer server) {
    Request request = new Request(restClient);
    request.setURI(URI.create(server.url("/upload").toString()));
    request.setMethod(Request.Method.POST);
    request.setHeader(Headers.CONTENT_TYPE, "application/octet-stream");
    request.setHeader(Headers.TRANSFER_ENCODING, Headers.CHUNKED);
    return request;
  }

  @Test
  public void streamingPostCarriesTheCredentialOnTheFirstAttempt() throws Exception {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200).setBody("proxy-auth-answer"));
    server.start();

    RestClient restClient = clientWithProxy(server, new ProxyCredential(USER, PASSWORD));
    Response response;
    String body;
    try {
      com.aliyun.odps.commons.transport.Connection connection =
          restClient.getTransport().connect(postRequest(restClient, server));
      try (OutputStream out = connection.getOutputStream()) {
        out.write("arrow-payload".getBytes(StandardCharsets.UTF_8));
      }
      response = connection.getResponse();
      java.io.InputStream in = connection.getInputStream();
      java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
      byte[] chunk = new byte[512];
      int read;
      while ((read = in.read(chunk)) > 0) {
        buffer.write(chunk, 0, read);
      }
      body = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
      connection.disconnect();
    } finally {
      server.shutdown();
    }

    RecordedRequest recorded = server.takeRequest(2, TimeUnit.SECONDS);
    assertNotNull("the request must have reached the proxy", recorded);
    Assert.assertTrue("a streaming POST cannot wait for a 407 to learn the credential: the body is "
            + "already gone by then, so the header must be on the first attempt. Seen: "
            + recorded.getHeader("Proxy-Authorization"),
        recorded.getHeader("Proxy-Authorization") != null);
    assertEquals("Basic " + java.util.Base64.getEncoder().encodeToString(
            (USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)),
        recorded.getHeader("Proxy-Authorization"));
    assertEquals(1, server.getRequestCount());
    assertTrue("the answer must be usable", response.isOK());
    assertEquals("proxy-auth-answer", body);
  }

  @Test
  public void withoutCredentialTheChallengeIsSurfacedNotAnswered() throws Exception {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(407)
        .addHeader("Proxy-Authenticate", "Basic realm=\"mce2e\""));
    server.start();

    RestClient restClient = clientWithProxy(server, null);
    int status;
    try {
      com.aliyun.odps.commons.transport.Connection connection =
          restClient.getTransport().connect(postRequest(restClient, server));
      try (OutputStream out = connection.getOutputStream()) {
        out.write("arrow-payload".getBytes(StandardCharsets.UTF_8));
      }
      status = connection.getResponse().getStatus();
      connection.disconnect();
    } finally {
      server.shutdown();
    }

    assertEquals("with no credential there is nothing to answer the 407 with", 407, status);
    assertEquals("and no second attempt may be made", 1, server.getRequestCount());
  }
}
