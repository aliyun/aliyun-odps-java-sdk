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

package com.aliyun.odps.storage.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.retry.RetryHeaders;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.internal.retry.RetryHandler;
import com.aliyun.odps.storage.internal.retry.RetryPolicy;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import okhttp3.RequestBody;

class HttpClientRetryHeadersTest {

  private final List<CapturedRequest> requests =
      Collections.synchronizedList(new ArrayList<CapturedRequest>());
  private final AtomicInteger metadataAttempts = new AtomicInteger();
  private final AtomicInteger downloadAttempts = new AtomicInteger();
  private HttpServer server;
  private HttpClient client;
  private String endpoint;

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
    endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
    client = new HttpClient(newSettings(endpoint, retryOnceWithoutWaiting()));
  }

  @AfterEach
  void tearDown() {
    if (client != null) {
      client.shutdown();
    }
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void metadataRetrySendsStableTraceAndIncreasingIndex() {
    Map<String, String> callerHeaders = new HashMap<>();
    callerHeaders.put("test-header", "unchanged");

    HttpResponse response = client.request(
        "metadata", "POST", Collections.emptyMap(), callerHeaders, "{}");

    assertEquals("ok", response.getBody());
    assertEquals(Collections.singletonMap("test-header", "unchanged"), callerHeaders);
    assertRetrySequence("/metadata", 2);
  }

  @Test
  void downloadRetrySendsStableTraceAndIncreasingIndex() throws IOException {
    Map<String, String> callerHeaders = new HashMap<>();
    callerHeaders.put("test-header", "unchanged");

    HttpResponse response = client.streamDownload(
        "download", "POST", Collections.emptyMap(), callerHeaders, "{}");

    assertNotNull(response.getInputStream());
    response.getInputStream().close();
    assertEquals(Collections.singletonMap("test-header", "unchanged"), callerHeaders);
    assertRetrySequence("/download", 2);
  }

  @Test
  void directUploadSendsInitialRetryContextWithoutMutatingHeaders() {
    Map<String, String> callerHeaders = new HashMap<>();
    callerHeaders.put("test-header", "unchanged");

    client.streamUpload(
        "upload", "POST", Collections.emptyMap(), callerHeaders,
        RequestBody.create(new byte[] {1, 2, 3}, HttpClient.OCTET_STREAM));

    assertEquals(Collections.singletonMap("test-header", "unchanged"), callerHeaders);
    List<CapturedRequest> uploads = requestsFor("/upload");
    assertEquals(1, uploads.size());
    assertNotNull(uploads.get(0).traceId);
    assertFalse(uploads.get(0).traceId.isEmpty());
    assertEquals("0", uploads.get(0).retryIndex);
  }

  @Test
  void endpointDiscoverySendsInitialRetryContext() {
    HttpClient discoveryClient = null;
    try {
      discoveryClient = new HttpClient(newSettings(null, retryOnceWithoutWaiting()));
      List<CapturedRequest> endpointRequests = requestsFor("/projects/project/tunnel");
      assertEquals(1, endpointRequests.size());
      assertNotNull(endpointRequests.get(0).traceId);
      assertFalse(endpointRequests.get(0).traceId.isEmpty());
      assertEquals("0", endpointRequests.get(0).retryIndex);
    } finally {
      if (discoveryClient != null) {
        discoveryClient.shutdown();
      }
    }
  }

  private StubSettings newSettings(String tunnelEndpoint, RetryHandler retryHandler) {
    ICredentials credentials = mock(ICredentials.class);
    when(credentials.getAccessKeyId()).thenReturn("test-ak");
    when(credentials.getAccessKeySecret()).thenReturn("test-sk");
    when(credentials.getSecurityToken()).thenReturn(null);

    ICredentialsProvider provider = mock(ICredentialsProvider.class);
    when(provider.getCredentials()).thenReturn(credentials);

    return StubSettings.newBuilder()
        .withEndpoint(endpoint)
        .withTunnelEndpoint(tunnelEndpoint)
        .withProject("project")
        .withCredentialsProvider(provider)
        .withRetryHandler(retryHandler)
        .withHttpSettings(HttpSettings.newBuilder().build())
        .build();
  }

  private RetryHandler retryOnceWithoutWaiting() {
    RetryPolicy retryOnce = new RetryPolicy() {
      @Override
      public boolean shouldRetry(Exception exception, int attempt) {
        return attempt == 1;
      }

      @Override
      public long getRetryWaitTime(int attempt) {
        return 0;
      }
    };
    return new RetryHandler() {
      @Override
      protected RetryPolicy getRetryPolicy(Exception exception) {
        return retryOnce;
      }
    };
  }

  private void assertRetrySequence(String path, int expectedRequests) {
    List<CapturedRequest> captured = requestsFor(path);
    assertEquals(expectedRequests, captured.size());
    String traceId = captured.get(0).traceId;
    assertNotNull(traceId);
    assertFalse(traceId.isEmpty());
    assertEquals(traceId, captured.get(1).traceId);
    assertEquals("0", captured.get(0).retryIndex);
    assertEquals("1", captured.get(1).retryIndex);
  }

  private List<CapturedRequest> requestsFor(String path) {
    synchronized (requests) {
      return requests.stream()
          .filter(request -> path.equals(request.path))
          .collect(Collectors.toList());
    }
  }

  private void handle(HttpExchange exchange) throws IOException {
    requests.add(new CapturedRequest(
        exchange.getRequestURI().getPath(),
        exchange.getRequestHeaders().getFirst(RetryHeaders.TRACE_ID),
        exchange.getRequestHeaders().getFirst(RetryHeaders.RETRY_INDEX)));
    exchange.getRequestBody().close();

    String path = exchange.getRequestURI().getPath();
    int status = 200;
    String response = "ok";
    if ("/metadata".equals(path) && metadataAttempts.getAndIncrement() == 0) {
      status = 502;
      response = "{\"Code\":\"retry\",\"Message\":\"retry\"}";
    } else if ("/download".equals(path) && downloadAttempts.getAndIncrement() == 0) {
      status = 502;
      response = "{\"Code\":\"retry\",\"Message\":\"retry\"}";
    } else if ("/projects/project/tunnel".equals(path)) {
      response = "127.0.0.1:" + server.getAddress().getPort();
    }

    byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private static final class CapturedRequest {

    private final String path;
    private final String traceId;
    private final String retryIndex;

    private CapturedRequest(String path, String traceId, String retryIndex) {
      this.path = path;
      this.traceId = traceId;
      this.retryIndex = retryIndex;
    }
  }
}
