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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.odps.credentials.StaticCredentialProvider;
import com.aliyun.odps.storage.MaxStorageClient;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.retry.RetryHandler;
import com.aliyun.odps.storage.internal.retry.RetryPolicy;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.tracing.RequestTrace;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import okhttp3.RequestBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StorageAgentTraceTest {
  private HttpServer server;
  private String endpoint;
  private final BlockingQueue<Headers> captured = new LinkedBlockingQueue<>();
  private final BlockingQueue<Integer> responses = new LinkedBlockingQueue<>();

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      try (InputStream input = exchange.getRequestBody()) {
        byte[] buffer = new byte[1024];
        while (input.read(buffer) != -1) { /* drain */ }
      }
      Headers copy = new Headers();
      copy.putAll(exchange.getRequestHeaders());
      captured.add(copy);
      byte[] body = "{\"Code\":\"Probe\",\"Message\":\"expected\"}"
          .getBytes(StandardCharsets.UTF_8);
      Integer status = responses.poll();
      exchange.sendResponseHeaders(status == null ? 400 : status, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
    endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private MaxStorageClient.Builder builder() {
    return MaxStorageClient.builder().endpoint(endpoint).tunnelEndpoint(endpoint).project("p")
        .credentialsProvider(StaticCredentialProvider.of("test-ak", "test-sk"));
  }

  private void assertTrace(Headers headers, String suffix, String parent, String state) {
    assertNotNull(headers);
    assertEquals(parent, headers.getFirst("traceparent"));
    assertEquals(state, headers.getFirst("tracestate"));
    assertTrue(headers.getFirst("User-Agent").endsWith(" " + suffix));
    assertNull(headers.getFirst("x-odps-call-context"));
    assertNotNull(headers.getFirst("Authorization"));
  }

  @Test
  void publicClientPropagatesReadWriteAndPreviewAndCanClearValues() throws Exception {
    TableIdentifier table = TableIdentifier.of("p", "default", "t");
    try (MaxStorageClient client = builder().userAgent("application")
        .userAgentSuffix("agent/1").traceContext("parent", "state").build()) {
      assertThrows(ServiceException.class,
          () -> client.createTableReadSessionBuilder(table).build());
      assertThrows(ServiceException.class, () -> client.previewTable(table, null, null, 1));
      assertThrows(ServiceException.class,
          () -> client.createTableWriteSessionBuilder(table).build());
      for (int i = 0; i < 3; i++) {
        assertTrace(captured.poll(2, TimeUnit.SECONDS), "agent/1", "parent", "state");
      }
    }
    try (MaxStorageClient client = builder().userAgentSuffix(null).traceContext(null).build()) {
      assertThrows(ServiceException.class,
          () -> client.createTableReadSessionBuilder(table).build());
      Headers headers = captured.poll(2, TimeUnit.SECONDS);
      assertNotNull(headers);
      assertNull(headers.getFirst("traceparent"));
      assertNull(headers.getFirst("tracestate"));
      assertFalse(headers.getFirst("User-Agent").contains("agent/1"));
    }
  }

  @Test
  void uploadUsesTheSameHeadersBeforeAdditionalSigning() throws Exception {
    final Map<String, String> signedHeaders = new HashMap<>();
    HttpClient http = new HttpClient(StubSettings.newBuilder().withEndpoint(endpoint)
        .withTunnelEndpoint(endpoint).withProject("p")
        .withHttpSettings(HttpSettings.newBuilder().build())
        .withCredentialsProvider(StaticCredentialProvider.of("test-ak", "test-sk"))
        .withRequestTrace(RequestTrace.of("agent/1", "parent", "state", true))
        .withAdditionalHeaderSigner(signedHeaders::putAll).build());
    try {
      assertThrows(ServiceException.class, () -> http.streamUpload("api/storage/v3", "POST",
          Collections.emptyMap(), Collections.emptyMap(),
          RequestBody.create(new byte[] {1}, HttpClient.OCTET_STREAM)));
      assertTrace(captured.poll(2, TimeUnit.SECONDS), "agent/1", "parent", "state");
      assertEquals("parent", signedHeaders.get("traceparent"));
      assertEquals("state", signedHeaders.get("tracestate"));
      assertTrue(signedHeaders.get("User-Agent").endsWith(" agent/1"));
      assertNull(signedHeaders.get("x-odps-call-context"));
    } finally {
      http.shutdown();
    }
  }

  @Test
  void traceRemainsOnRetriedRequests() throws Exception {
    responses.add(502);
    responses.add(200);
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
    RetryHandler retryHandler = new RetryHandler() {
      @Override
      protected RetryPolicy getRetryPolicy(Exception exception) {
        return retryOnce;
      }
    };
    HttpClient http = new HttpClient(StubSettings.newBuilder().withEndpoint(endpoint)
        .withTunnelEndpoint(endpoint).withProject("p")
        .withHttpSettings(HttpSettings.newBuilder().build())
        .withCredentialsProvider(StaticCredentialProvider.of("test-ak", "test-sk"))
        .withRequestTrace(RequestTrace.of("agent/1", "parent", "state", true))
        .withRetryHandler(retryHandler).build());
    try {
      http.request("api/storage/v3", "POST", Collections.emptyMap(), null, "{}");
      assertTrace(captured.poll(2, TimeUnit.SECONDS), "agent/1", "parent", "state");
      assertTrace(captured.poll(2, TimeUnit.SECONDS), "agent/1", "parent", "state");
    } finally {
      http.shutdown();
    }
  }

  @Test
  void environmentOverridesAndConflictsMatchTheCoreContract() {
    Map<String, String> env = new HashMap<>();
    env.put("MC_USER_AGENT_SUFFIX", "env/1");
    env.put("TRACEPARENT", "env-parent");
    env.put("TRACESTATE", "env-state");
    env.put("MC_CALL_CONTEXT", "never-send");
    RequestTrace initial = RequestTrace.fromEnvironment(env);
    Map<String, String> output = new HashMap<>();
    output.put("User-Agent", "base");
    initial.apply(output, null, null);
    assertEquals("base env/1", output.get("User-Agent"));
    assertEquals("env-parent", output.get("traceparent"));
    assertEquals("env-state", output.get("tracestate"));
    assertFalse(output.containsKey("x-odps-call-context"));

    output.clear();
    output.put("User-Agent", "base");
    initial.withSuffix(null).withTrace("api-parent", null).apply(output, null, null);
    assertEquals("base", output.get("User-Agent"));
    assertEquals("api-parent", output.get("traceparent"));
    assertFalse(output.containsKey("tracestate"));

    Map<String, String> raw = new HashMap<>();
    raw.put("TraceParent", "raw");
    output = new HashMap<>(raw);
    initial.apply(output, null, raw);
    assertFalse(output.containsKey("TraceParent"));
    assertFalse(output.containsKey("traceparent"));
    assertFalse(output.containsKey("tracestate"));
    output = new HashMap<>(raw);
    RequestTrace.fromEnvironment(Collections.emptyMap()).apply(output, null, raw);
    assertEquals("raw", output.get("TraceParent"));
  }

  @Test
  void defaultsAreReadFromEnvironmentInFreshProcess() throws Exception {
    String cp =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    List<String> command = new ArrayList<>();
    command.add(System.getProperty("java.home") + "/bin/java");
    if (!System.getProperty("java.specification.version").startsWith("1.")) {
      command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
    }
    Collections.addAll(command, "-cp", cp, EnvProbe.class.getName());
    ProcessBuilder child = new ProcessBuilder(command);
    child.environment().put("MC_USER_AGENT_SUFFIX", "env/1");
    child.environment().put("TRACEPARENT", "env-parent");
    child.environment().put("TRACESTATE", "env-state");
    child.environment().put("MC_CALL_CONTEXT", "never-send");
    child.redirectErrorStream(true);
    Process process = child.start();
    boolean finished = process.waitFor(15, TimeUnit.SECONDS);
    if (!finished) {
      process.destroyForcibly();
    }
    assertTrue(finished);
    assertEquals(0, process.exitValue());
  }

  public static class EnvProbe {
    public static void main(String[] args) throws Exception {
      Map<String, String> headers = new HashMap<>();
      headers.put("User-Agent", "base");
      StubSettings.newBuilder().build().getRequestTrace().apply(headers, null, null);
      if (!"base env/1".equals(headers.get("User-Agent"))
          || !"env-parent".equals(headers.get("traceparent"))
          || !"env-state".equals(headers.get("tracestate"))
          || headers.containsKey("x-odps-call-context")) {
        throw new AssertionError("environment metadata was not applied");
      }
      StorageAgentTraceTest test = new StorageAgentTraceTest();
      test.startServer();
      try {
        TableIdentifier table = TableIdentifier.of("p", "default", "t");
        try (MaxStorageClient client = test.builder().build()) {
          assertThrows(ServiceException.class,
              () -> client.createTableReadSessionBuilder(table).build());
          test.assertTrace(test.captured.poll(2, TimeUnit.SECONDS), "env/1", "env-parent",
              "env-state");
        }
        try (MaxStorageClient client = test.builder().userAgentSuffix(null).traceContext(null)
            .build()) {
          assertThrows(ServiceException.class,
              () -> client.createTableReadSessionBuilder(table).build());
          Headers cleared = test.captured.poll(2, TimeUnit.SECONDS);
          assertNotNull(cleared);
          assertFalse(cleared.getFirst("User-Agent").contains("env/1"));
          assertNull(cleared.getFirst("traceparent"));
          assertNull(cleared.getFirst("tracestate"));
        }
      } finally {
        test.stopServer();
      }
    }
  }
}
