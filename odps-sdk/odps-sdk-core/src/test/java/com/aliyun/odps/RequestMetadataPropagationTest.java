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

import static org.junit.Assert.*;

import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.DefaultTransport;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.data.StorageAPIResultSet;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.table.InstanceIdentifier;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TunnelException;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class RequestMetadataPropagationTest {
  private static Odps client(RestClient client) {
    Odps odps = RequestMetadataTest.client();
    odps.client = client;
    client.setAccount(odps.getAccount());
    client.setEndpoint("http://localhost");
    client.setDefaultProject("project");
    client.disableDeprecatedLogger();
    odps.setOptions(odps.options());
    return odps;
  }

  private static void context(Odps odps, String value) {
    odps.options().setCallContext(Collections.singletonMap("session_id", value));
    odps.options().setTraceContext(RequestMetadataTest.PARENT);
  }

  @Test
  public void existingInstanceUsesCurrentClientConfigurationWithoutBindingItsLifetime()
      throws Exception {
    RecordingTransport transport = new RecordingTransport();
    Odps odps = client(new RestClient(transport));
    context(odps, "A");
    Instance instance = odps.instances().get("project", "instance");
    assertSame(odps, instance.getOdps());
    instance.getTaskInfo("task", "progress");
    context(odps, "B");
    instance.getTaskInfo("task", "progress");
    instance.stop();
    assertEquals("A", RequestMetadataTest.business(transport.headers.get(0))
        .get("session_id").getAsString());
    for (int i = 1; i < 3; i++) {
      assertEquals("B", RequestMetadataTest.business(transport.headers.get(i))
          .get("session_id").getAsString());
    }
    odps.options().setCallContext(null);
    odps.options().setTraceContext(null);
    instance.getTaskInfo("task", "progress");
    assertFalse(transport.headers.get(3).containsKey(RequestMetadataTest.CALL));
    assertFalse(transport.headers.get(3).containsKey("traceparent"));
  }

  @Test
  public void refreshedCredentialsAreSharedByInstancePollingAndNewTunnelClients() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    Odps odps = client(new RestClient(transport));
    odps.setAccount(new StsAccount("dummy-id", "dummy-key", "old-token"));
    Instance instance = odps.instances().get("project", "instance");
    instance.getOdps().setAccount(new StsAccount("dummy-id", "dummy-key", "new-token"));
    instance.getTaskInfo("task", "progress");
    assertEquals("new-token", transport.headers.get(0).get(Headers.AUTHORIZATION_STS_TOKEN));
    odps.setAccount(new StsAccount("dummy-id", "dummy-key", "latest-token"));
    instance.getTaskInfo("task", "progress");
    Configuration tunnel = new Configuration(instance.getOdps());
    tunnel.setEndpoint(URI.create("http://localhost"));
    Map<String, String> tunnelHeaders = tunnel.newRestClient("project")
        .buildRequest("/projects/project", "GET", null, null).getHeaders();
    assertEquals("latest-token", transport.headers.get(1).get(Headers.AUTHORIZATION_STS_TOKEN));
    assertEquals("latest-token", tunnelHeaders.get(Headers.AUTHORIZATION_STS_TOKEN));
  }

  @Test
  public void tunnelDataRequestsOmitBusinessContextWithoutChangingTheOdpsClient()
      throws Exception {
    Odps odps = RequestMetadataTest.client();
    context(odps, "submit");
    odps.options().setUserAgentSuffix("tunnel-test/1");

    Configuration tunnel = new Configuration(odps);
    tunnel.setEndpoint(URI.create("http://localhost:8081"));
    Map<String, String> headers = tunnel.newRestClient("project")
        .buildRequest("/projects/project/instances/instance", "GET", null, null).getHeaders();

    assertFalse(headers.containsKey(RequestMetadataTest.CALL));
    assertEquals(RequestMetadataTest.PARENT, headers.get("traceparent"));
    assertTrue(headers.get(Headers.ODPS_USER_AGENT).endsWith(" tunnel-test/1"));
    assertNotNull(headers.get(Headers.AUTHORIZATION));
    assertEquals("submit", RequestMetadataTest.business(RequestMetadataTest.headers(odps))
        .get("session_id").getAsString());
  }

  @Test
  public void streamUploadSessionDoesNotSendBusinessContext() throws Exception {
    try (HeaderServer server = new HeaderServer()) {
      Odps odps = RequestMetadataTest.client();
      context(odps, "stream");
      odps.options().setUserAgentSuffix("agent-scope/1");
      odps.setTunnelEndpoint(server.endpoint());

      try {
        odps.tableTunnel().buildStreamUploadSession("project", "table").build();
        fail("expected the loopback service to reject the request");
      } catch (TunnelException expected) {
        // The loopback service responds after capturing the request.
      }

      com.sun.net.httpserver.Headers headers = server.takeHeaders();
      assertNull(headers.getFirst(RequestMetadataTest.CALL));
      assertEquals(RequestMetadataTest.PARENT, headers.getFirst("traceparent"));
      assertTrue(headers.getFirst(Headers.USER_AGENT).endsWith(" agent-scope/1"));
      assertNotNull(headers.getFirst(Headers.AUTHORIZATION));
      assertEquals("stream", RequestMetadataTest.business(RequestMetadataTest.headers(odps))
          .get("session_id").getAsString());
    }
  }

  @Test
  public void storageResultSetInheritsOdpsTraceWithoutBusinessContext() throws Exception {
    String cp =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    List<String> command = new ArrayList<>();
    command.add(System.getProperty("java.home") + "/bin/java");
    if (!System.getProperty("java.specification.version").startsWith("1.")) {
      command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
    }
    Collections.addAll(command, "-cp", cp, StorageResultSetProbe.class.getName());
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.environment().put("MC_USER_AGENT_SUFFIX", "environment-agent/1");
    builder.environment().put("MC_CALL_CONTEXT", "environment-context");
    builder.environment().put("TRACEPARENT", "environment-parent");
    builder.environment().put("TRACESTATE", "environment=1");
    builder.redirectErrorStream(true);
    Path output = Files.createTempFile("storage-result-probe-", ".log");
    try {
      builder.redirectOutput(output.toFile());
      Process process = builder.start();
      boolean finished = process.waitFor(20, TimeUnit.SECONDS);
      if (!finished) {
        process.destroyForcibly();
        process.waitFor(5, TimeUnit.SECONDS);
        fail("Storage result-set probe timed out");
      }
      assertEquals(new String(Files.readAllBytes(output), StandardCharsets.UTF_8),
          0, process.exitValue());
    } finally {
      Files.deleteIfExists(output);
    }
  }

  public static class StorageResultSetProbe {
    public static void main(String[] args) throws Exception {
      checkStorageResultSet();
    }
  }

  private static void checkStorageResultSet() throws Exception {
    try (HeaderServer server = new HeaderServer()) {
      Odps odps = RequestMetadataTest.client();
      context(odps, "result");
      odps.options().setTraceContext(RequestMetadataTest.PARENT, "vendor=value");
      odps.options().setUserAgentSuffix("agent-scope/1");
      odps.setTunnelEndpoint(server.endpoint());

      requestStorageResultSet(odps);
      com.sun.net.httpserver.Headers headers = server.takeHeaders();
      assertNull(headers.getFirst(RequestMetadataTest.CALL));
      assertEquals(RequestMetadataTest.PARENT, headers.getFirst("traceparent"));
      assertEquals("vendor=value", headers.getFirst("tracestate"));
      assertTrue(headers.getFirst(Headers.USER_AGENT).endsWith(" agent-scope/1"));
      assertNotNull(headers.getFirst(Headers.AUTHORIZATION));
      assertEquals("result", RequestMetadataTest.business(RequestMetadataTest.headers(odps))
          .get("session_id").getAsString());

      odps.options().setUserAgentSuffix(null);
      odps.options().setTraceContext(null);
      requestStorageResultSet(odps);
      headers = server.takeHeaders();
      assertNull(headers.getFirst(RequestMetadataTest.CALL));
      assertNull(headers.getFirst("traceparent"));
      assertNull(headers.getFirst("tracestate"));
      assertFalse(headers.getFirst(Headers.USER_AGENT).contains("agent-scope/1"));
      assertFalse(headers.getFirst(Headers.USER_AGENT).contains("environment-agent/1"));
    }
  }

  private static void requestStorageResultSet(Odps odps) {
    try {
      StorageAPIResultSet.of(odps, new Configuration(odps),
          InstanceIdentifier.of("project", "instance"), false, null);
      fail("expected the loopback service to reject the request");
    } catch (ServiceException expected) {
      // The loopback service responds after capturing the request.
    }
  }

  private static class HeaderServer implements AutoCloseable {
    private final HttpServer server;
    private final BlockingQueue<com.sun.net.httpserver.Headers> requests =
        new LinkedBlockingQueue<>();

    HeaderServer() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/", exchange -> {
        try {
          com.sun.net.httpserver.Headers headers = new com.sun.net.httpserver.Headers();
          headers.putAll(exchange.getRequestHeaders());
          requests.add(headers);
          byte[] body = "{\"Code\":\"Probe\",\"Message\":\"expected\"}"
              .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(400, body.length);
          exchange.getResponseBody().write(body);
        } finally {
          exchange.close();
        }
      });
      server.start();
    }

    String endpoint() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    com.sun.net.httpserver.Headers takeHeaders() throws InterruptedException {
      com.sun.net.httpserver.Headers headers = requests.poll(5, TimeUnit.SECONDS);
      assertNotNull("expected a loopback HTTP request", headers);
      return headers;
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  @Test
  public void instanceRetainsCustomByteAndStringRequestDispatch() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    int[] calls = new int[2];
    RestClient custom = new RestClient(transport) {
      @Override
      public Response request(String resource, String method, Map<String, String> params,
                              Map<String, String> headers, byte[] body) throws OdpsException {
        calls[0]++;
        return super.request(resource, method, params, headers, body);
      }

      @Override
      public Response stringRequest(String resource, String method, Map<String, String> params,
                                    Map<String, String> headers, String body) throws OdpsException {
        calls[1]++;
        return super.stringRequest(resource, method, params, headers, body);
      }
    };
    Odps odps = client(custom);
    context(odps, "custom");
    Instance instance = odps.instances().get("project", "instance");
    instance.getTaskInfo("task", "progress");
    instance.stop();
    assertArrayEquals(new int[] {2, 1}, calls);
    for (Map<String, String> headers : transport.headers) {
      assertEquals("custom", RequestMetadataTest.business(headers).get("session_id").getAsString());
    }
  }

  @Test
  public void retryRetainsConfiguredHeadersAndDoesNotAlterCallerHeaders() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    transport.failFirst = true;
    Odps odps = client(new RestClient(transport));
    context(odps, "retry");
    odps.getRestClient().setRetryTimes(1);
    odps.getRestClient().setRetryWaitTime(1);
    Map<String, String> raw = Collections.singletonMap("x-test", "caller");
    odps.getRestClient().request("/projects/project", "GET", null, raw, null, 0);
    assertEquals(2, transport.headers.size());
    for (Map<String, String> headers : transport.headers) {
      assertEquals("retry", RequestMetadataTest.business(headers).get("session_id").getAsString());
      assertEquals(RequestMetadataTest.PARENT, headers.get("traceparent"));
      assertEquals("caller", headers.get("x-test"));
    }
    assertEquals(Collections.singletonMap("x-test", "caller"), raw);
  }

  @Test
  public void submittingSqlOnlyAddsHttpHeaders() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    Odps odps = client(new RestClient(transport));
    context(odps, "submit");
    SQLTask task = new SQLTask();
    task.setName("task");
    task.setQuery("select 1;");
    Instance instance = new Instances(odps).create(task);
    assertSame(odps, instance.getOdps());
    assertEquals("submit", RequestMetadataTest.business(transport.headers.get(0))
        .get("session_id").getAsString());
    assertFalse(transport.bodies.get(0).contains("session_id"));
    assertFalse(transport.bodies.get(0).contains(RequestMetadataTest.PARENT));
  }

  @Test
  public void rawResponseAndConnectPathsAlsoInjectHeadersBeforeTransport() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    Odps odps = client(new RestClient(transport));
    context(odps, "direct");
    odps.getRestClient().requestForRawResponse("/projects/project", "GET", null, null, null, 0);
    odps.getRestClient().connect("/projects/project", "GET", null, null);
    odps.getRestClient().connect("/projects/project", "GET", null, null, "http://localhost");
    assertEquals(3, transport.headers.size());
    for (Map<String, String> headers : transport.headers) {
      assertEquals("direct", RequestMetadataTest.business(headers).get("session_id").getAsString());
      assertNotNull(headers.get(Headers.AUTHORIZATION));
    }
  }

  private static class RecordingTransport extends DefaultTransport {
    final List<Map<String, String>> headers = new ArrayList<>();
    final List<String> bodies = new ArrayList<>();
    boolean failFirst;

    @Override
    public Connection connect(Request request) {
      headers.add(new HashMap<>(request.getHeaders()));
      return null;
    }

    @Override
    public Response request(Request request) throws IOException {
      headers.add(new HashMap<>(request.getHeaders()));
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      InputStream body = request.getBody();
      if (body != null) {
        byte[] buffer = new byte[512];
        int read;
        while ((read = body.read(buffer)) != -1) {
          bytes.write(buffer, 0, read);
        }
      }
      bodies.add(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
      boolean failure = failFirst && headers.size() == 1;
      return new Response() {{
        status = failure ? 500 : request.getMethod() == Request.Method.POST ? 201 : 200;
        body = (failure ? "<Error><Code>InternalError</Code><Message>retry</Message></Error>"
            : "ready").getBytes(StandardCharsets.UTF_8);
        headers.put(Headers.LOCATION, "/projects/project/instances/instance");
      }};
    }
  }
}
