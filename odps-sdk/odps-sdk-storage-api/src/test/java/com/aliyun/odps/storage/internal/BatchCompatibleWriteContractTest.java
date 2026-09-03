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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.aliyun.odps.storage.write.BatchCompatibleOptions;
import com.aliyun.odps.storage.write.BlockWriteResult;
import com.aliyun.odps.storage.write.TableBlockWriter;
import com.aliyun.odps.storage.write.TableWriteSession;
import com.aliyun.odps.storage.write.TableWriteSessionBuilder;
import com.aliyun.odps.storage.write.WriteMode;
import com.aliyun.odps.table.TableIdentifier;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

class BatchCompatibleWriteContractTest {

  private static final String SESSION_RESPONSE = "{"
      + "\"SessionId\":\"session-1\","
      + "\"SessionStatus\":\"NORMAL\","
      + "\"DataSchema\":{"
      + "\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"INT\",\"Nullable\":true}],"
      + "\"PartitionColumns\":[]},"
      + "\"MaxBlockNumber\":16,"
      + "\"EnhanceWriteCheck\":true}";

  private final List<CapturedRequest> requests =
      Collections.synchronizedList(new ArrayList<CapturedRequest>());
  private HttpServer server;
  private StorageStub storageStub;

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();

    ICredentials credentials = mock(ICredentials.class);
    when(credentials.getAccessKeyId()).thenReturn("test-ak");
    when(credentials.getAccessKeySecret()).thenReturn("test-sk");
    when(credentials.getSecurityToken()).thenReturn(null);

    ICredentialsProvider provider = mock(ICredentialsProvider.class);
    when(provider.getCredentials()).thenReturn(credentials);

    String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
    storageStub = new StorageStub(StubSettings.newBuilder()
        .withEndpoint(endpoint)
        .withTunnelEndpoint(endpoint)
        .withProject("project")
        .withCredentialsProvider(provider)
        .withHttpSettings(HttpSettings.newBuilder().build())
        .build());
  }

  @AfterEach
  void tearDown() throws IOException {
    if (storageStub != null) {
      storageStub.close();
    }
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void highLevelLifecycleUsesBatchCompatibleBlockProtocol() throws Exception {
    TableIdentifier table = TableIdentifier.of("project", "schema", "table");
    try (BufferAllocator allocator = new RootAllocator(16L * 1024 * 1024)) {
      TableWriteSession session = new TableWriteSessionBuilder(storageStub, allocator, table)
          .withWriteMode(WriteMode.BATCH_COMPATIBLE)
          .withPartition(new PartitionSpec("pt='20260812',region='cn'"))
          .withOverwrite(true)
          .withBatchCompatibleOptions(BatchCompatibleOptions.newBuilder()
              .withEnhanceWriteCheck(true)
              .withMaxFieldSize(4096)
              .withDynamicPartitionLimit(32)
              .build())
          .build();

      BlockWriteResult result;
      try (TableBlockWriter writer = session.createBlockWriter(7, 2);
           VectorSchemaRoot root = writer.createVectorSchemaRoot()) {
        root.allocateNew();
        IntVector vector = (IntVector) root.getVector("a");
        vector.setSafe(0, 10);
        vector.setSafe(1, 20);
        vector.setSafe(2, 30);
        root.setRowCount(3);
        writer.writeBatch(root);
        result = writer.commit();
      }
      session.commit(Collections.singletonList(result));

      assertEquals(7, result.getBlockNumber());
      assertEquals(2, result.getAttemptNumber());
      assertEquals(3, result.getRecordCount());
      assertRequestContract(allocator);

      TableWriteSession reloaded = new TableWriteSessionBuilder(storageStub, allocator, table)
          .withWriteMode(WriteMode.BATCH_COMPATIBLE)
          .withSessionId("session-1")
          .build();
      reloaded.abort();
    }

    CapturedRequest get = requestFor("TableGetWriteSession");
    assertCommonQuery(get, true);
    assertEquals("{}", get.utf8Body());
    assertNull(get.firstHeader(Constants.ROUTE_TOKEN_HEADER));

    CapturedRequest abort = requestFor("TableAbortWriteSession");
    assertCommonQuery(abort, true);
    assertEquals("{}", abort.utf8Body());
    assertEquals("route-get", abort.firstHeader(Constants.ROUTE_TOKEN_HEADER));
  }

  private void assertRequestContract(BufferAllocator allocator) throws IOException {
    CapturedRequest create = requestFor("TableCreateWriteSession");
    assertCommonQuery(create, false);
    JsonObject createBody = JsonParser.parseString(create.utf8Body()).getAsJsonObject();
    assertEquals("pt=20260812/region=cn", createBody.get("PartitionSpec").getAsString());
    assertTrue(createBody.get("Overwrite").getAsBoolean());
    assertEquals(4096, createBody.get("MaxFieldSize").getAsLong());
    assertTrue(createBody.get("EnhanceWriteCheck").getAsBoolean());
    assertEquals("Exception", createBody.getAsJsonObject("DynamicPartitionOptions")
        .get("InvalidStrategy").getAsString());
    assertEquals(32, createBody.getAsJsonObject("DynamicPartitionOptions")
        .get("DynamicPartitionLimit").getAsInt());
    assertNotNull(createBody.get("ArrowOptions"));
    assertFalse(create.query.containsKey("legacy"));
    assertEquals("true", create.query.get("enableQuotaToken"));

    CapturedRequest reserve = requestFor("TableCreateWriteStream");
    assertCommonQuery(reserve, true);
    assertEquals("route-create", reserve.firstHeader(Constants.ROUTE_TOKEN_HEADER));
    JsonObject reserveBody = JsonParser.parseString(reserve.utf8Body()).getAsJsonObject();
    assertEquals("block-7-attempt-2", reserveBody.get("StreamId").getAsString());
    assertEquals(1, reserveBody.get("StreamVersion").getAsLong());

    CapturedRequest write = requestFor("TableWrite");
    assertCommonQuery(write, true);
    assertEquals("7", write.query.get("BlockNumber"));
    assertEquals("2", write.query.get("AttemptNumber"));
    assertFalse(write.query.containsKey("StreamId"));
    assertFalse(write.query.containsKey("StreamVersion"));
    assertFalse(write.query.containsKey("legacy"));
    assertEquals("quota-7-2", write.query.get("quotaToken"));
    assertEquals("route-reservation", write.firstHeader(Constants.ROUTE_TOKEN_HEADER));
    assertTrue(write.firstHeader("Content-Type")
        .startsWith("application/vnd.apache.arrow.stream"));

    try (ArrowStreamReader reader = new ArrowStreamReader(
        new ByteArrayInputStream(write.body), allocator)) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(3, root.getRowCount());
      assertEquals(20, ((IntVector) root.getVector("a")).get(1));
      assertFalse(reader.loadNextBatch());
    }

    CapturedRequest commit = requestFor("TableCommitWriteSession");
    assertCommonQuery(commit, true);
    assertEquals("route-create", commit.firstHeader(Constants.ROUTE_TOKEN_HEADER));
    JsonObject commitBody = JsonParser.parseString(commit.utf8Body()).getAsJsonObject();
    assertEquals(1, commitBody.entrySet().size());
    assertEquals(1, commitBody.getAsJsonArray("CommitMessages").size());
    assertFalse(commitBody.has("StreamIds"));
    assertFalse(commitBody.has("StreamVersions"));
  }

  private void assertCommonQuery(CapturedRequest request, boolean hasSession) {
    assertEquals("/api/storage/v3", request.path);
    assertEquals("POST", request.method);
    assertEquals("projects.project.schemas.schema.tables.table", request.query.get("Target"));
    assertEquals("BatchCompatible", request.query.get("WriteMode"));
    assertEquals(hasSession, request.query.containsKey("SessionId"));
  }

  private CapturedRequest requestFor(String action) {
    synchronized (requests) {
      return requests.stream()
          .filter(request -> action.equals(request.query.get("Action")))
          .findFirst()
          .orElseThrow(() -> new AssertionError("No request captured for action " + action));
    }
  }

  private void handle(HttpExchange exchange) throws IOException {
    CapturedRequest request = new CapturedRequest(
        exchange.getRequestMethod(),
        exchange.getRequestURI(),
        exchange.getRequestHeaders(),
        readAll(exchange.getRequestBody()));
    requests.add(request);

    String action = request.query.get("Action");
    String response;
    int status = 200;
    if ("TableCreateWriteStream".equals(action)) {
      response = "{\"QuotaToken\":\"quota-7-2\"}";
      status = 201;
      exchange.getResponseHeaders().add(
          Constants.ROUTE_TOKEN_HEADER, "route-reservation");
    } else if ("TableWrite".equals(action)) {
      response = "{\"CommitMessage\":\"{\\\"BlockNumber\\\":7,"
          + "\\\"AttemptNumber\\\":2,\\\"WriterStats\\\":{\\\"RecordNum\\\":3}}\","
          + "\"RecordCount\":3}";
    } else if ("TableCommitWriteSession".equals(action)) {
      response = "{\"SessionId\":\"session-1\",\"SessionStatus\":\"COMMITTED\"}";
      status = 201;
    } else if ("TableAbortWriteSession".equals(action)) {
      response = "{}";
    } else {
      response = SESSION_RESPONSE;
      if ("TableCreateWriteSession".equals(action)) {
        exchange.getResponseHeaders().add(Constants.ROUTE_TOKEN_HEADER, "route-create");
      } else if ("TableGetWriteSession".equals(action)) {
        exchange.getResponseHeaders().add(Constants.ROUTE_TOKEN_HEADER, "route-get");
      }
    }

    byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private static byte[] readAll(InputStream input) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int read;
    while ((read = input.read(buffer)) != -1) {
      output.write(buffer, 0, read);
    }
    return output.toByteArray();
  }

  private static Map<String, String> parseQuery(String query) {
    Map<String, String> values = new LinkedHashMap<>();
    if (query == null || query.isEmpty()) {
      return values;
    }
    for (String pair : query.split("&")) {
      String[] parts = pair.split("=", 2);
      String name = decode(parts[0]);
      String value = parts.length == 2 ? decode(parts[1]) : "";
      values.put(name, value);
    }
    return values;
  }

  private static String decode(String value) {
    try {
      return URLDecoder.decode(value, "UTF-8");
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static final class CapturedRequest {

    private final String method;
    private final String path;
    private final Map<String, String> query;
    private final Map<String, List<String>> headers;
    private final byte[] body;

    private CapturedRequest(
        String method,
        URI uri,
        com.sun.net.httpserver.Headers headers,
        byte[] body) {
      this.method = method;
      this.path = uri.getPath();
      this.query = parseQuery(uri.getRawQuery());
      this.headers = new LinkedHashMap<>(headers);
      this.body = body;
    }

    private String utf8Body() {
      return new String(body, StandardCharsets.UTF_8);
    }

    private String firstHeader(String name) {
      for (Map.Entry<String, List<String>> header : headers.entrySet()) {
        if (header.getKey().equalsIgnoreCase(name)) {
          return header.getValue().isEmpty() ? null : header.getValue().get(0);
        }
      }
      return null;
    }
  }
}
