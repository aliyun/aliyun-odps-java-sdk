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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.storage.MaxStorageClient;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.impl.v2.V2ClientHolder;
import com.aliyun.odps.tunnel.Configuration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StorageAgentTraceTableApiTest {
  private MockWebServer server;
  private TableIdentifier table;

  @Before
  public void start() throws Exception {
    server = new MockWebServer();
    server.start();
    table = TableIdentifier.of("project", "default", "table");
  }

  @After
  public void stop() throws Exception {
    server.shutdown();
  }

  private EnvironmentSettings.Builder settings() {
    String url = server.url("/").toString();
    return EnvironmentSettings.newBuilder().inRemoteMode()
        .withCredentials(Credentials.newBuilder()
            .withAccount(new AliyunAccount("test-ak", "test-sk")).build())
        .withDefaultProject("project").withServiceEndpoint(url).withTunnelEndpoint(url);
  }

  @Test
  public void tableApiV1AppliesOptionsBeforeSigningAndSupportsClear() {
    EnvironmentSettings settings = settings().withUserAgentSuffix("table-agent/1")
        .withTraceContext("table-parent", "table-state").build();
    Map<String, String> headers = ExecutionEnvironment.create(settings)
        .createHttpClient("project").buildRequest("/projects/project", "GET", null, null)
        .getHeaders();
    assertEquals("table-parent", headers.get("traceparent"));
    assertEquals("table-state", headers.get("tracestate"));
    assertTrue(headers.get(Headers.USER_AGENT).endsWith(" table-agent/1"));
    assertTrue(headers.get(Headers.ODPS_USER_AGENT).endsWith(" table-agent/1"));
    assertNotNull(headers.get(Headers.AUTHORIZATION));
    assertFalse(headers.containsKey("x-odps-call-context"));

    EnvironmentSettings cleared =
        settings().withUserAgentSuffix(null).withTraceContext(null).build();
    headers = ExecutionEnvironment.create(cleared).createHttpClient("project")
        .buildRequest("/projects/project", "GET", null, null).getHeaders();
    assertFalse(headers.containsKey("traceparent"));
    assertFalse(headers.containsKey("tracestate"));
    assertFalse(headers.get(Headers.USER_AGENT).contains("table-agent/1"));
  }

  @Test
  public void tableApiV2HolderCarriesTheSameSettings() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(400)
        .setBody("{\"Code\":\"Probe\",\"Message\":\"expected\"}"));
    EnvironmentSettings settings = settings().withUserAgentSuffix("table-agent/1")
        .withTraceContext("table-parent", "table-state").build();
    try (V2ClientHolder holder = new V2ClientHolder(table, settings)) {
      try {
        holder.stub().preview(table, null, null, 1);
        fail("expected mock response");
      } catch (ServiceException expected) {
        // Expected after the local server records the request.
      }
    }
    RecordedRequest request = server.takeRequest(2, TimeUnit.SECONDS);
    assertNotNull(request);
    assertEquals("table-parent", request.getHeader("traceparent"));
    assertEquals("table-state", request.getHeader("tracestate"));
    assertTrue(request.getHeader("User-Agent").endsWith(" table-agent/1"));
    assertNull(request.getHeader("x-odps-call-context"));
  }

  @Test
  public void odpsConstructedStorageClientInheritsOptionsWithoutCallContext() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(400)
        .setBody("{\"Code\":\"Probe\",\"Message\":\"expected\"}"));
    String url = server.url("/").toString();
    Odps odps = new Odps(new AliyunAccount("test-ak", "test-sk"));
    odps.setEndpoint(url);
    odps.setTunnelEndpoint(url);
    odps.options().setUserAgentSuffix("odps-agent/1");
    odps.options().setTraceContext("odps-parent", "odps-state");
    odps.options().setCallContextJson("business-context");
    try (MaxStorageClient client = new Configuration(odps).newStorageClient("project")) {
      try {
        client.previewTable(table, null, null, 1);
        fail("expected mock response");
      } catch (ServiceException expected) {
        // Expected after the local server records the request.
      }
    }
    RecordedRequest request = server.takeRequest(2, TimeUnit.SECONDS);
    assertNotNull(request);
    assertEquals("odps-parent", request.getHeader("traceparent"));
    assertEquals("odps-state", request.getHeader("tracestate"));
    assertTrue(request.getHeader("User-Agent").endsWith(" odps-agent/1"));
    assertNull(request.getHeader("x-odps-call-context"));
  }
}
