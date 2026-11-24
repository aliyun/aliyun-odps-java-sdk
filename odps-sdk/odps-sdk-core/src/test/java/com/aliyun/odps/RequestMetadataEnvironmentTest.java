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

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.Headers;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class RequestMetadataEnvironmentTest {
  private static final String RAW_CONTEXT = "  not-json / 会话  ";
  private static final String RAW_PARENT = "external-parent";
  private static final String RAW_STATE = "external state";
  @Test
  public void environmentIsAutomaticWithoutCallingAnyNewSetter() throws Exception {
    JsonObject output = probe("env");
    assertTrue(output.get("ua").getAsString().endsWith(RequestMetadataTest.SUFFIX));
    assertEquals("application", output.get("base").getAsString());
    assertEquals(RAW_PARENT, output.get("parent").getAsString());
    assertEquals(RAW_STATE, output.get("state").getAsString());
    assertEquals(RAW_CONTEXT,
        RequestMetadataTest.decodeBusiness(output.get("call").getAsString()));
  }

  @Test
  public void explicitClearingAndCloningNeverReloadsEnvironment() throws Exception {
    JsonObject output = probe("clear-clone");
    assertFalse(output.has("call"));
    assertFalse(output.has("parent"));
    assertFalse(output.has("state"));
    assertFalse(output.get("ua").getAsString().contains("agentic/1"));
  }

  @Test
  public void apiOverridesWholeEnvironmentBusinessObject() throws Exception {
    JsonObject output = probe("override");
    JsonObject call = JsonParser.parseString(
        RequestMetadataTest.decodeBusiness(output.get("call").getAsString())).getAsJsonObject();
    JsonObject extensions = call.getAsJsonObject("extensions");
    assertEquals("api-session", extensions.get("session_id").getAsString());
    assertEquals(1, extensions.size());
    assertEquals(RAW_PARENT, output.get("parent").getAsString());
    assertEquals(RAW_STATE, output.get("state").getAsString());
  }

  private JsonObject probe(String mode) throws Exception {
    String cp =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    ProcessBuilder builder =
        new ProcessBuilder(
            System.getProperty("java.home") + "/bin/java", "-cp", cp, Probe.class.getName(), mode);
    builder.redirectErrorStream(true);
    builder.environment().put("MC_USER_AGENT_SUFFIX", RequestMetadataTest.SUFFIX);
    builder.environment().put("MC_CALL_CONTEXT", RAW_CONTEXT);
    builder.environment().put("TRACEPARENT", RAW_PARENT);
    builder.environment().put("TRACESTATE", RAW_STATE);
    Process process = builder.start();
    if (!process.waitFor(15, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      fail("probe timeout");
    }
    String result = null;
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("PROBE:")) result = line.substring(6);
      }
    }
    assertEquals("probe failed", 0, process.exitValue());
    assertNotNull("probe did not emit selected headers", result);
    return JsonParser.parseString(result).getAsJsonObject();
  }

  public static class Probe {
    public static void main(String[] args) {
      Odps odps = new Odps(new AliyunAccount("dummy-id", "dummy-key"));
      odps.setEndpoint("http://localhost");
      odps.setUserAgent("application");
      if (args[0].equals("clear-clone")) {
        odps.options().setCallContext(null);
        odps.options().setTraceContext(null);
        odps.options().setUserAgentSuffix(null);
        odps = odps.clone();
      } else if (args[0].equals("override")) {
        odps.options()
            .setCallContext(java.util.Collections.singletonMap("session_id", "api-session"));
      }
      Map<String, String> headers =
          odps.getRestClient().buildRequest("/projects/project", "GET", null, null).getHeaders();
      Map<String, String> selected = new HashMap<>();
      selected.put("base", odps.getUserAgent());
      selected.put("ua", headers.get(Headers.USER_AGENT));
      selected.put("call", headers.get(RequestMetadataTest.CALL));
      selected.put("parent", headers.get("traceparent"));
      selected.put("state", headers.get("tracestate"));
      System.out.println("PROBE:" + new Gson().toJson(selected));
    }
  }
}
