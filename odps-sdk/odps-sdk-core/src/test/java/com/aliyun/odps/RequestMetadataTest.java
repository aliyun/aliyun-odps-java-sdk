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
import com.aliyun.odps.commons.transport.Request;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

public class RequestMetadataTest {
  static final String PARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
  static final String OTHER = "00-4bf92f3577b34da6a3ce929d0e0e4737-00f067aa0ba902b8-00";
  static final String SUFFIX = "agentic/1 (source=dataworks; agent=dataagent; mode=workflow)";
  static final String CALL = "x-odps-call-context";

  static Odps client() {
    Odps odps = new Odps(new AliyunAccount("dummy-id", "dummy-key"));
    odps.setEndpoint("http://localhost");
    odps.setDefaultProject("project");
    odps.getRestClient().disableDeprecatedLogger();
    odps.options().setUserAgentSuffix(null);
    odps.options().setCallContext(null);
    odps.options().setTraceContext(null);
    return odps;
  }

  static Map<String, String> headers(Odps odps) {
    return odps.getRestClient().buildRequest("/projects/project", "GET", null, null).getHeaders();
  }

  static JsonObject business(Map<String, String> headers) {
    return JsonParser.parseString(decodeBusiness(headers.get(CALL)))
        .getAsJsonObject()
        .getAsJsonObject("extensions");
  }

  static String decodeBusiness(String value) {
    assertNotNull(value);
    assertFalse(value.contains("="));
    return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
  }

  @Test
  public void mapIsCopiedAndReplacedWithoutMerging() {
    Odps odps = client();
    Map<String, String> input = new LinkedHashMap<>();
    input.put("session_id", "A");
    input.put("dw.node", "node");
    odps.options().setCallContext(input);
    input.put("session_id", "mutated");
    assertEquals("A", business(headers(odps)).get("session_id").getAsString());
    odps.options().setCallContext(Collections.singletonMap("session_id", "B"));
    JsonObject map = business(headers(odps));
    assertEquals(1, map.size());
    assertEquals("B", map.get("session_id").getAsString());
  }

  @Test
  public void jsonAndMapEncodeTheSameUnicodeValue() {
    Odps odps = client();
    odps.options().setCallContext(Collections.singletonMap("session_id", "会话/%+"));
    String encoded = headers(odps).get(CALL);
    odps.options().setCallContextJson("{\"version\":1,\"extensions\":{\"session_id\":\"会话/%+\"}}");
    assertEquals(encoded, headers(odps).get(CALL));
    assertEquals("会话/%+", business(headers(odps)).get("session_id").getAsString());
  }

  @Test
  public void allBusinessClearFormsPreventEnvironmentFallback() {
    for (int clear = 0; clear < 4; clear++) {
      Odps odps = client();
      odps.options().requestMetadata =
          RequestMetadata.fromEnvironment(
              Collections.singletonMap(
                  "MC_CALL_CONTEXT", "{\"version\":1,\"extensions\":{\"session_id\":\"env\"}}"));
      if (clear == 0) odps.options().setCallContext(null);
      if (clear == 1) odps.options().setCallContext(Collections.emptyMap());
      if (clear == 2) odps.options().setCallContextJson(null);
      if (clear == 3) odps.options().setCallContextJson("");
      assertFalse(headers(odps).containsKey(CALL));
      assertFalse(headers(odps.clone()).containsKey(CALL));
    }
  }

  @Test
  public void traceReplacementIsAtomicAndSingleParentClearsOldState() {
    Odps odps = client();
    odps.options().setTraceContext(PARENT, "vendor=value");
    assertEquals("vendor=value", headers(odps).get("tracestate"));
    odps.options().setTraceContext(OTHER);
    assertEquals(OTHER, headers(odps).get("traceparent"));
    assertFalse(headers(odps).containsKey("tracestate"));
    odps.options().setTraceContext(null);
    assertFalse(headers(odps.clone()).containsKey("traceparent"));
  }

  @Test
  public void traceValuesArePassedThroughIndependently() {
    Odps odps = client();
    odps.options().setTraceContext(" raw-parent ", " vendor=first,vendor=second ");
    assertEquals(" raw-parent ", headers(odps).get("traceparent"));
    assertEquals(" vendor=first,vendor=second ", headers(odps).get("tracestate"));
    odps.options().setTraceContext("", "standalone-state");
    assertFalse(headers(odps).containsKey("traceparent"));
    assertEquals("standalone-state", headers(odps).get("tracestate"));
  }

  @Test
  public void jsonInputIsEncodedVerbatim() {
    Odps odps = client();
    for (String text : new String[] {
        "not JSON",
        "  \n{\"version\":2,\"extra\":1,\"extensions\":{\"key\":1,\"key\":2}}\t ",
        "{\"version\":1}",
        "   ",
        String.join("", Collections.nCopies(2100, "x"))}) {
      odps.options().setCallContextJson(text);
      assertEquals(text, decodeBusiness(headers(odps).get(CALL)));
    }
  }

  @Test
  public void suffixPreservesBaseGetterAndSingleOrDualHeaders() {
    Odps odps = client();
    odps.setUserAgent("application");
    odps.options().setUserAgentSuffix(SUFFIX);
    assertEquals("application", odps.getUserAgent());
    Map<String, String> both = headers(odps);
    assertTrue(both.get(Headers.USER_AGENT).contains("JavaSDK"));
    assertTrue(both.get(Headers.USER_AGENT).endsWith("application " + SUFFIX));
    assertEquals(both.get(Headers.USER_AGENT), both.get(Headers.ODPS_USER_AGENT));
    Map<String, String> single = new HashMap<>();
    single.put(Headers.USER_AGENT, "custom");
    Request request = odps.getRestClient().buildRequest("/projects/project", "GET", null, single);
    assertEquals("custom " + SUFFIX, request.getHeaders().get(Headers.USER_AGENT));
    assertFalse(request.getHeaders().containsKey(Headers.ODPS_USER_AGENT));
    assertEquals("custom", single.get(Headers.USER_AGENT));
    odps.options().setUserAgentSuffix(null);
    assertFalse(headers(odps).get(Headers.USER_AGENT).contains("agentic/1"));
  }

  @Test
  public void suffixIsIdempotentAndAcceptsOpaqueProductAttributes() {
    Odps odps = client();
    odps.setUserAgent(SUFFIX);
    odps.options().setUserAgentSuffix(SUFFIX);
    String value = headers(odps).get(Headers.USER_AGENT);
    assertEquals(value.indexOf(SUFFIX), value.lastIndexOf(SUFFIX));
    odps.options().setUserAgentSuffix(SUFFIX.replace("dataworks", "other"));
    assertTrue(headers(odps).get(Headers.USER_AGENT).endsWith(SUFFIX.replace("dataworks", "other")));
  }

  @Test
  public void conflictingRawHeaderAndCaseDuplicatesDoNotLeakOrMutateInputs() {
    Odps odps = client();
    odps.options().setCallContext(Collections.singletonMap("session_id", "new"));
    Map<String, String> raw = new HashMap<>();
    raw.put("X-ODPS-Call-Context", "raw-old");
    Request request = odps.getRestClient().buildRequest("/projects/project", "GET", null, raw);
    assertFalse(request.getHeaders().containsKey(CALL));
    assertFalse(request.getHeaders().containsKey("X-ODPS-Call-Context"));
    assertEquals("raw-old", raw.get("X-ODPS-Call-Context"));
    odps.options().setCallContext(null);
    assertFalse(
        odps.getRestClient()
            .buildRequest("/projects/project", "GET", null, raw)
            .getHeaders()
            .containsKey("X-ODPS-Call-Context"));
  }

  @Test
  public void businessHeaderIsPresentWhenRequestIsSigned() {
    Odps odps = client();
    odps.options().setCallContext(Collections.singletonMap("session_id", "signed"));
    Request request = odps.getRestClient().buildRequest("/projects/project", "GET", null, null);
    String authorization = request.getHeaders().get(Headers.AUTHORIZATION);
    odps.getAccount().getRequestSigner().sign("/projects/project", request);
    assertEquals(authorization, request.getHeaders().get(Headers.AUTHORIZATION));
    request.setHeader(CALL, "tampered");
    odps.getAccount().getRequestSigner().sign("/projects/project", request);
    assertNotEquals(authorization, request.getHeaders().get(Headers.AUTHORIZATION));
  }

  @Test
  public void mapEnvelopePreservesUnrestrictedExtensions() {
    Odps odps = client();
    Map<String, String> extensions = new LinkedHashMap<>();
    String value = "Unicode\u2028\u2029,\"quoted\" and \\backslash";
    extensions.put("sys.Future Key", value);
    extensions.put("empty", "");
    extensions.put("nullable", null);
    odps.options().setCallContext(extensions);

    JsonObject envelope = JsonParser.parseString(decodeBusiness(headers(odps).get(CALL)))
        .getAsJsonObject();
    assertEquals(1, envelope.get("version").getAsInt());
    JsonObject encoded = envelope.getAsJsonObject("extensions");
    assertEquals(value, encoded.get("sys.Future Key").getAsString());
    assertEquals("", encoded.get("empty").getAsString());
    assertTrue(encoded.get("nullable").isJsonNull());
  }

  @Test
  public void typedTraceConflictsSuppressBothRawHeadersWithoutAffectingBusiness() {
    Odps odps = client();
    odps.options().setTraceContext(PARENT, "vendor=value");
    odps.options().setCallContext(Collections.singletonMap("session_id", "valid"));
    Map<String, String> raw = new HashMap<>();
    raw.put("TraceParent", OTHER);
    raw.put("TraceState", "other=value");
    Map<String, String> output =
        odps.getRestClient().buildRequest("/projects/project", "GET", null, raw).getHeaders();
    assertFalse(output.containsKey("traceparent"));
    assertFalse(output.containsKey("tracestate"));
    assertFalse(output.containsKey("TraceParent"));
    assertFalse(output.containsKey("TraceState"));
    assertEquals("valid", business(output).get("session_id").getAsString());
    assertEquals(2, raw.size());
  }

  @Test
  public void stsV2V4AndAppAuthenticationSignTheFinalBusinessHeader() {
    for (String region : new String[] {null, "cn-hangzhou"}) {
      com.aliyun.odps.account.StsAccount account =
          new com.aliyun.odps.account.StsAccount("dummy-id", "dummy-key", "dummy-sts", region);
      com.aliyun.odps.account.AppAccount app =
          new com.aliyun.odps.account.AppAccount(new AliyunAccount("dummy-app", "dummy-app-key"));
      Odps odps = new Odps(account, app);
      odps.setEndpoint("http://localhost");
      odps.options().setUserAgentSuffix(null);
      odps.options().setCallContext(Collections.singletonMap("session_id", "signed"));
      odps.options().setTraceContext(PARENT);
      Request request = odps.getRestClient().buildRequest("/projects/project", "GET", null, null);
      assertEquals("dummy-sts", request.getHeaders().get(Headers.AUTHORIZATION_STS_TOKEN));
      String authorization = request.getHeaders().get(Headers.AUTHORIZATION);
      String appAuthorization = request.getHeaders().get(Headers.APP_AUTHENTICATION);
      account.getRequestSigner().sign("/projects/project", request);
      app.getRequestSigner().sign("/projects/project", request);
      assertEquals(authorization, request.getHeaders().get(Headers.AUTHORIZATION));
      assertEquals(appAuthorization, request.getHeaders().get(Headers.APP_AUTHENTICATION));
      request.setHeader(CALL, "tampered");
      account.getRequestSigner().sign("/projects/project", request);
      app.getRequestSigner().sign("/projects/project", request);
      assertNotEquals(authorization, request.getHeaders().get(Headers.AUTHORIZATION));
      assertNotEquals(appAuthorization, request.getHeaders().get(Headers.APP_AUTHENTICATION));
    }
  }

  @Test
  public void cloningRetainsOptionsAndHasIndependentMutableConfiguration() {
    Odps original = client();
    original.options().setCallContext(Collections.singletonMap("session_id", "A"));
    original.options().setTraceContext(PARENT);
    Odps copy = original.clone();
    original.options().setCallContext(Collections.singletonMap("session_id", "B"));
    copy.options().setTraceContext(null);
    assertEquals("A", business(headers(copy)).get("session_id").getAsString());
    assertEquals("B", business(headers(original)).get("session_id").getAsString());
    assertEquals(PARENT, headers(original).get("traceparent"));
    assertFalse(headers(copy).containsKey("traceparent"));
  }
}
