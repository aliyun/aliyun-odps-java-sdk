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

package com.aliyun.odps.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsDeprecatedLogger;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.AppAccount;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.retry.RetryHeaders;

public class RestClientTest extends TestBase {

  private RestClient client;


  @Test
  public void testRestRequest() throws OdpsException {

    String resource = "/projects/" + odps.getDefaultProject();
    String method = "GET";
    Map<String, String> params = null;
    Map<String, String> headers = null;
    Response response = odps.getRestClient().request(resource, method, params, headers, null, 0);
    System.out.println(new String(response.getBody()));
  }

  @Test
  @Ignore("Unreasonable when loop calls api 20 times, will return '429 Retry After'")
  public void testRetryAfter() {
    String resource = "/projects/retry_after/tables/test_retry";
    String method = "GET";
    boolean ok = false;
    for (int i = 0; i < 20; i++) {
      try {
        odps.getRestClient().setRetryTimes(0);
        odps.getRestClient().request(resource, method, null, null, null, 0);
      } catch (OdpsException err) {
        ok = err.existRetryAfter();
        if (ok) {
          break;
        }
      }
    }
    assertTrue(ok);
  }

  @Test
  public void testRestHeaderValueNull() throws OdpsException {

    String resource = "/projects/" + odps.getDefaultProject();
    String method = "GET";
    Map<String, String> params = null;
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.ODPS_COMMENT, null);
    Response response = odps.getRestClient().request(resource, method, params, headers, null, 0);
    System.out.println(new String(response.getBody()));
  }

  @Test
  public void testUploadDeprecated() throws OdpsException {
    OdpsDeprecatedLogger.getDeprecatedCalls().put("ABC", 1L);
    String resource = "/projects/" + odps.getDefaultProject();
    String method = "GET";
    Map<String, String> params = null;
    Map<String, String> headers = null;
    Response response = odps.getRestClient().request(resource, method, params, headers, null, 0);
    System.out.println(response.getMessage());
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertTrue(OdpsDeprecatedLogger.getDeprecatedCalls().isEmpty());
  }

  @Test
  public void testUserAgent() throws OdpsException {
    System.err.println(odps.getRestClient().getUserAgent());
    assertTrue(odps.getRestClient().getUserAgent().contains("JavaVersion"));
    assertTrue(odps.getRestClient().getUserAgent().contains("Version"));
    assertTrue(odps.getRestClient().getUserAgent().contains("Revision"));
  }

  @Test
  public void testPlatformIdIsAddedToUserAgentHeaders() {
    RestClient restClient = new RestClient(null);
    restClient.setAccount(new AliyunAccount("id", "key"));
    restClient.setEndpoint("http://localhost");
    restClient.setUserAgent("custom", "qwen-code");

    Request request = restClient.buildRequest("/projects/project_name", "GET", null, null);

    String expected = restClient.getUserAgent();
    assertTrue(expected.endsWith("custom Platform:qwen-code"));
    assertEquals(expected, request.getHeaders().get(Headers.USER_AGENT));
    assertEquals(expected, request.getHeaders().get(Headers.ODPS_USER_AGENT));
  }

  @Test
  public void testTunnelRetryHeadersAcrossInternalGetRetry() throws OdpsException {
    TracingRestClient restClient = new TracingRestClient();
    restClient.enableTunnelRetryHeaders();
    restClient.setRetryTimes(1);
    restClient.setConnectTimeout(1);
    Map<String, String> originalHeaders = new HashMap<>();

    restClient.request("/projects/project", "GET", null, originalHeaders, null, 0);

    assertEquals(2, restClient.capturedHeaders.size());
    Map<String, String> first = restClient.capturedHeaders.get(0);
    Map<String, String> second = restClient.capturedHeaders.get(1);
    assertEquals(first.get(RetryHeaders.TRACE_ID), second.get(RetryHeaders.TRACE_ID));
    assertEquals("0", first.get(RetryHeaders.RETRY_INDEX));
    assertEquals("1", second.get(RetryHeaders.RETRY_INDEX));
    assertTrue(!originalHeaders.containsKey(RetryHeaders.TRACE_ID));
  }

  @Test
  public void testExplicitRetryContextAvoidsNestedGetRetry() {
    TracingRestClient restClient = new TracingRestClient();
    restClient.enableTunnelRetryHeaders();
    restClient.setRetryTimes(1);
    restClient.setConnectTimeout(1);
    Map<String, String> headers = new HashMap<>();
    headers.put(RetryHeaders.TRACE_ID, "outer-trace");
    headers.put(RetryHeaders.RETRY_INDEX, "7");

    try {
      restClient.request("/projects/project", "GET", null, headers, null, 0);
      fail("expected the first request to fail without an inner retry");
    } catch (OdpsException expected) {
      assertEquals(1, restClient.capturedHeaders.size());
      assertEquals("outer-trace",
          restClient.capturedHeaders.get(0).get(RetryHeaders.TRACE_ID));
      assertEquals("7", restClient.capturedHeaders.get(0).get(RetryHeaders.RETRY_INDEX));
    }
  }

  @Test(expected = RuntimeException.class)
  public void testError() throws OdpsException {
    Odps errorOdps = odps.clone();
    errorOdps.setEndpoint("http://error");
    errorOdps.projects().get().getCreatedTime();
  }

  @Test
  public void testApplicationAuthentication() {
    String accessId = "id";
    String accessKey = "key";
    String appAccessId = "app_id";
    String appAccessKey = "app_key";
    Odps odps = new Odps(new AliyunAccount(accessId, accessKey),
                         new AppAccount(new AliyunAccount(appAccessId, appAccessKey)));
    RestClient restClient = odps.getRestClient();

    Map<String, String> params = new HashMap<String, String>();
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.ODPS_USER_AGENT,
                "JavaSDK Revision:295ed47 Version:0.30.3 JavaVersion:1.8.0_172 CLT(0.30.0 : c36da9d); Mac OS X(30.5.26.135/jondeMacBook-Pro.local)");
    headers.put(Headers.DATE, "Fri, 30 Nov 2018 03:31:29 GMT");
    Request
        request =
        restClient.buildRequest("/projects/project_name/instances/instance_name", "POST", params,
                                headers);
    assertTrue(request.getHeaders().containsKey(Headers.APP_AUTHENTICATION));
  }

  @Test
  public void testRequestInterceptor() throws OdpsException {
    String resource = "/projects/" + odps.getDefaultProject();
    String method = "GET";
    Map<String, String> params = null;
    Map<String, String> headers = null;
    List<String> testList = new ArrayList<>();
    odps.getRestClient().addRequestInterceptor(context -> {
      testList.add("request");
      context.getRequest().setHeader("UNIT-TEST", "TRUE");
      return context.getRequest();
    });
    odps.getRestClient().addResponseInterceptor(context -> {
      testList.add("response");
      assertEquals("TRUE", context.getRequest().getHeaders().getOrDefault("UNIT-TEST", ""));
      assertEquals(200, context.getResponse().getStatus());
      return context.getResponse();
    });
    Response response = odps.getRestClient().request(resource, method, params, headers, null, 0);
    System.out.println(new String(response.getBody()));
    assertEquals("request", testList.get(0));
    assertEquals("response", testList.get(1));
  }

  private static class TracingRestClient extends RestClient {

    private int requests;
    private final List<Map<String, String>> capturedHeaders = new ArrayList<>();

    TracingRestClient() {
      super(null);
    }

    @Override
    protected Response requestWithNoRetry(String resource, String method,
                                          Map<String, String> params,
                                          Map<String, String> headers,
                                          java.io.InputStream body, long bodyLen)
        throws OdpsException {
      capturedHeaders.add(new HashMap<>(headers));
      if (requests++ == 0) {
        OdpsException exception = new OdpsException("retry");
        exception.setStatus(500);
        throw exception;
      }
      return new SuccessfulResponse();
    }
  }

  private static class SuccessfulResponse extends Response {

    SuccessfulResponse() {
      status = 200;
      body = new byte[0];
    }
  }
}
