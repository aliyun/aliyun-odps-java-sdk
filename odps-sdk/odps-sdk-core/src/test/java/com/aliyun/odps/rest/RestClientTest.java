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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsDeprecatedLogger;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;

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
    odps.getRestClient().request(resource, method, params, headers, null, 0);
    assertTrue(OdpsDeprecatedLogger.getDeprecatedCalls().isEmpty());
  }

  @Test
  public void testUserAgent() throws OdpsException {
    System.err.println(odps.getRestClient().getUserAgent());
    assertTrue(odps.getRestClient().getUserAgent().contains("JavaVersion"));
    assertTrue(odps.getRestClient().getUserAgent().contains("Version"));
    assertTrue(odps.getRestClient().getUserAgent().contains("Revision"));
  }

  @Test (expected = RuntimeException.class)
  public void testError() throws OdpsException {
    Odps errorOdps = odps.clone();
    errorOdps.setEndpoint("http://error");
    errorOdps.projects().get().getCreatedTime();
  }
}
