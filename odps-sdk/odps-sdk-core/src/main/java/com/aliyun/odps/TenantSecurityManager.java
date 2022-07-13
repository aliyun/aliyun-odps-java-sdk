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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.security.SecurityManager.AuthorizationQueryInstance;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.utils.GsonObjectBuilder;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonParser;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class TenantSecurityManager {

  @Root(name = "Authorization", strict = false)
  private static class AuthorizationQueryRequest {

    @Element(name = "Query", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String query;

    @Element(name = "ResponseInJsonFormat", required = false)
    private boolean responseInJsonFormat;

    @SuppressWarnings("unused")
    public AuthorizationQueryRequest() {
      this.query = "";
      this.responseInJsonFormat = true;
    }

    public AuthorizationQueryRequest(String query, boolean jsonFormat) {
      this.query = query;
      this.responseInJsonFormat = jsonFormat;
    }
  }

  private static class AuthorizationQueryResponse {

    @Expose
    @SerializedName("Response")
    private String result;

    public static AuthorizationQueryResponse from(byte[] body) {
      if (body == null) {
        return null;
      }

      return fromJson(body);
    }

    private static AuthorizationQueryResponse fromJson(byte[] body) {
      try {
        return GsonObjectBuilder
            .get()
            .fromJson(new String(body, StandardCharsets.UTF_8), AuthorizationQueryResponse.class);
      } catch (Exception ignore) {
        return null;
      }
    }

    public String getResult() {
      return result;
    }
  }

  private static final String TENANT_ID_PLACE_HOLDER = "_empty_tenant_";

  private RestClient client;

  TenantSecurityManager(RestClient client) {
    this.client = client;
  }

  public String getTenantRolePolicy(String roleName) throws OdpsException {
    if (StringUtils.isNullOrEmpty(roleName)) {
      throw new IllegalArgumentException("Argument 'roleName' cannot be null or empty");
    }

    String resource = ResourceBuilder.buildTenantRoleResource(TENANT_ID_PLACE_HOLDER, roleName);
    Map<String, String> params = new HashMap<>();
    params.put("policy", null);
    Response response = client.request(resource, "GET", params, null, null);

    String rawResult = new String(response.getBody(), StandardCharsets.UTF_8);
    JsonParser parser = new JsonParser();
    return parser.parse(rawResult).getAsJsonObject().get("Policy").getAsString();
  }

  public void putTenantRolePolicy(String roleName, String policy) throws OdpsException {
    if (StringUtils.isNullOrEmpty(roleName)) {
      throw new IllegalArgumentException("Argument 'roleName' cannot be null or empty");
    }
    if (policy == null) {
      throw new IllegalArgumentException("Argument 'policy' cannot be null");
    }

    String resource = ResourceBuilder.buildTenantRoleResource(TENANT_ID_PLACE_HOLDER, roleName);
    Map<String, String> params = new HashMap<>();
    params.put("policy", null);
    client.stringRequest(resource, "PUT", params, null, policy);
  }

  public AuthorizationQueryInstance run(String query, boolean jsonOutput) throws OdpsException {

    String resource = ResourceBuilder.buildTenantSecurityManagerResource(TENANT_ID_PLACE_HOLDER);
    AuthorizationQueryRequest request = new AuthorizationQueryRequest(query, jsonOutput);

    String xmlRequest;
    try {
      xmlRequest = SimpleXmlUtils.marshal(request);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
    HashMap<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    Response response =
        client.stringRequest(resource, "POST", null, headers, xmlRequest);
    try {
      AuthorizationQueryResponse queryResponse =
          AuthorizationQueryResponse.from(response.getBody());

      if (response.getStatus() == 200) {
        return new AuthorizationQueryInstance(queryResponse.getResult());
      } else {
        throw new OdpsException("Async tenant authorization instance is not supported yet.");
      }
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }
}
