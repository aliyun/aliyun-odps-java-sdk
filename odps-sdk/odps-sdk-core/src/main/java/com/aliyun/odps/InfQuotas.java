/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.odps.InfQuota.InfQuotaModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public class InfQuotas {

  private static final Gson GSON = InfQuota.GSON;

  private class InfQuotaListIterator extends ListIterator<InfQuota> {

    private final Map<String, String> params = new HashMap<>();

    InfQuotaListIterator(String tenantId, String regionId) {
      params.put("tenantId", tenantId);
      if (regionId != null) {
        params.put("regionId", regionId);
      }
    }

    @Override
    protected List<InfQuota> list() {
      String lastToken = params.get("token");
      if (params.containsKey("token") && (lastToken == null || lastToken.isEmpty())) {
        return null;
      }

      String resource = ResourceBuilder.buildInfQuotasResource();
      try {
        Response resp = client.request(resource, "GET", params, null, null);
        String body = new String(resp.getBody(), StandardCharsets.UTF_8);
        JsonObject jsonObject = new JsonParser().parse(body).getAsJsonObject();

        String nextToken = "";
        if (jsonObject.has("Token") && !jsonObject.get("Token").isJsonNull()) {
          nextToken = jsonObject.get("Token").getAsString();
        }
        params.put("token", nextToken);

        List<InfQuota> result = new ArrayList<>();
        if (jsonObject.has("Quotas") && !jsonObject.get("Quotas").isJsonNull()) {
          Type t = new TypeToken<List<InfQuotaModel>>() {}.getType();
          List<InfQuotaModel> models = GSON.fromJson(
              jsonObject.get("Quotas").getAsJsonArray(), t);
          result.addAll(
              models.stream()
                  .map(m -> new InfQuota(odps, m))
                  .collect(Collectors.toList()));
        }
        return result;
      } catch (OdpsException e) {
        String msg = e.getMessage();
        if (e.getRequestId() != null) {
          msg = "RequestId=" + e.getRequestId() + ", " + msg;
        }
        throw new RuntimeException(msg, e);
      }
    }
  }

  private final Odps odps;
  private final RestClient client;

  InfQuotas(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * Create an inference quota tree.
   *
   * @param quota the root quota (tree structure with children set via setChildren)
   * @return the created InfQuota
   */
  public InfQuota create(InfQuota quota) throws OdpsException {
    String resource = ResourceBuilder.buildInfQuotasResource();
    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/json");

    String bodyJson = GSON.toJson(quota.model);
    Response resp = client.request(
        resource, "POST", null, headers,
        bodyJson.getBytes(StandardCharsets.UTF_8));

    String respBody = new String(resp.getBody(), StandardCharsets.UTF_8);
    JsonObject jsonObject = new JsonParser().parse(respBody).getAsJsonObject();
    InfQuotaModel resultModel = GSON.fromJson(
        jsonObject.get("Quota"), InfQuotaModel.class);
    return new InfQuota(odps, resultModel);
  }

  /**
   * Get an inference quota by name (lazy-loaded).
   */
  public InfQuota get(String tenantId, String quotaName, String regionId) {
    return new InfQuota(odps, tenantId, quotaName, regionId);
  }

  /**
   * List all inference quota trees for a tenant.
   */
  public Iterator<InfQuota> list(String tenantId, String regionId) {
    return new InfQuotaListIterator(tenantId, regionId);
  }

  /**
   * Update an inference quota. All fields in the quota object are sent to the server;
   * the server decides which fields to accept or ignore.
   *
   * @param quota the quota with updated fields (tenantId, regionId, name must be set)
   * @return the updated InfQuota
   */
  public InfQuota update(InfQuota quota) throws OdpsException {
    String quotaName = quota.model.quotaName;
    String tenantId = quota.model.tenantId;
    String regionId = quota.model.regionId;

    String resource = ResourceBuilder.buildInfQuotaResource(quotaName);
    Map<String, String> params = new HashMap<>();
    params.put("tenantId", tenantId);
    if (regionId != null) {
      params.put("regionId", regionId);
    }

    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/json");

    String bodyJson = GSON.toJson(quota.model);
    Response resp = client.request(
        resource, "PUT", params, headers,
        bodyJson.getBytes(StandardCharsets.UTF_8));

    String respBody = new String(resp.getBody(), StandardCharsets.UTF_8);
    JsonObject jsonObject = new JsonParser().parse(respBody).getAsJsonObject();
    InfQuotaModel resultModel = GSON.fromJson(
        jsonObject.get("Quota"), InfQuotaModel.class);
    return new InfQuota(odps, resultModel);
  }

  /**
   * Delete an inference quota.
   */
  public void delete(
      String tenantId, String regionId, String quotaName) throws OdpsException {
    String resource = ResourceBuilder.buildInfQuotaResource(quotaName);
    Map<String, String> params = new HashMap<>();
    params.put("tenantId", tenantId);
    if (regionId != null) {
      params.put("regionId", regionId);
    }

    client.request(resource, "DELETE", params, null, null);
  }


}
