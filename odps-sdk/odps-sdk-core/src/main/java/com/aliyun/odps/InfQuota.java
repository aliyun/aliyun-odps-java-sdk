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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;

public class InfQuota extends LazyLoad {

  static final Gson GSON = new Gson();

  // Package-private model class — not exposed to external users
  static class InfQuotaModel {

    @SerializedName("tenantId")
    String tenantId;

    @SerializedName("quotaName")
    String quotaName;

    @SerializedName("nickname")
    String nickname;

    @SerializedName("quotaId")
    String quotaId;

    @SerializedName("regionId")
    String regionId;

    @SerializedName("orderInstanceId")
    String orderInstanceId;

    @SerializedName("billingMethod")
    String billingMethod;

    @SerializedName("apiKey")
    String apiKey;

    @SerializedName("status")
    String status;

    @SerializedName("concurrencyLimit")
    Long concurrencyLimit;

    @SerializedName("createTime")
    Long createTime;

    @SerializedName("updateTime")
    Long updateTime;

    @SerializedName("parentName")
    String parentQuotaName;

    @SerializedName("parentQuotaId")
    String parentQuotaId;

    @SerializedName("properties")
    Map<String, String> properties;

    @SerializedName("dashscope")
    String dashscope;

    @SerializedName("children")
    List<InfQuotaModel> children;
  }

  private Odps odps;
  InfQuotaModel model;

  /**
   * Public no-arg constructor. Use chain setters to build an InfQuota for create/update.
   */
  public InfQuota() {
    this.model = new InfQuotaModel();
  }

  InfQuota(Odps odps, String tenantId, String name, String regionId) {
    this.odps = odps;
    this.model = new InfQuotaModel();
    this.model.tenantId = tenantId;
    this.model.quotaName = name;
    this.model.regionId = regionId;
  }

  InfQuota(Odps odps, InfQuotaModel model) {
    this.odps = odps;
    this.model = model;
    setLoaded(true);
  }

  // ======================== Chain Setters ========================

  public InfQuota setTenantId(String tenantId) {
    this.model.tenantId = tenantId;
    return this;
  }

  public InfQuota setQuotaName(String quotaName) {
    this.model.quotaName = quotaName;
    return this;
  }

  public InfQuota setNickname(String nickname) {
    this.model.nickname = nickname;
    return this;
  }

  public InfQuota setRegionId(String regionId) {
    this.model.regionId = regionId;
    return this;
  }

  public InfQuota setBillingMethod(String billingMethod) {
    this.model.billingMethod = billingMethod;
    return this;
  }

  public InfQuota setStatus(String status) {
    this.model.status = status;
    return this;
  }

  public InfQuota setConcurrencyLimit(Long concurrencyLimit) {
    this.model.concurrencyLimit = concurrencyLimit;
    return this;
  }

  public InfQuota setQuotaId(String quotaId) {
    this.model.quotaId = quotaId;
    return this;
  }

  public InfQuota setOrderInstanceId(String orderInstanceId) {
    this.model.orderInstanceId = orderInstanceId;
    return this;
  }

  public InfQuota setApiKey(String apiKey) {
    this.model.apiKey = apiKey;
    return this;
  }

  public InfQuota setCreateTime(Long createTime) {
    this.model.createTime = createTime;
    return this;
  }

  public InfQuota setUpdateTime(Long updateTime) {
    this.model.updateTime = updateTime;
    return this;
  }

  public InfQuota setParentName(String parentName) {
    this.model.parentQuotaName = parentName;
    return this;
  }

  public InfQuota setParentQuotaId(String parentQuotaId) {
    this.model.parentQuotaId = parentQuotaId;
    return this;
  }

  public InfQuota setDashscope(String dashscope) {
    this.model.dashscope = dashscope;
    return this;
  }

  public InfQuota setProperties(Map<String, String> properties) {
    this.model.properties = properties;
    return this;
  }

  public InfQuota setChildren(List<InfQuota> children) {
    if (children == null) {
      this.model.children = null;
    } else {
      this.model.children = children.stream()
          .map(c -> c.model)
          .collect(Collectors.toList());
    }
    return this;
  }

  // ======================== Getters ========================

  public String getTenantId() {
    lazyLoad();
    return model.tenantId;
  }

  public String getQuotaName() {
    lazyLoad();
    return model.quotaName;
  }

  public String getNickname() {
    lazyLoad();
    return model.nickname;
  }

  public String getQuotaId() {
    lazyLoad();
    return model.quotaId;
  }

  public String getRegionId() {
    lazyLoad();
    return model.regionId;
  }

  public String getOrderInstanceId() {
    lazyLoad();
    return model.orderInstanceId;
  }

  public String getBillingMethod() {
    lazyLoad();
    return model.billingMethod;
  }

  public String getApiKey() {
    lazyLoad();
    return model.apiKey;
  }

  public String getStatus() {
    lazyLoad();
    return model.status;
  }

  public Long getConcurrencyLimit() {
    lazyLoad();
    return model.concurrencyLimit;
  }

  public Long getCreateTime() {
    lazyLoad();
    return model.createTime;
  }

  public Long getUpdateTime() {
    lazyLoad();
    return model.updateTime;
  }

  public String getParentName() {
    lazyLoad();
    return model.parentQuotaName;
  }

  public String getParentQuotaId() {
    lazyLoad();
    return model.parentQuotaId;
  }

  public String getDashscope() {
    lazyLoad();
    return model.dashscope;
  }

  public Map<String, String> getProperties() {
    lazyLoad();
    if (model.properties != null) {
      return new HashMap<>(model.properties);
    }
    return Collections.emptyMap();
  }

  public List<InfQuota> getChildren() {
    lazyLoad();
    if (model.children == null || model.children.isEmpty()) {
      return Collections.emptyList();
    }
    return model.children.stream()
        .map(child -> new InfQuota(odps, child))
        .collect(Collectors.toList());
  }

  @Override
  public void reload() throws OdpsException {
    if (odps == null) {
      return;
    }
    String resource = ResourceBuilder.buildInfQuotaResource(model.quotaName);
    Map<String, String> params = new HashMap<>();
    params.put("tenantId", model.tenantId);
    if (model.regionId != null) {
      params.put("regionId", model.regionId);
    }

    Response resp = odps.getRestClient().request(resource, "GET", params, null, null);
    String body = new String(resp.getBody(), StandardCharsets.UTF_8);
    JsonObject jsonObject = new JsonParser().parse(body).getAsJsonObject();
    model = GSON.fromJson(jsonObject.get("Quota"), InfQuotaModel.class);
    setLoaded(true);
  }
}
