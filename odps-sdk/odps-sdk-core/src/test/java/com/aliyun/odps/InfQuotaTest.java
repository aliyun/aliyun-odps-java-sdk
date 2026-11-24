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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.InfQuota.InfQuotaModel;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public class InfQuotaTest {

  private static final Gson GSON = InfQuota.GSON;

  @Test
  public void testDashscopeAndOptionalMetadataRoundTrip() {
    InfQuota quota = new InfQuota().setQuotaName("quota").setDashscope("enabled")
        .setApiKey("test-api-key");
    JsonObject json = GSON.toJsonTree(quota.model).getAsJsonObject();
    json.addProperty("orderInstanceId", "order");
    json.addProperty("quotaId", "quota-id");
    json.addProperty("createTime", 1L);
    json.addProperty("updateTime", 2L);
    InfQuotaModel model = GSON.fromJson(json, InfQuotaModel.class);
    InfQuota decoded = new InfQuota(null, model);
    assertEquals("enabled", decoded.getDashscope());
    assertEquals("test-api-key", decoded.getApiKey());
    assertEquals("order", decoded.getOrderInstanceId());
    assertEquals("quota-id", decoded.getQuotaId());
    assertEquals(Long.valueOf(1L), decoded.getCreateTime());
    assertEquals(Long.valueOf(2L), decoded.getUpdateTime());
  }

  @Test
  public void testDeserializeQuota() {
    String json = "{"
        + "\"tenantId\": \"project-40822\","
        + "\"quotaName\": \"root_quota\","
        + "\"nickname\": \"RootQuota_v1\","
        + "\"quotaId\": \"iq-20260510-001\","
        + "\"regionId\": \"cn-shanghai\","
        + "\"orderInstanceId\": \"ord-7f3a9b2c4d\","
        + "\"billingMethod\": \"payasyougo\","
        + "\"apiKey\": \"AK-8xP3mQvR7yU2\","
        + "\"status\": \"ON\","
        + "\"concurrencyLimit\": 100,"
        + "\"createTime\": 1778983633,"
        + "\"updateTime\": 1778983633,"
        + "\"properties\": {\"community_code\": \"CC-001\"},"
        + "\"children\": ["
        + "  {"
        + "    \"tenantId\": \"project-40822\","
        + "    \"quotaName\": \"child_1\","
        + "    \"nickname\": \"ChildAlpha_v1\","
        + "    \"quotaId\": \"iq-20260510-002\","
        + "    \"regionId\": \"cn-shanghai\","
        + "    \"parentName\": \"root_quota\","
        + "    \"parentQuotaId\": \"iq-20260510-001\","
        + "    \"status\": \"ON\","
        + "    \"concurrencyLimit\": 100,"
        + "    \"createTime\": 1778983633,"
        + "    \"updateTime\": 1778983633,"
        + "    \"properties\": {},"
        + "    \"children\": []"
        + "  }"
        + "]"
        + "}";

    InfQuotaModel model = GSON.fromJson(json, InfQuotaModel.class);

    assertEquals("project-40822", model.tenantId);
    assertEquals("root_quota", model.quotaName);
    assertEquals("RootQuota_v1", model.nickname);
    assertEquals("iq-20260510-001", model.quotaId);
    assertEquals("cn-shanghai", model.regionId);
    assertEquals("payasyougo", model.billingMethod);
    assertEquals("ON", model.status);
    assertEquals(Long.valueOf(100), model.concurrencyLimit);
    assertEquals(Long.valueOf(1778983633L), model.createTime);
    assertEquals("CC-001", model.properties.get("community_code"));

    assertNotNull(model.children);
    assertEquals(1, model.children.size());
    InfQuotaModel child = model.children.get(0);
    assertEquals("child_1", child.quotaName);
    assertEquals("root_quota", child.parentQuotaName);
    assertEquals("iq-20260510-001", child.parentQuotaId);
  }

  @Test
  public void testGettersWithModel() {
    InfQuotaModel model = new InfQuotaModel();
    model.tenantId = "tenant-1";
    model.quotaName = "my_quota";
    model.nickname = "MyQuota_v1";
    model.quotaId = "iq-001";
    model.regionId = "cn-shanghai";
    model.billingMethod = "payasyougo";
    model.status = "OFF";
    model.concurrencyLimit = 50L;
    model.createTime = 1778983633L;
    model.updateTime = 1778984000L;
    model.parentQuotaName = "parent";
    model.parentQuotaId = "iq-000";
    model.properties = Collections.singletonMap("k", "v");
    model.children = Collections.emptyList();

    InfQuota quota = new InfQuota(null, model);

    assertEquals("tenant-1", quota.getTenantId());
    assertEquals("my_quota", quota.getQuotaName());
    assertEquals("MyQuota_v1", quota.getNickname());
    assertEquals("iq-001", quota.getQuotaId());
    assertEquals("cn-shanghai", quota.getRegionId());
    assertEquals("payasyougo", quota.getBillingMethod());
    assertEquals("OFF", quota.getStatus());
    assertEquals(Long.valueOf(50), quota.getConcurrencyLimit());
    assertEquals("parent", quota.getParentName());
    assertEquals("iq-000", quota.getParentQuotaId());
    assertEquals("v", quota.getProperties().get("k"));
    assertTrue(quota.getChildren().isEmpty());
  }

  @Test
  public void testNullChildrenAndProperties() {
    InfQuotaModel model = new InfQuotaModel();
    model.quotaName = "minimal";
    model.status = "ON";

    InfQuota quota = new InfQuota(null, model);

    assertTrue(quota.getChildren().isEmpty());
    assertTrue(quota.getProperties().isEmpty());
  }

  @Test
  public void testGetPropertiesReturnsDefensiveCopy() {
    InfQuotaModel model = new InfQuotaModel();
    model.quotaName = "test";
    model.properties = new HashMap<>();
    model.properties.put("key1", "val1");

    InfQuota quota = new InfQuota(null, model);

    Map<String, String> props = quota.getProperties();
    props.put("key2", "val2");

    assertNull(quota.getProperties().get("key2"));
    assertEquals(1, quota.getProperties().size());
  }

  @Test
  public void testSerializeCreateModel() {
    InfQuotaModel model = new InfQuotaModel();
    model.tenantId = "10001";
    model.regionId = "cn-shanghai";
    model.quotaName = "root_quota";
    model.nickname = "RootQuota_v1";
    model.billingMethod = "payasyougo";
    model.status = "ON";
    model.concurrencyLimit = 100L;

    InfQuotaModel child = new InfQuotaModel();
    child.quotaName = "child_1";
    child.nickname = "Child_v1";
    child.status = "ON";
    child.concurrencyLimit = 100L;
    child.parentQuotaName = "root_quota";
    child.parentQuotaId = "iq-001";
    model.children = Collections.singletonList(child);

    String json = GSON.toJson(model);
    JsonObject parsed = new JsonParser().parse(json).getAsJsonObject();

    assertEquals("10001", parsed.get("tenantId").getAsString());
    assertEquals("cn-shanghai", parsed.get("regionId").getAsString());
    assertEquals("root_quota", parsed.get("quotaName").getAsString());
    assertEquals("ON", parsed.get("status").getAsString());
    assertEquals(1, parsed.get("children").getAsJsonArray().size());

    JsonObject childObj = parsed.get("children").getAsJsonArray().get(0).getAsJsonObject();
    assertEquals("root_quota", childObj.get("parentName").getAsString());
    assertEquals("iq-001", childObj.get("parentQuotaId").getAsString());
  }

  @Test
  public void testDeserializeListResponse() {
    String responseJson = "{"
        + "\"Success\": true,"
        + "\"Code\": \"OK\","
        + "\"HttpStatusCode\": 200,"
        + "\"RequestId\": \"req-abc-008\","
        + "\"Token\": \"\","
        + "\"Quotas\": ["
        + "  {"
        + "    \"tenantId\": \"10001\","
        + "    \"quotaName\": \"root_quota\","
        + "    \"status\": \"ON\","
        + "    \"concurrencyLimit\": 100,"
        + "    \"children\": []"
        + "  }"
        + "]"
        + "}";

    JsonObject jsonObject = new JsonParser().parse(responseJson).getAsJsonObject();
    String token = jsonObject.get("Token").getAsString();
    assertTrue(token.isEmpty());

    List<InfQuotaModel> models = GSON.fromJson(
        jsonObject.get("Quotas"),
        new TypeToken<List<InfQuotaModel>>() {}.getType());
    assertEquals(1, models.size());
    assertEquals("root_quota", models.get(0).quotaName);
  }

  @Test
  public void testChainSetters() {
    InfQuota quota = new InfQuota()
        .setTenantId("t1")
        .setQuotaName("q1")
        .setNickname("nick")
        .setRegionId("cn-shanghai")
        .setBillingMethod("payasyougo")
        .setStatus("ON")
        .setConcurrencyLimit(100L)
        .setParentName("parent")
        .setParentQuotaId("pid")
        .setProperties(Collections.singletonMap("k", "v"));

    assertEquals("t1", quota.getTenantId());
    assertEquals("q1", quota.getQuotaName());
    assertEquals("nick", quota.getNickname());
    assertEquals("cn-shanghai", quota.getRegionId());
    assertEquals("payasyougo", quota.getBillingMethod());
    assertEquals("ON", quota.getStatus());
    assertEquals(Long.valueOf(100), quota.getConcurrencyLimit());
    assertEquals("parent", quota.getParentName());
    assertEquals("pid", quota.getParentQuotaId());
    assertEquals("v", quota.getProperties().get("k"));
  }

  @Test
  public void testSetChildren() {
    InfQuota child = new InfQuota()
        .setQuotaName("child1")
        .setStatus("ON");

    InfQuota parent = new InfQuota()
        .setQuotaName("parent")
        .setChildren(Collections.singletonList(child));

    List<InfQuota> children = parent.getChildren();
    assertEquals(1, children.size());
    assertEquals("child1", children.get(0).getQuotaName());
  }
}
