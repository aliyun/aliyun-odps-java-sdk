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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.TopConsoleHttpServer.Call;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class InfQuotasHttpTest {
  private TopConsoleHttpServer server;
  private Odps odps;

  @Before
  public void setUp() throws Exception {
    server = new TopConsoleHttpServer();
    odps = server.newOdps();
  }

  @After
  public void tearDown() {
    server.close();
  }

  @Test
  public void createSendsQuotaTreeAndReturnsLoadedResult() throws Exception {
    server.enqueue(200, quota("root"));
    InfQuota child = new InfQuota().setQuotaName("child").setConcurrencyLimit(2L);
    InfQuota root = new InfQuota().setTenantId("tenant").setRegionId("cn-hangzhou")
        .setQuotaName("root").setBillingMethod("payasyougo").setConcurrencyLimit(4L)
        .setDashscope("enabled").setChildren(Collections.singletonList(child));
    InfQuota created = odps.infQuotas().create(root);

    assertEquals("root", created.getQuotaName());
    assertEquals(1, server.calls().size());
    Call call = server.calls().get(0);
    assertEquals("POST", call.method);
    assertEquals("/api/inferencequotas", call.path);
    assertTrue(call.authorization.startsWith("ODPS test-id:"));
    assertTrue(call.contentType.startsWith("application/json"));
    JsonObject body = json(call.body);
    assertEquals("tenant", body.get("tenantId").getAsString());
    assertEquals("cn-hangzhou", body.get("regionId").getAsString());
    assertEquals("enabled", body.get("dashscope").getAsString());
    assertEquals("child", body.getAsJsonArray("children").get(0).getAsJsonObject()
        .get("quotaName").getAsString());
  }

  @Test
  public void getIsLazyAndEscapesNamesAndQueries() throws Exception {
    InfQuota quota = odps.infQuotas().get("tenant/a", "quota/a b", "cn-hangzhou");
    assertTrue(server.calls().isEmpty());
    server.enqueue(200, quota("quota/a b"));
    assertEquals("quota/a b", quota.getQuotaName());
    assertEquals("tenant", quota.getTenantId());
    assertEquals(1, server.calls().size());
    Call call = server.calls().get(0);
    assertEquals("GET", call.method);
    assertEquals("/api/inferencequotas/quota%2Fa%20b", call.path);
    assertEquals("tenant/a", call.query.get("tenantId"));
    assertEquals("cn-hangzhou", call.query.get("regionId"));
  }

  @Test
  public void reloadRefreshesMetadata() throws Exception {
    server.enqueue(200, "{\"Quota\":{\"quotaName\":\"root\",\"nickname\":\"first\"}}");
    InfQuota quota = odps.infQuotas().get("tenant", "root", null);
    assertEquals("first", quota.getNickname());
    server.enqueue(200, "{\"Quota\":{\"quotaName\":\"root\",\"nickname\":\"second\"}}");
    quota.reload();
    assertEquals("second", quota.getNickname());
    assertEquals(2, server.calls().size());
    assertFalse(server.calls().get(0).query.containsKey("regionId"));
  }

  @Test
  public void listFollowsTokensAcrossEmptyPagesAndStopsWithoutAnotherRequest() {
    server.enqueue(200, "{\"Token\":\"next/+ token\",\"Quotas\":[{\"quotaName\":\"one\"}]}");
    server.enqueue(200, "{\"Token\":\"last\",\"Quotas\":[]}");
    server.enqueue(200, "{\"Token\":\"\",\"Quotas\":[{\"quotaName\":\"two\"}]}");
    Iterator<InfQuota> it = odps.infQuotas().list("tenant", "cn-hangzhou");

    assertEquals("one", it.next().getQuotaName());
    assertEquals("two", it.next().getQuotaName());
    assertFalse(it.hasNext());
    assertFalse(it.hasNext());
    try {
      it.next();
      fail("Exhausted iterator must throw");
    } catch (NoSuchElementException expected) {
      assertEquals(3, server.calls().size());
    }
    assertFalse(server.calls().get(0).query.containsKey("token"));
    assertEquals("next/+ token", server.calls().get(1).query.get("token"));
    assertEquals("last", server.calls().get(2).query.get("token"));
    assertEquals("tenant", server.calls().get(2).query.get("tenantId"));
  }

  @Test
  public void listWithoutTokenOrQuotasIsEmpty() {
    server.enqueue(200, "{}");
    Iterator<InfQuota> it = odps.infQuotas().list("tenant", null);
    assertFalse(it.hasNext());
    assertFalse(it.hasNext());
    assertEquals(1, server.calls().size());
    assertFalse(server.calls().get(0).query.containsKey("regionId"));
  }

  @Test
  public void listHandlesNullTokenAndQuotaArray() {
    server.enqueue(200, "{\"Token\":null,\"Quotas\":null}");
    assertFalse(odps.infQuotas().list("tenant", "region").hasNext());
    assertEquals(1, server.calls().size());
  }

  @Test
  public void updateSendsModelAndUsesReturnedState() throws Exception {
    server.enqueue(200, "{\"Quota\":{\"quotaName\":\"root\",\"nickname\":\"accepted\"}}");
    InfQuota result = odps.infQuotas().update(new InfQuota().setTenantId("tenant")
        .setRegionId("region").setQuotaName("root").setNickname("requested"));
    assertEquals("accepted", result.getNickname());
    assertEquals(1, server.calls().size());
    Call call = server.calls().get(0);
    assertEquals("PUT", call.method);
    assertEquals("/api/inferencequotas/root", call.path);
    assertEquals("tenant", call.query.get("tenantId"));
    assertEquals("region", call.query.get("regionId"));
    assertEquals("requested", json(call.body).get("nickname").getAsString());
  }

  @Test
  public void deleteEncodesNameAndOmitsAbsentRegion() throws Exception {
    server.enqueue(200, "{}");
    odps.infQuotas().delete("tenant", null, "root/child");
    Call call = server.calls().get(0);
    assertEquals("DELETE", call.method);
    assertEquals("/api/inferencequotas/root%2Fchild", call.path);
    assertEquals("tenant", call.query.get("tenantId"));
    assertFalse(call.query.containsKey("regionId"));
    assertEquals("", call.body);
  }

  @Test
  public void httpErrorsKeepStatusCodeAndRequestId() throws Exception {
    server.enqueue(403, "<Error><Code>AccessDenied</Code><Message>denied</Message>"
        + "<RequestId>request-create-denied</RequestId></Error>");
    try {
      odps.infQuotas().create(new InfQuota().setQuotaName("root"));
      fail("Expected service error");
    } catch (OdpsException expected) {
      assertEquals(Integer.valueOf(403), expected.getStatus());
      assertEquals("AccessDenied", expected.getErrorCode());
      assertEquals("request-create-denied", expected.getRequestId());
    }
    assertEquals(1, server.calls().size());
  }

  @Test
  public void listErrorRetainsOriginalOdpsException() {
    server.enqueue(404, "<Error><Code>NoSuchObject</Code><Message>missing</Message>"
        + "<RequestId>request-list-missing</RequestId></Error>");
    try {
      odps.infQuotas().list("tenant", null).hasNext();
      fail("Expected list error");
    } catch (RuntimeException expected) {
      assertTrue(expected.getCause() instanceof OdpsException);
      OdpsException cause = (OdpsException) expected.getCause();
      assertEquals(Integer.valueOf(404), cause.getStatus());
      assertEquals("request-list-missing", cause.getRequestId());
      assertTrue(expected.getMessage().contains("request-list-missing"));
    }
  }

  @Test
  public void detachedQuotaRemainsUsableWithoutNetwork() {
    InfQuota quota = new InfQuota().setQuotaName("detached").setDashscope("enabled");
    assertEquals("detached", quota.getQuotaName());
    assertEquals("enabled", quota.getDashscope());
    assertNotNull(quota.getProperties());
    assertTrue(quota.getChildren().isEmpty());
    assertTrue(server.calls().isEmpty());
  }

  private static String quota(String name) {
    JsonObject quota = new JsonObject();
    quota.addProperty("quotaName", name);
    quota.addProperty("tenantId", "tenant");
    JsonObject response = new JsonObject();
    response.add("Quota", quota);
    return response.toString();
  }

  private static JsonObject json(String body) {
    return new JsonParser().parse(body).getAsJsonObject();
  }
}
