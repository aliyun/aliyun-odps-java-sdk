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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.TopConsoleHttpServer.Call;
import com.aliyun.odps.security.CheckPermissionConstants.ActionType;
import com.aliyun.odps.security.CheckPermissionConstants.CheckPermissionResult;
import com.aliyun.odps.security.CheckPermissionConstants.ObjectType;
import com.aliyun.odps.security.SecurityManager.PermissionDesc;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class QuotaUsageHttpTest {
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
  public void usagePermissionTravelsThroughProjectAuthWithEncodedResource() throws Exception {
    server.enqueue(200, "<Auth><Result>Allow</Result><Message>allowed</Message></Auth>");
    PermissionDesc desc = new PermissionDesc("test_project", ObjectType.Quota, "quota/a b",
        ActionType.Usage, "tenant/a", "cn-hangzhou");
    desc.setPrincipal("RAM$test:reader");
    assertEquals(CheckPermissionResult.Allow,
        odps.projects().get().getSecurityManager().checkPermission(desc).getResult());
    Call call = server.calls().get(0);
    assertEquals("POST", call.method);
    assertEquals("/api/projects/test_project/auth/", call.path);
    JsonObject body = new JsonParser().parse(call.body).getAsJsonArray().get(0).getAsJsonObject();
    assertEquals("Usage", body.get("Action").getAsString());
    assertEquals("/tenants/tenant%2Fa/regions/cn-hangzhou/quotas/quota%2Fa%20b",
        body.get("Resource").getAsString());
    assertEquals("tenant/a", body.get("tenantId").getAsString());
    assertEquals("cn-hangzhou", body.get("regionId").getAsString());
    assertEquals("test_project", body.get("projectName").getAsString());
    assertEquals("RAM$test:reader", body.get("Principal").getAsString());
    assertTrue(call.contentType.startsWith("application/json"));
    assertEquals(1, server.calls().size());
  }

  @Test
  public void denyDecisionAndMessageAreReturned() throws Exception {
    server.enqueue(200, "<Auth><Result>Deny</Result><Message>quota denied</Message></Auth>");
    PermissionDesc desc = new PermissionDesc("test_project", ObjectType.Quota, "quota",
        ActionType.Usage, "tenant", "region");
    com.aliyun.odps.security.SecurityManager.CheckPermissionResultInfo result =
        odps.projects().get().getSecurityManager().checkPermission(desc);
    assertEquals(CheckPermissionResult.Deny, result.getResult());
    assertEquals("quota denied", result.getMessage());
  }

  @Test
  public void existingTablePermissionKeepsItsResourceContract() throws Exception {
    server.enqueue(200, "<Auth><Result>Allow</Result></Auth>");
    PermissionDesc desc = new PermissionDesc("test_project", ObjectType.Table, "table",
        ActionType.Select);
    odps.projects().get().getSecurityManager().checkPermission(desc);
    JsonObject body = new JsonParser().parse(server.calls().get(0).body).getAsJsonArray()
        .get(0).getAsJsonObject();
    assertEquals("/projects/test_project/tables/table", body.get("Resource").getAsString());
    assertEquals("Select", body.get("Action").getAsString());
  }

  @Test
  public void permissionTransportFailureIsNotAFalseDenyDecision() throws Exception {
    server.enqueue(403, "<Error><Code>AccessDenied</Code><Message>auth API forbidden</Message>"
        + "<RequestId>request-auth-denied</RequestId></Error>");
    try {
      odps.projects().get().getSecurityManager().checkPermission(new PermissionDesc(
          "test_project", ObjectType.Quota, "quota", ActionType.Usage, "tenant", "region"));
      fail("Expected an authorization API error");
    } catch (OdpsException expected) {
      assertEquals(Integer.valueOf(403), expected.getStatus());
      assertEquals("request-auth-denied", expected.getRequestId());
    }
  }
}
