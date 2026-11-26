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

package com.aliyun.odps.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.aliyun.odps.security.CheckPermissionConstants.ActionType;
import com.aliyun.odps.security.CheckPermissionConstants.ObjectType;
import com.aliyun.odps.security.SecurityManager.PermissionDesc;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class PermissionDescTest {

  @Test
  public void testProjectPermissionJson() {
    PermissionDesc desc = new PermissionDesc(
        "my_project", ObjectType.Project, "my_project", ActionType.Read);

    String json = desc.toJson();
    JsonArray arr = new JsonParser().parse(json).getAsJsonArray();
    JsonObject obj = arr.get(0).getAsJsonObject();

    assertEquals("Read", obj.get("Action").getAsString());
    assertEquals("/projects/my_project", obj.get("Resource").getAsString());
  }

  @Test
  public void testQuotaUsagePermissionJson() {
    PermissionDesc desc = new PermissionDesc(
        "my_project", ObjectType.Quota, "my_quota",
        ActionType.Usage, "tenant-001", "cn-hangzhou");

    String json = desc.toJson();
    JsonArray arr = new JsonParser().parse(json).getAsJsonArray();
    JsonObject obj = arr.get(0).getAsJsonObject();

    assertEquals("Usage", obj.get("Action").getAsString());
    assertEquals("/tenants/tenant-001/regions/cn-hangzhou/quotas/my_quota",
        obj.get("Resource").getAsString());
    assertEquals("tenant-001", obj.get("tenantId").getAsString());
    assertEquals("cn-hangzhou", obj.get("regionId").getAsString());
    assertEquals("my_project", obj.get("projectName").getAsString());
  }

  @Test
  public void testQuotaPermissionWithSpecialChars() {
    PermissionDesc desc = new PermissionDesc(
        "proj", ObjectType.Quota, "quota/name",
        ActionType.Usage, "tenant/1", "cn-shanghai");

    String json = desc.toJson();
    JsonArray arr = new JsonParser().parse(json).getAsJsonArray();
    JsonObject obj = arr.get(0).getAsJsonObject();

    // names with special chars should be encoded
    String resource = obj.get("Resource").getAsString();
    assertTrue(resource.contains("/quotas/"));
    assertEquals("Usage", obj.get("Action").getAsString());
  }

  @Test
  public void testAddParamAppearsInJson() {
    PermissionDesc desc = new PermissionDesc(
        "proj", ObjectType.Quota, "q1",
        ActionType.Usage, "t1", "r1");
    desc.addParam("customKey", "customValue");

    String json = desc.toJson();
    JsonArray arr = new JsonParser().parse(json).getAsJsonArray();
    JsonObject obj = arr.get(0).getAsJsonObject();

    assertEquals("customValue", obj.get("customKey").getAsString());
  }
}
