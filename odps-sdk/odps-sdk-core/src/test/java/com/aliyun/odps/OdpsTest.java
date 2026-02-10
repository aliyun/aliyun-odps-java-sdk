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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import java.net.Proxy;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class OdpsTest {

  @Test
  public void testSets() {
    Odps odps = OdpsTestUtils.newDefaultOdps();

    // endpoint
    odps.setEndpoint("xxxx");
    Assert.assertEquals("xxxx", odps.getEndpoint());

    // default project
    odps.setDefaultProject("prj");
    Assert.assertEquals("prj", odps.getDefaultProject());
  }

  @Test
  public void testClone() {
    Odps odps1 = OdpsTestUtils.newDefaultOdps();
    odps1.instances().setDefaultRunningCluster("test_cluster");
    Odps odps2 = odps1.clone();
    assertFalse(odps1 == odps2);
    assertEquals(odps1.getAccount(), odps2.getAccount());
    assertEquals(odps1.getUserAgent(), odps2.getUserAgent());
    assertEquals(odps1.getEndpoint(), odps2.getEndpoint());
    assertEquals(odps1.getDefaultProject(), odps2.getDefaultProject());
    assertEquals(odps1.instances().getDefaultRunningCluster(), odps2.instances().getDefaultRunningCluster());
  }




  @Test
  public void testSetGetGlobalSettings() {
    final String SETTINGS = "settings";
    final String USER_DEFINED_SETTING = "hello";
    final String USER_DEFINED_SETTING_2 = "hello2";
    final String USER_DEFINED_VAL = "world";

    AliyunAccount account = new AliyunAccount("foo", "bar");
    Odps odps = new Odps(account);

    // Test set global settings
    Map<String, String> globalSettings = new HashMap<>();
    globalSettings.put(USER_DEFINED_SETTING, USER_DEFINED_VAL);
    globalSettings.put(USER_DEFINED_SETTING_2, USER_DEFINED_VAL);
    odps.setGlobalSettings(globalSettings);
    Task task = new SQLTask();
    task.loadGlobalSettings();
    String settingsStr = task.getProperties().get(SETTINGS);
    assertNotNull(settingsStr);
    JsonParser parser = new JsonParser();
    JsonObject jsonObject = parser.parse(settingsStr).getAsJsonObject();
    assertEquals(USER_DEFINED_VAL, jsonObject.get(USER_DEFINED_SETTING).getAsString());
    assertEquals(USER_DEFINED_VAL, jsonObject.get(USER_DEFINED_SETTING_2).getAsString());

    // Test get global settings
    Map<String, String> globalSettingsCopy = odps.getGlobalSettings();
    assertNotSame(globalSettingsCopy, globalSettings);
    assertEquals(globalSettings, globalSettingsCopy);
  }

  @Test
  public void testGlobalSettingsPriority() {
    // The priority of global settings is lower than settings set by users explicitly
    final String SETTINGS = "settings";
    final String SETTING_NAME = "hello";
    final String GLOBAL_VAL = "world";
    final String SETTING_VAL = "human";

    // Add global setting, this should be overwrote later
    AliyunAccount account = new AliyunAccount("foo", "bar");
    Odps odps = new Odps(account);
    Map<String, String> globalSettings = new HashMap<>();
    globalSettings.put(SETTING_NAME, GLOBAL_VAL);
    odps.setGlobalSettings(globalSettings);

    // Set the setting explicitly
    Task task = new SQLTask();
    Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    Map<String, String> settings = new HashMap<>();
    settings.put(SETTING_NAME, SETTING_VAL);
    gson.toJson(settings);
    task.setProperty(SETTINGS, gson.toJson(settings));

    // Check if the global setting is overwrote
    task.loadGlobalSettings();
    String settingsStr = task.getProperties().get(SETTINGS);
    assertNotNull(settingsStr);
    JsonParser parser = new JsonParser();
    JsonObject jsonObject = parser.parse(settingsStr).getAsJsonObject();
    assertEquals(SETTING_VAL, jsonObject.get(SETTING_NAME).getAsString());
  }

  @Test
  public void testProxyConfig() {
    Odps odps = OdpsTestUtils.newProxyConfigOdps();

    Proxy proxy = odps.getRestClient().getProxy();
    assertEquals(Proxy.Type.HTTP, proxy.type());
    assertEquals("proxy.example.com:8888", proxy.address().toString());
  }

  @Test
  public void testSetProxy() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();

    odps.options().setProxyConfig(
      ProxyConfig.builder().withHttpsProxy("https://proxy.example.com:8888").build());
    Proxy proxy = odps.getRestClient().getProxy();
    assertNull( proxy);

    odps.options().setProxyConfig(
      ProxyConfig.builder().withHttpProxy("http://proxy.example.com:8888").build());
    proxy = odps.getRestClient().getProxy();

    assertEquals(Proxy.Type.HTTP, proxy.type());
    assertEquals("proxy.example.com:8888", proxy.address().toString());

    // clear proxy
    odps.options().setProxyConfig(null);
    // should be ok
    odps.projects().get().reload();
  }


  @Test
  public void testCatalogApiHost() throws Exception {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    long start = System.currentTimeMillis();
    try {
      String catalogApiHost = odps.getCatalogApiHost();
      System.out.println(catalogApiHost);
    } catch (Exception e) {
      boolean
        catalogApiEndpointNotAvailable =
        e.getMessage().contains("Catalog API endpoint not available");
      System.out.println(catalogApiEndpointNotAvailable);
    }
    long cost = System.currentTimeMillis() - start;
    System.out.println("Get catalog api endpoint cost " + cost + "ms");

    odps.setCatalogApiHost("https://catalogapi.example.com");

    Assert.assertEquals("https://catalogapi.example.com", odps.getCatalogApiHost());
  }
}
