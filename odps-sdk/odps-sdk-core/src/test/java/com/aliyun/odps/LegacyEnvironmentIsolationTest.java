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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.options.CreateInstanceOption;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.task.SQLRTTask;
import com.aliyun.odps.task.SQLTask;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** Offline regression coverage; also run with MC_PLATFORM_ID and TRACEPARENT populated. */
public class LegacyEnvironmentIsolationTest {

  @Test
  public void testTaskSettingsIgnoreEnvironment() throws OdpsException {
    for (Task task : tasks()) {
      submit(task);
      JsonObject settings = JsonParser.parseString(
          task.getProperties().get("settings")).getAsJsonObject();
      assertFalse(settings.has("EXT_PLATFORM_ID"));
      assertFalse(settings.has("EXT_TASK_ID"));
    }
  }

  @Test
  public void testExplicitTaskSettingsArePreserved() throws OdpsException {
    for (Task task : tasks()) {
      task.setProperty("settings", "{\"EXT_PLATFORM_ID\":\"explicit-platform\","
          + "\"EXT_TASK_ID\":\"explicit-task\"}");
      submit(task);
      JsonObject settings = JsonParser.parseString(
          task.getProperties().get("settings")).getAsJsonObject();
      assertEquals("explicit-platform", settings.get("EXT_PLATFORM_ID").getAsString());
      assertEquals("explicit-task", settings.get("EXT_TASK_ID").getAsString());
    }
  }

  @Test
  public void testSessionAcceptsNullHints() throws OdpsException {
    for (boolean deprecatedApi : new boolean[] {false, true}) {
      assertEquals(new JsonObject(), sessionSettings(null, deprecatedApi));
    }
  }

  @Test
  public void testSessionPreservesExplicitHints() throws OdpsException {
    Map<String, String> hints = new HashMap<>();
    hints.put("EXT_PLATFORM_ID", "explicit-platform");
    hints.put("EXT_TASK_ID", "explicit-task");
    for (boolean deprecatedApi : new boolean[] {false, true}) {
      JsonObject settings = sessionSettings(hints, deprecatedApi);
      assertEquals("explicit-platform", settings.get("EXT_PLATFORM_ID").getAsString());
      assertEquals("explicit-task", settings.get("EXT_TASK_ID").getAsString());
      assertEquals(2, settings.size());
    }
    assertEquals(2, hints.size());
  }

  @Test
  public void testUserAgentIgnoresEnvironment() {
    RestClient client = new RestClient(null);
    client.setAccount(new AliyunAccount("dummy-id", "dummy-key"));
    client.setEndpoint("http://localhost");
    client.setUserAgent("custom");

    Request request = client.buildRequest("/projects/project", "GET", null, null);
    String expected = client.getUserAgent();
    assertTrue(expected.endsWith(" custom"));
    assertEquals(expected, request.getHeaders().get(Headers.USER_AGENT));
    assertEquals(expected, request.getHeaders().get(Headers.ODPS_USER_AGENT));
  }

  private Iterable<Task> tasks() {
    return Arrays.asList(new SQLTask(), new SQLRTTask(), new MergeTask("merge", "project.t"));
  }

  private void submit(Task task) throws OdpsException {
    task.setName("task");
    Odps odps = new Odps(new AliyunAccount("dummy-id", "dummy-key"));
    odps.client = new RestClient(null) {
      @Override
      public Response stringRequest(String resource, String method, Map<String, String> params,
                                    Map<String, String> headers, String body) {
        assertEquals("POST", method);
        return new CreatedResponse();
      }
    };
    new Instances(odps).create(task,
        new CreateInstanceOption.Builder().setProjectName("project").build());
  }

  @SuppressWarnings("deprecation")
  private JsonObject sessionSettings(Map<String, String> hints, boolean deprecatedApi)
      throws OdpsException {
    final String[] capturedBody = new String[1];
    Odps odps = new Odps(new AliyunAccount("dummy-id", "dummy-key"));
    Instance instance = new Instance("project", new Instance.TaskStatusModel(), null, odps) {
      @Override
      public SetInformationResult setInformation(String taskName, String infoKey, String infoValue) {
        assertEquals("task", taskName);
        assertEquals("query", infoKey);
        capturedBody[0] = infoValue;
        SetInformationResult result = new SetInformationResult();
        result.status = Session.SubQueryInfo.kOKCode;
        result.result = "{}";
        return result;
      }
    };
    Session session = new Session(null, instance, "session", "task");
    if (deprecatedApi) {
      session.run("select 1", hints);
    } else {
      session.runSubQuery("select 1", hints);
    }
    return JsonParser.parseString(capturedBody[0]).getAsJsonObject().getAsJsonObject("settings");
  }

  private static class CreatedResponse extends Response {
    CreatedResponse() {
      status = 201;
      headers.put(Headers.LOCATION, "/projects/project/instances/instance");
    }
  }
}
