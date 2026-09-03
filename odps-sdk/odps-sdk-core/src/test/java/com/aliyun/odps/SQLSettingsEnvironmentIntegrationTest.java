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
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.task.SQLRTTask;
import com.aliyun.odps.task.SQLTask;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SQLSettingsEnvironmentIntegrationTest {

  @Test
  public void testSQLTaskMapsEnvironmentVariablesToSettings() {
    Task task = new SQLTask();
    task.setProperty("settings", "{\"EXT_PLATFORM_ID\":\"explicit-agent\"}");
    Map<String, String> environment = new HashMap<>();
    environment.put(SQLSettingsEnvironment.PLATFORM_ID_ENV, "environment-agent");
    environment.put(
        SQLSettingsEnvironment.TRACEPARENT_ENV,
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");

    task.loadEnvironmentSettings(SQLSettingsEnvironment.getSettings(environment));

    JsonObject settings = getSettings(task);
    assertEquals("explicit-agent", settings.get("EXT_PLATFORM_ID").getAsString());
    assertEquals(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        settings.get("EXT_TASK_ID").getAsString());
  }

  @Test
  public void testSQLRTTaskUsesEnvironmentSettings() {
    Task task = new SQLRTTask();
    Map<String, String> environmentSettings = new HashMap<>();
    environmentSettings.put("EXT_TASK_ID", "trace-123");

    task.loadEnvironmentSettings(environmentSettings);

    JsonObject settings = getSettings(task);
    assertEquals("trace-123", settings.get("EXT_TASK_ID").getAsString());
  }

  @Test
  public void testMergeSQLTaskUsesEnvironmentSettings() {
    Task task = new MergeTask("merge-task", "project.table");
    Map<String, String> environmentSettings = new HashMap<>();
    environmentSettings.put("EXT_TASK_ID", "run-123");

    task.loadEnvironmentSettings(environmentSettings);

    JsonObject settings = getSettings(task);
    assertEquals("run-123", settings.get("EXT_TASK_ID").getAsString());
  }

  @Test
  public void testNonSQLTaskDoesNotUseEnvironmentSettings() {
    Task task = new NonSQLTask();
    Map<String, String> environmentSettings = new HashMap<>();
    environmentSettings.put("EXT_TASK_ID", "run-123");

    task.loadEnvironmentSettings(environmentSettings);

    assertNull(task.getProperties().get("settings"));
  }

  @Test
  public void testSQLTaskIsUnchangedWithoutEnvironmentSettings() {
    Task task = new SQLTask();

    task.loadEnvironmentSettings(new HashMap<String, String>());

    assertNull(task.getProperties().get("settings"));
  }

  @Test
  public void testInvalidExistingSettingsArePreserved() {
    Task task = new SQLTask();
    task.setProperty("settings", "not-json");
    Map<String, String> environmentSettings = new HashMap<>();
    environmentSettings.put("EXT_PLATFORM_ID", "qwen-code");

    task.loadEnvironmentSettings(environmentSettings);

    assertEquals("not-json", task.getProperties().get("settings"));
  }

  private JsonObject getSettings(Task task) {
    return JsonParser.parseString(task.getProperties().get("settings")).getAsJsonObject();
  }

  private static class NonSQLTask extends Task {
  }
}
