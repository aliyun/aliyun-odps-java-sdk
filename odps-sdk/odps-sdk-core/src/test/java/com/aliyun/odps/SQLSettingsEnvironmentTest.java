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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SQLSettingsEnvironmentTest {

  private static final String TRACEPARENT =
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

  @Test
  public void testGetSettingsMapsFixedEnvironmentVariables() {
    Map<String, String> environment = new HashMap<>();
    environment.put("IGNORED", "value");
    environment.put("MC_EXT_NODE_ID", "must-not-be-expanded");
    environment.put(SQLSettingsEnvironment.PLATFORM_ID_ENV, "qwen-code");
    environment.put(SQLSettingsEnvironment.TRACEPARENT_ENV, TRACEPARENT);

    Map<String, String> settings = SQLSettingsEnvironment.getSettings(environment);

    assertEquals(2, settings.size());
    assertEquals("qwen-code", settings.get(SQLSettingsEnvironment.PLATFORM_ID_SETTING));
    assertEquals(TRACEPARENT, settings.get(SQLSettingsEnvironment.TASK_ID_SETTING));
    assertFalse(settings.containsKey("EXT_NODE_ID"));
  }

  @Test
  public void testExplicitSettingsTakePrecedence() {
    Map<String, String> environment = new HashMap<>();
    environment.put(SQLSettingsEnvironment.PLATFORM_ID_ENV, "environment-agent");
    environment.put(SQLSettingsEnvironment.TRACEPARENT_ENV, TRACEPARENT);
    Map<String, String> explicitSettings = new HashMap<>();
    explicitSettings.put("EXT_PLATFORM_ID", "explicit-agent");
    explicitSettings.put("EXT_TASK_ID", "explicit-task");

    Map<String, String> settings =
        SQLSettingsEnvironment.merge(explicitSettings, environment);

    assertEquals("explicit-agent", settings.get("EXT_PLATFORM_ID"));
    assertEquals("explicit-task", settings.get("EXT_TASK_ID"));
  }

  @Test
  public void testTraceparentIsPreservedWithoutParsing() {
    Map<String, String> environment = new HashMap<>();
    String traceparent = "not-parsed-by-sdk";
    environment.put(SQLSettingsEnvironment.TRACEPARENT_ENV, traceparent);

    Map<String, String> settings = SQLSettingsEnvironment.getSettings(environment);

    assertEquals(traceparent, settings.get(SQLSettingsEnvironment.TASK_ID_SETTING));
  }

  @Test
  public void testEmptyValuesAreIgnored() {
    Map<String, String> environment = new HashMap<>();
    environment.put(SQLSettingsEnvironment.PLATFORM_ID_ENV, "");
    environment.put(SQLSettingsEnvironment.TRACEPARENT_ENV, "");

    Map<String, String> settings = SQLSettingsEnvironment.getSettings(environment);

    assertEquals(0, settings.size());
  }
}
