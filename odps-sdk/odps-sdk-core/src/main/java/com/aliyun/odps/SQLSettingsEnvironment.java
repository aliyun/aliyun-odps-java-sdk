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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Loads default SQL task settings from environment variables.
 *
 * <p>{@code MC_PLATFORM_ID} is converted to {@code EXT_PLATFORM_ID}, and {@code TRACEPARENT} is
 * converted to {@code EXT_TASK_ID}.
 *
 * <p>Environment settings are defaults. Settings explicitly supplied by the caller take
 * precedence.
 */
final class SQLSettingsEnvironment {

  static final String PLATFORM_ID_ENV = "MC_PLATFORM_ID";
  static final String TRACEPARENT_ENV = "TRACEPARENT";

  static final String PLATFORM_ID_SETTING = "EXT_PLATFORM_ID";
  static final String TASK_ID_SETTING = "EXT_TASK_ID";

  private SQLSettingsEnvironment() {
  }

  static Map<String, String> getSettings() {
    return getSettings(System.getenv());
  }

  static Map<String, String> merge(Map<String, String> settings) {
    return merge(settings, System.getenv());
  }

  static Map<String, String> merge(Map<String, String> settings,
                                   Map<String, String> environment) {
    Map<String, String> merged = getSettings(environment);
    if (settings != null) {
      merged.putAll(settings);
    }
    return merged;
  }

  static Map<String, String> getSettings(Map<String, String> environment) {
    Map<String, String> settings = new LinkedHashMap<>();
    String platformId = environment.get(PLATFORM_ID_ENV);
    if (platformId != null && !platformId.isEmpty()) {
      settings.put(PLATFORM_ID_SETTING, platformId);
    }

    String traceparent = environment.get(TRACEPARENT_ENV);
    if (traceparent != null && !traceparent.isEmpty()) {
      settings.put(TASK_ID_SETTING, traceparent);
    }
    return settings;
  }
}
