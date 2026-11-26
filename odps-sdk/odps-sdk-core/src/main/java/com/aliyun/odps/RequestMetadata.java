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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.aliyun.odps.tracing.RequestTrace;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Internal immutable request metadata. Applications configure {@link OdpsOptions} instead.
 *
 * <p>This class is public only for use by SDK subpackages; it is not an application context API.
 */
public final class RequestMetadata {
  private static final Logger LOG = Logger.getLogger(RequestMetadata.class.getName());
  private static final Set<String> DIAGNOSED = ConcurrentHashMap.newKeySet();
  private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
  private static final int CALL = 1;
  private static final int TRACE = 2;
  private static final RequestMetadata EMPTY = new RequestMetadata(null, null, null, null, 0);
  private final String suffix;
  private final String call;
  private final String parent;
  private final String state;
  private final int managed;
  private final RequestTrace storageTrace;

  private RequestMetadata(String suffix, String call, String parent, String state, int managed) {
    this.suffix = suffix;
    this.call = call;
    this.parent = parent;
    this.state = state;
    this.managed = managed;
    this.storageTrace = RequestTrace.of(suffix, parent, state, (managed & TRACE) != 0);
  }

  private static class Environment {
    private static final RequestMetadata VALUE = load();

    private static RequestMetadata load() {
      try {
        return fromEnvironment(System.getenv());
      } catch (SecurityException e) {
        diagnose("env_unreadable");
        return EMPTY;
      }
    }
  }

  static RequestMetadata environment() {
    return Environment.VALUE;
  }

  static RequestMetadata empty() {
    return EMPTY;
  }

  /** Internal: read one immutable configuration for HTTP header assembly. */
  public static RequestMetadata capture(OdpsOptions options) {
    return options == null ? EMPTY : options.requestMetadata;
  }

  /** Internal: retain the UA and trace groups while excluding the Tunnel-incompatible call header. */
  public RequestTrace forStorage() {
    return storageTrace;
  }

  static RequestMetadata fromEnvironment(Map<String, String> env) {
    RequestMetadata value = EMPTY.withSuffix(env.get("MC_USER_AGENT_SUFFIX"));
    if (env.containsKey("MC_CALL_CONTEXT")) {
      value = value.withJson(env.get("MC_CALL_CONTEXT"));
    }
    if (env.containsKey("TRACEPARENT") || env.containsKey("TRACESTATE")) {
      value = value.withTrace(env.get("TRACEPARENT"), env.get("TRACESTATE"));
    }
    return value;
  }

  RequestMetadata withSuffix(String value) {
    return new RequestMetadata(absent(value), call, parent, state, managed);
  }

  RequestMetadata withMap(Map<String, String> values) {
    if (values == null || values.isEmpty()) {
      return withJson(null);
    }
    Map<String, Object> envelope = new LinkedHashMap<>();
    envelope.put("version", 1);
    envelope.put("extensions", values);
    return withJson(JSON.toJson(envelope));
  }

  RequestMetadata withJson(String value) {
    String header = absent(value) == null ? null : Base64.getUrlEncoder().withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    return new RequestMetadata(suffix, header, parent, state, managed | CALL);
  }

  RequestMetadata withTrace(String newParent, String newState) {
    return new RequestMetadata(suffix, call, absent(newParent), absent(newState), managed | TRACE);
  }

  /** Internal: apply only before signing, without mutating caller-owned raw header maps. */
  public void apply(
      Map<String, String> output, Map<String, String> defaults, Map<String, String> supplied) {
    applyGroup(
        output,
        defaults,
        supplied,
        CALL,
        new String[] {"x-odps-call-context"},
        new String[] {call});
    storageTrace.apply(output, defaults, supplied);
  }

  private void applyGroup(
      Map<String, String> output,
      Map<String, String> defaults,
      Map<String, String> supplied,
      int flag,
      String[] names,
      String[] values) {
    boolean conflict = false;
    for (String name : names) {
      int count = count(defaults, name) + count(supplied, name);
      conflict |= count > 1 || ((managed & flag) != 0 && count > 0);
    }
    if (conflict || (managed & flag) != 0) {
      for (String key : new ArrayList<>(output.keySet())) {
        for (String name : names) {
          if (name.equalsIgnoreCase(key)) {
            output.remove(key);
          }
        }
      }
      if (conflict) {
        diagnose("header_conflict");
      } else {
        for (int i = 0; i < names.length; i++) {
          if (values[i] != null) {
            output.put(names[i], values[i]);
          }
        }
      }
    }
  }

  private static int count(Map<String, String> headers, String name) {
    int count = 0;
    if (headers != null) {
      for (String key : headers.keySet()) {
        if (name.equalsIgnoreCase(key)) {
          count++;
        }
      }
    }
    return count;
  }

  private static String absent(String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  private static void diagnose(String reason) {
    if (DIAGNOSED.add(reason)) {
      LOG.warning("Request metadata: " + reason);
    }
  }
}
