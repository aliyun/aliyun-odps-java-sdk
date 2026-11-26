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

package com.aliyun.odps.tracing;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/** Internal, immutable UA and trace configuration for Storage and Tunnel requests. */
public final class RequestTrace {
  private static final Logger LOG = Logger.getLogger(RequestTrace.class.getName());
  private static final Set<String> DIAGNOSED = ConcurrentHashMap.newKeySet();
  private static final RequestTrace EMPTY = new RequestTrace(null, null, null, false);
  private final String suffix;
  private final String parent;
  private final String state;
  private final boolean managed;

  private RequestTrace(String suffix, String parent, String state, boolean managed) {
    this.suffix = absent(suffix);
    this.parent = absent(parent);
    this.state = absent(state);
    this.managed = managed;
  }

  private static final class Environment {
    private static final RequestTrace VALUE = load();

    private static RequestTrace load() {
      try {
        return fromEnvironment(System.getenv());
      } catch (SecurityException e) {
        diagnose("env_unreadable");
        return EMPTY;
      }
    }
  }

  public static RequestTrace environment() {
    return Environment.VALUE;
  }

  public static RequestTrace fromEnvironment(Map<String, String> env) {
    return new RequestTrace(env.get("MC_USER_AGENT_SUFFIX"), env.get("TRACEPARENT"),
        env.get("TRACESTATE"), env.containsKey("TRACEPARENT") || env.containsKey("TRACESTATE"));
  }

  public static RequestTrace of(String suffix, String parent, String state, boolean managed) {
    return new RequestTrace(suffix, parent, state, managed);
  }

  public RequestTrace withSuffix(String value) {
    return new RequestTrace(value, parent, state, managed);
  }

  public RequestTrace withTrace(String newParent, String newState) {
    return new RequestTrace(suffix, newParent, newState, true);
  }

  public String getSuffix() {
    return suffix;
  }

  public String getParent() {
    return parent;
  }

  public String getState() {
    return state;
  }

  public boolean isManaged() {
    return managed;
  }

  /** Applies the configured trace before signing, without mutating the caller's header maps. */
  public void apply(Map<String, String> output, Map<String, String> defaults,
                    Map<String, String> supplied) {
    boolean conflict = false;
    for (String name : new String[] {"traceparent", "tracestate"}) {
      int count = count(defaults, name) + count(supplied, name);
      conflict |= count > 1 || (managed && count > 0);
    }
    if (managed || conflict) {
      for (String name : new ArrayList<>(output.keySet())) {
        if ("traceparent".equalsIgnoreCase(name) || "tracestate".equalsIgnoreCase(name)) {
          output.remove(name);
        }
      }
      if (conflict) {
        diagnose("header_conflict");
      } else {
        if (parent != null) {
          output.put("traceparent", parent);
        }
        if (state != null) {
          output.put("tracestate", state);
        }
      }
    }
    if (suffix != null) {
      String tail = " " + suffix;
      for (Map.Entry<String, String> header : output.entrySet()) {
        if ("User-Agent".equalsIgnoreCase(header.getKey())
            || "x-odps-user-agent".equalsIgnoreCase(header.getKey())) {
          String base = header.getValue();
          if (base != null && !base.equals(suffix) && !base.endsWith(tail)) {
            header.setValue(base + tail);
          }
        }
      }
    }
  }

  private static int count(Map<String, String> headers, String name) {
    int result = 0;
    if (headers != null) {
      for (String key : headers.keySet()) {
        if (name.equalsIgnoreCase(key)) {
          result++;
        }
      }
    }
    return result;
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
