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

package com.aliyun.odps.local.common.utils;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;

public class PartitionUtils {

  public static PartitionSpec convert(String[] partitions) {
    PartitionSpec partitionSpec = null;
    if (partitions != null && partitions.length > 0) {
      partitionSpec = new PartitionSpec();
      for (String p : partitions) {
        String[] kv = p.split("=");
        if (kv.length != 2) {
          continue;
        }
        partitionSpec.set(kv[0], kv[1]);
      }
    }
    return partitionSpec;
  }

  public static PartitionSpec convert(Map<String, String> hash) {
    if (hash == null || hash.size() == 0) {
      return null;
    }
    StringBuffer sb = new StringBuffer();
    for (String key : hash.keySet()) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(key + "=" + hash.get(key));
    }
    return new PartitionSpec(sb.toString());
  }

  public static LinkedHashMap<String, String> convert(PartitionSpec partSpec) {
    if (partSpec == null || partSpec.isEmpty()) {
      return null;
    }
    LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
    for (String key : partSpec.keys()) {
      result.put(key, partSpec.get(key));
    }
    return result;

  }

  public static String toString(Map<String, String> partSpec) {
    if (partSpec == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (String k : partSpec.keySet()) {
      if (sb.length() > 0) {
        sb.append('/');
      }
      sb.append(k).append('=').append(partSpec.get(k));
    }
    return sb.toString();
  }

  public static String toString(PartitionSpec partSpec) {
    if (partSpec == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (String k : partSpec.keys()) {
      if (sb.length() > 0) {
        sb.append('/');
      }
      sb.append(k).append('=').append(partSpec.get(k));
    }
    return sb.toString();
  }

  public static PartitionSpec convert(String partPath) {
    PartitionSpec spec = new PartitionSpec();
    for (String kv : partPath.split("/|\\\\")) {
      String[] p = kv.trim().split("=", 2);
      if (p.length == 2) {
        spec.set(p[0], p[1]);
      }
    }
    return spec;
  }

  public static boolean valid(Column[] partitionsScheme, PartitionSpec partSpec) {
    if (partSpec == null || partitionsScheme == null || partitionsScheme.length == 0) {
      return false;
    }

    for (String key : partSpec.keys()) {
      boolean flag = false;
      for (int i = 0; !flag && i < partitionsScheme.length; i++) {
        if (partitionsScheme[i].getName().equals(key)) {
          flag = true;
        }
      }
      if (!flag) {
        return false;
      }
    }
    return true;
  }

  public static boolean isEqual(PartitionSpec p1, PartitionSpec p2) {
    if (p1 == null && p2 == null) {
      return true;
    } else if (p1 == null || p2 == null) {
      return false;
    }

    if (p1.keys().size() != p2.keys().size()) {
      return false;
    }
    boolean flag = true;
    for (String key : p1.keys()) {
      if (p1.get(key) == null || !p1.get(key).equals(p2.get(key))) {
        flag = false;
        break;
      }
    }
    return flag;
  }

  /**
   * examples:
   *
   * pattern parts return
   *
   * null p1=1/p2=1 true p1=1 p1=1/p2=1 true p1=1/p2=1 p1=1/p2=1 true p1=1/p2=2
   * p1=1/p2=1 false p1=1/p2=2/p3=1 p1=1/p2=1 false
   */

  public static boolean match(PartitionSpec pattern, PartitionSpec parts) {
    if (pattern == null || pattern.isEmpty()) {
      return true;
    }
    if (parts == null || pattern.keys().size() > parts.keys().size()) {
      return false;
    }
    for (String key : pattern.keys()) {
      String expectedValue = pattern.get(key);
      if (expectedValue == null) {
        continue;
      }
      String value = parts.get(key);
      if (!expectedValue.equals(value)) {
        return false;
      }
    }
    return true;
  }

}
