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
import java.util.Set;

/**
 * PartitionSpec类表示一个特定分区的定义
 */
public class PartitionSpec {

  private Map<String, String> kv = new LinkedHashMap<String, String>();

  /**
   * 构造此类的对象
   */
  public PartitionSpec() {
  }

  /**
   * 通过字符串构造此类对象
   *
   * @param spec
   *     分区定义字符串，比如: pt='1',ds='2'
   */
  public PartitionSpec(String spec) {
    if (spec == null) {
      throw new IllegalArgumentException();
    }
    String[] groups = spec.split(",");
    for (String group : groups) {
      String[] kv = group.split("=");
      if (kv.length != 2) {
        throw new IllegalArgumentException("Invalid partition spec.");
      }

      String k = kv[0].trim();
      String v = kv[1].trim().replaceAll("'", "").replaceAll("\"", "");
      if (k.length() == 0 || v.length() == 0) {
        throw new IllegalArgumentException("Invalid partition spec.");
      }

      set(k, v);
    }
  }

  /**
   * 设置分区字段值
   *
   * @param key
   *     分区字段名
   * @param value
   *     分区字段值
   */
  public void set(String key, String value) {
    kv.put(key, value);
  }

  /**
   * 获得指定分区字段值
   *
   * @param key
   *     分区字段名
   * @return 分区字段值
   */
  public String get(String key) {
    return kv.get(key);
  }

  /**
   * 获取所有分区字段
   *
   * @return 分区字段名集合
   */
  public Set<String> keys() {
    return kv.keySet();
  }

  /**
   * 是否指定了分区字段
   *
   * @return 如果指定了分区字段，则返回false，否则返回true
   */
  public boolean isEmpty() {
    return kv.isEmpty();
  }

  /**
   * 返回PartitionSpec的字符串表示, 如: pt='2014',ds='03'
   *
   * @return 分区定义的字符串表示
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    String[] keys = keys().toArray(new String[0]);
    for (int i = 0; i < keys.length; i++) {
      sb.append(keys[i]).append("='").append(get(keys[i])).append("'");

      if (i + 1 < keys.length) {
        sb.append(',');
      }
    }

    return sb.toString();
  }
}
