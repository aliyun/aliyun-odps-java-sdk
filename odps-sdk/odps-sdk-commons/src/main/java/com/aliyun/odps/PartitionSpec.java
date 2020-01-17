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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PartitionSpec类表示一个特定分区的定义
 */
public class PartitionSpec {

  private static final String COMMA = ",";
  private static final String SLASH = "/";

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
   * 分区定义字符串，分区列之间可以用逗号 (",") 或斜线 ("/") 分隔. 例如: "pt1=foo/pt2=1" 或 "pt1=foo,pt2=1"
   */
  public PartitionSpec(String spec) {
    if (spec == null) {
      throw new IllegalArgumentException("Argument \'spec\' cannot be null");
    }
    String[] groups = spec.split("[,/]");
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
   * Convert a {@link PartitionSpec} object to {@link String}. Partition column values are quoted
   * with single quotation marks. Partition column name and value pairs are separated with comma.
   *
   * @return partition spec in string.
   */
  @Override
  public String toString() {
    return toString(true, false);
  }

  /**
   * Convert a {@link PartitionSpec} object to {@link String}. The delimiter of partition column
   * name and value pairs can be comma (",") or slash ("/").
   *
   * @param quote quote the partition column value
   * @param useSlashDelimiter use comma as delimiter
   * @return
   */
  public String toString(boolean quote, boolean useSlashDelimiter) {
    String delimiter = useSlashDelimiter ? SLASH : COMMA;

    List<String> entries = new LinkedList<>();
    String[] keys = keys().toArray(new String[0]);

    for (String key : keys) {
      StringBuilder entryBuilder = new StringBuilder();
      entryBuilder.append(key).append("=");
      if (quote) {
        entryBuilder.append("\'").append(kv.get(key)).append("\'");
      } else {
        entryBuilder.append(kv.get(key));
      }
      entries.add(entryBuilder.toString());
    }

    return String.join(delimiter, entries);
  }
}
