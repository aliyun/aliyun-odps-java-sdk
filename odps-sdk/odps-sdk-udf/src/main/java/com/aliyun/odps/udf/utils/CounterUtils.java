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

package com.aliyun.odps.udf.utils;

import java.util.Map.Entry;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.CounterGroup;
import com.aliyun.odps.counter.Counters;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 与 Counter 相关的静态工具类，包含一些工具方法
 *
 * @see Counter
 * @see CounterGroup
 * @see Counters
 */
public class CounterUtils {

  /**
   * 将 {@link Counters} 对象转化成一个JSON字符串
   *
   * @param counters
   *     需要转化的 {@link Counters} 对象
   * @return JSON字符串
   */
  public static String toJsonString(Counters counters) {
    JsonObject js = toJson(counters);
    return js.toString();
  }

  /**
   * 通过JSON字符串创建{@link Counters}对象
   * 字符串异常时会抛出 {@link RuntimeException}
   *
   * @param json
   *     JSON字符串
   * @return 构建好的{@link Counters}对象
   * @see #toJsonString(Counters)
   */
  public static Counters createFromJsonString(String json) {
    JsonParser parser = new JsonParser();
    JsonElement el = parser.parse(json);
    return createFromJson(el.getAsJsonObject());
  }

  private static Counters createFromJson(JsonObject obj) {
    Counters counters = new Counters();
    for (Entry<String, JsonElement> entry : obj.entrySet()) {
      String key = entry.getKey();
      CounterGroup group = counters.getGroup(key);
      fromJson(group, entry.getValue().getAsJsonObject());
    }
    return counters;
  }

  private static void fromJson(CounterGroup group, JsonObject obj) {
    JsonArray counterArray = obj.get("counters").getAsJsonArray();
    for (int i = 0; i < counterArray.size(); i++) {
      JsonObject subObj = counterArray.get(i).getAsJsonObject();
      String counterName = subObj.get("name").getAsString();
      Counter counter = group.findCounter(counterName);
      long value = subObj.get("value").getAsLong();
      counter.increment(value);
    }
  }

  private static JsonObject toJson(Counters counters) {
    JsonObject obj = new JsonObject();
    for (CounterGroup group : counters) {
      obj.add(group.getName(), toJson(group));
    }
    return obj;
  }

  private static JsonObject toJson(CounterGroup counterGroup) {
    JsonObject obj = new JsonObject();
    obj.addProperty("name", counterGroup.getName());
    JsonArray counterArray = new JsonArray();
    for (Counter entry : counterGroup) {
      counterArray.add(toJson(entry));
    }
    obj.add("counters", counterArray);
    return obj;
  }

  private static JsonObject toJson(Counter counter) {
    JsonObject js = new JsonObject();
    js.addProperty("name", counter.getName());
    js.addProperty("value", counter.getValue());
    return js;
  }

}
