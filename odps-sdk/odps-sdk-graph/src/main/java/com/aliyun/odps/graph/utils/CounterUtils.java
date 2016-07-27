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

package com.aliyun.odps.graph.utils;

import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.CounterGroup;
import com.aliyun.odps.counter.Counters;

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
    JSONObject js = toJson(counters);
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
    JSONObject el = JSON.parseObject(json);
    return createFromJson(el);
  }

  private static Counters createFromJson(JSONObject obj) {
    Counters counters = new Counters();
    for (Entry<String, Object> entry : obj.entrySet()) {
      String key = entry.getKey();
      CounterGroup group = counters.getGroup(key);
      fromJson(group, (JSONObject) entry.getValue());
    }
    return counters;
  }

  private static void fromJson(CounterGroup group, JSONObject obj) {
    JSONArray counterArray = obj.getJSONArray("counters");
    for (int i = 0; i < counterArray.size(); i++) {
      JSONObject subObj = counterArray.getJSONObject(i);
      String counterName = subObj.getString("name");
      Counter counter = group.findCounter(counterName);
      long value = subObj.getLongValue("value");
      counter.increment(value);
    }
  }

  private static JSONObject toJson(Counters counters) {
    JSONObject obj = new JSONObject(true);
    for (CounterGroup group : counters) {
      obj.put(group.getName(), toJson(group));
    }
    return obj;
  }

  private static JSONObject toJson(CounterGroup counterGroup) {
    JSONObject obj = new JSONObject(true);
    obj.put("name", counterGroup.getName());
    JSONArray counterArray = new JSONArray();
    for (Counter entry : counterGroup) {
      counterArray.add(toJson(entry));
    }
    obj.put("counters", counterArray);
    return obj;
  }

  private static JSONObject toJson(Counter counter) {
    JSONObject js = new JSONObject(true);
    js.put("name", counter.getName());
    js.put("value", counter.getValue());
    return js;
  }

}
