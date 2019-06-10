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

package com.aliyun.odps.commons.util;

import java.util.Map;

import com.aliyun.odps.utils.GsonObjectBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Cost计费预测模式结果解析器
 *
 * @author zhenyi.zzy
 */
public class CostResultParser {

  private static String LINE_SEPARATOR = System.getProperty("line.separator");

  /**
   * 对约定的Cost结果进行解析，若解析失败则直接将原结果返回;若解析成功则返回解析后的内容
   * 约定格式为：
   * <pre>
   *     {
   *        "Cost": {
   *           <taskType>: {
   *             <k1>: <v1>,
   *             <k2>: <v2>,
   *             ...
   *           }
   *        }
   *     }
   * </pre>
   * 例如SQL的result为:
   * <pre>
   *     {
   *        "Cost": {
   *           "SQL": {
   *             "Input": "1840",
   *             "UDF": "0",
   *             "Complexity": "1.0"
   *           }
   *        }
   *     }
   * </pre>
   * @param result
   * @param taskType
   * @return
   */
  public static String parse(String result, String taskType) {
    Map<String, Object> node = null;
    try {
      Gson gson = GsonObjectBuilder.get();
      Map cost = gson.fromJson(result, Map.class);
      node = (Map) ((Map) cost.get("Cost")).get(taskType);
    } catch (Exception e) {
      return result;
    }
    if (node == null) {
      return result;
    }
    StringBuffer sb = new StringBuffer();

    for (Map.Entry<String, Object> entry : node.entrySet()) {
      if ("Input".equalsIgnoreCase(entry.getKey())) {
        sb.append(String.format("%s:%s Bytes", entry.getKey(), entry.getValue()))
            .append(LINE_SEPARATOR);
      } else {
        sb.append(String.format("%s:%s", entry.getKey(), entry.getValue())).append(LINE_SEPARATOR);
      }
    }
    
    if (sb.toString().isEmpty()) {
      return result;
    } else {
      return sb.toString();
    }
  }

}
