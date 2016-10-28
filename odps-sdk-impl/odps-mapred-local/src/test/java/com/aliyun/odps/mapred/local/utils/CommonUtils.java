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

package com.aliyun.odps.mapred.local.utils;

import java.util.LinkedHashMap;

public class CommonUtils {

  public final static String GENERAL_INPUT_TABLE = "mr_empty";
  // public final static String GENERAL_OUTPUT_TABLE = "mr_wordcount_out";
  public final static String GENERAL_KEY_ROWSCHEMA = "key:string";
  public final static String GENERAL_VALUE_ROWSCHEMA = "value:string";

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
    }
  }

  public static LinkedHashMap<String, String> convertPartSpecToMap(
      String partSpec) {
    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
    if (partSpec != null && !partSpec.trim().isEmpty()) {
      String[] parts = partSpec.split("/");
      for (String part : parts) {
        String[] ss = part.split("=");
        if (ss.length != 2) {
          throw new RuntimeException("ODPS-0730001: error part spec format: "
                                     + partSpec);
        }
        map.put(ss[0], ss[1]);
      }
    }
    return map;
  }

}
