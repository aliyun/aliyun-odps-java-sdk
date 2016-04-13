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

package com.aliyun.odps.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import com.aliyun.odps.io.DatetimeWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Writable;

public class CommonUtils {

  private final static String[] ONE_STRING_ARRAY = new String[]{""};

  private final static String TIMESTAMP_FORMAT_S = "yyyyMMddHHmmss_SSS";

  /**
   * Get a date formatter for timestamp
   * @return new SimpleDateFormat("yyyyMMddHHmmss_SSS")
   */
  public static SimpleDateFormat getTimestampFormat() {
    return new SimpleDateFormat(TIMESTAMP_FORMAT_S);
  }

  /**
   * Please consider switch to getTimeStampFormat() which is thread safe.
   */
  @Deprecated
  public final static SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat(
      TIMESTAMP_FORMAT_S);

  private final static NullWritable nullWritable = NullWritable.get();

  private final static String DATETIME_FORMAT_S = "yyyy-MM-dd HH:mm:ss";
  /**
   * Get a date formatter for datetime
   * @return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
   */
  public static SimpleDateFormat getDatetimeFormat() {
    return new SimpleDateFormat(DATETIME_FORMAT_S);
  }
  /**
   * Please consider switch to getDatetimeFormat() which is thread safe.
   */
  @Deprecated
  public final static SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat(
      DATETIME_FORMAT_S);

  public static long getPID() {
    String processName = java.lang.management.ManagementFactory
        .getRuntimeMXBean().getName();
    return Long.parseLong(processName.split("@")[0]);
  }

  public static String generateMrTaskName() {
    return "console_mr_" + getTimestampFormat().format(new Date());
  }

  public static String generateLocalMrTaskName() {
    return "mr_" + getTimestampFormat().format(new Date()) + "_" + getPID();
  }

  public static String generateGraphTaskName() {
    return "console_graph_" + getTimestampFormat().format(new Date());
  }

  public static String generateLocalGraphTaskName() {
    return "graph_" + getTimestampFormat().format(new Date()) + "_" + getPID();
  }

  public static String getCurrentTime() {
    return getDatetimeFormat().format(new Date());
  }

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
    }
  }

  public static String toDelimitedString(Writable[] fields, char delim,
                                         String nullIndicator) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < fields.length; i++) {
      if (i != 0) {
        buf.append(delim);
      }
      Writable field = fields[i];
      if (field == null || field == nullWritable) {
        buf.append(nullIndicator);
      } else {
        buf.append(field.toString());
      }
    }
    return buf.toString();
  }


  private static Writable parseDateTime(String val) throws IOException {
    try {
      return new DatetimeWritable(Long.parseLong(val));
    } catch (NumberFormatException e) {
      try {
        return new DatetimeWritable(getDatetimeFormat().parse(val).getTime());
      } catch (ParseException ex) {
        throw new IOException("unsupported date time format:" + val);
      }
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

  public static String convertPartSpecMapToString(LinkedHashMap<String, String> partSpecMap) {
    String partSpec = "";
    if (partSpecMap != null) {
      for (Map.Entry<String, String> entry : partSpecMap.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          throw new RuntimeException(
              "ODPS-0730001: partition name or value can't be null: "
              + entry.getKey() + "=" + entry.getValue());
        }
        String part = entry.getKey().trim() + "=" + entry.getValue().trim();
        partSpec += (partSpec.isEmpty()) ? part : "/" + part;
      }
    }
    return partSpec;
  }

  public static void checkArgument(String name, int value, int lower_bound,
                                   int upper_bound) {
    if (value < lower_bound || value > upper_bound) {
      throw new RuntimeException("Local Run: Value of " + name
                                 + " out of bound, must be in range [" + lower_bound + ","
                                 + upper_bound + "].");
    }
  }


}
