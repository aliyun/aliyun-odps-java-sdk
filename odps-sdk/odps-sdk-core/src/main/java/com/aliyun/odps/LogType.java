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

import java.util.HashMap;
import java.util.Map;

/**
 * 日志类型。
 *
 * @author xiaoming.yin
 */
public enum LogType {

  /**
   * 标准输出日志。
   */
  STDOUT("stdout"),

  /**
   * 标准错误日志。
   */
  STDERR("stderr");

  private static final Map<String, LogType> strToEnum = new HashMap<String, LogType>();

  static { // Initialize the map
    for (LogType t : values()) {
      strToEnum.put(t.toString(), t);
    }
  }

  private String strVal;

  private LogType(String strVal) {
    this.strVal = strVal;
  }

  /**
   * 设置日志类型
   *
   * @param value值包括："stdout",
   *     "stderr"
   */
  public static LogType fromString(String value) {
    if (value == null) {
      throw new NullPointerException();
    }
    return strToEnum.get(value);
  }

  @Override
  public String toString() {
    return this.strVal;
  }
}
