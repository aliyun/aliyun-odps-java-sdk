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

package com.aliyun.odps.graph;

import java.io.IOException;

/**
 * ODPS 数据类型.
 *
 * <p>
 * 当前定义了如下类型：
 * <ul>
 * <li>INTEGER
 * <li>DOUBLE
 * <li>BOOLEAN
 * <li>STRING
 * <li>DATETIME
 * </ul>
 * </p>
 */
public class DataType {

  public final static byte INTEGER = 0;
  public final static byte DOUBLE = 1;
  public final static byte BOOLEAN = 2;
  public final static byte STRING = 3;
  public final static byte DATETIME = 4;

  /**
   * 字符串的数据类型转换为byte常量定义的数据类型.
   *
   * <p>
   * 转换规则：
   * <ul>
   * <li>tinyint, int, bigint, long - {@link #INTEGER}
   * <li>double, float - {@link #DOUBLE}
   * <li>string - {@link #STRING}
   * <li>boolean, bool - {@link #BOOLEAN}
   * <li>datetime - {@link #DATETIME}
   * </ul>
   * </p>
   *
   * @param type
   *     字符串的数据类型
   * @return byte常量定义的数据类型
   * @throws IOException
   */
  public static byte convertToDataType(String type) throws IOException {
    type = type.toLowerCase().trim();
    if ("string".equals(type)) {
      return STRING;
    } else if ("bigint".equals(type) || "int".equals(type)
               || "tinyint".equals(type) || "long".equals(type)) {
      return INTEGER;
    } else if ("boolean".equals(type) || "bool".equals(type)) {
      return BOOLEAN;
    } else if ("double".equals(type) || "float".equals(type)) {
      return DOUBLE;
    } else if ("datetime".equals(type)) {
      return DATETIME;
    } else {
      throw new IOException("unkown type: " + type);
    }
  }

}
