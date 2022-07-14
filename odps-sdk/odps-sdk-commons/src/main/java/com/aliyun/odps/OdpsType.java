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

import java.util.List;

/**
 * ODPS表支持的字段类型
 */
public enum OdpsType {
  /**
   * 8字节有符号整型
   */
  BIGINT,

  /**
   * 双精度浮点
   */
  DOUBLE,

  /**
   * 布尔型
   */
  BOOLEAN,

  /**
   * 日期类型
   */
  DATETIME,

  /**
   * 字符串类型
   */
  STRING,

  /**
   * 精确小数类型
   */
  DECIMAL,

  /**
   * MAP类型
   */
  MAP,

  /**
   * ARRAY类型
   */
  ARRAY,

  /**
   * 空
   */
  VOID,

  /**
   * 1字节有符号整型
   */
  TINYINT,

  /**
   * 2字节有符号整型
   */
  SMALLINT,

  /**
   * 4字节有符号整型
   */
  INT,

  /**
   * 单精度浮点
   */
  FLOAT,

  /**
   * 固定长度字符串
   */
  CHAR,

  /**
   * 可变长度字符串
   */
  VARCHAR,

  /**
   * 时间类型
   */
  DATE,

  /**
   * 时间戳
   */
  TIMESTAMP,

  /**
   * 字节数组
   */
  BINARY,

  /**
   * 日期间隔
   */
  INTERVAL_DAY_TIME,

  /**
   * 年份间隔
   */
  INTERVAL_YEAR_MONTH,

  /**
   * 结构体
   */
  STRUCT,

  /**
   * JSON类型
   */
  JSON,

  /**
   * Unsupported types from external systems
   */
  UNKNOWN;

  @Deprecated
  public static String getFullTypeString(OdpsType type, List<OdpsType> genericTypeList) {
    StringBuilder sb = new StringBuilder();
    sb.append(type.toString());
    if (genericTypeList != null && genericTypeList.size() != 0) {
      sb.append("<");
      for (OdpsType genericType : genericTypeList) {
        sb.append(genericType.toString()).append(",");
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.append(">");
    }
    return sb.toString();
  }
}
