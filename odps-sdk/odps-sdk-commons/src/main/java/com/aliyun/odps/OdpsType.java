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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ODPS表支持的字段类型
 */
public enum OdpsType implements Serializable {
  /**
   * 8字节有符号整型
   */
  BIGINT(0),

  /**
   * 双精度浮点
   */
  DOUBLE(1),

  /**
   * 布尔型
   */
  BOOLEAN(2),

  /**
   * 日期类型
   */
  DATETIME(3),

  /**
   * 字符串类型
   */
  STRING(4),

  /**
   * 精确小数类型
   */
  DECIMAL(5),

  /**
   * 1字节有符号整型
   */
  TINYINT(6),

  /**
   * 2字节有符号整型
   */
  SMALLINT(7),

  /**
   * 4字节有符号整型
   */
  INT(8),

  /**
   * 固定长度字符串
   */
  CHAR(9),

  /**
   * 可变长度字符串
   */
  VARCHAR(10),
  /**
   * 字节数组
   */
  BINARY(11),

  /**
   * 时间类型
   */
  DATE(12),

  /**
   * 时间戳
   */
  TIMESTAMP(13),

  /**
   * 单精度浮点
   */
  FLOAT(14),

  /**
   * 年份间隔
   */
  INTERVAL_YEAR_MONTH(15),

  /**
   * 日期间隔
   */
  INTERVAL_DAY_TIME(16),

  /**
   * ARRAY类型
   */
  ARRAY(17),

  /**
   * MAP类型
   */
  MAP(18),

  /**
   * 结构体
   */
  STRUCT(19),


  /**
   * JSON类型
   */
  JSON(20),

  /**
   * 时区无关的时间戳
   */
  TIMESTAMP_NTZ(21),

  /**
   * blob 类型
   */
  BLOB(22),

  /**
   * 半结构化类型
   */
  VARIANT(23),

  /**
   * 空
   */
  VOID(-3),

  /**
   * 地理类型
   */
  GEOGRAPHY(-2),

  /**
   * 向量类型
   */
  VECTOR(-4),


  /**
   * Unsupported types from external systems
   */
  UNKNOWN(-1);


  private final int value;
  private static final Map<Integer, OdpsType> intToTypeMap =
    Arrays.stream(values()).collect(Collectors.toMap(OdpsType::getValue, Function.identity()));

  OdpsType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static OdpsType fromInt(int i) {
    return intToTypeMap.getOrDefault(i, UNKNOWN);
  }

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
