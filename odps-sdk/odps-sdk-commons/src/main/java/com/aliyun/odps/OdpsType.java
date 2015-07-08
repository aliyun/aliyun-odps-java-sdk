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
   * 8字节有符号整型
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
  ARRAY;

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
