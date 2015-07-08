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

package com.aliyun.odps.mapred.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;

/**
 * 表达MapReduce任务行属性的工具类
 */
public class SchemaUtils {

  private static final String SEPERATOR = ",";
  private static final String DELIMITER = ":";

  /**
   * 从字符串描述解析行属性。其中行属性的分隔符是','，字段分隔符为':'。例如，"word:string,count:bigint"表示两列的一行，
   * 其中第一列名称为word，类型为string；第二列名称为count，类型为bigint。
   *
   * @param str
   *     字符串描述
   * @return 行属性
   * @see #toString()
   */
  public static Column[] fromString(String str) {
    if (str == null || str.isEmpty()) {
      return new Column[0];
    }
    String[] kv = str.split(SEPERATOR);
    Column[] cols = new Column[kv.length];
    for (int i = 0; i < kv.length; i++) {
      String[] knv = kv[i].split(DELIMITER, 2);
      if (knv.length != 2) {
        throw new IllegalArgumentException(
            "Malformed schema definition, expecting \"name:type\" but was \"" + kv[i] + "\"");
      }
      cols[i] = new Column(knv[0].trim(), OdpsType.valueOf(knv[1].trim().toUpperCase()));
    }
    return cols;
  }

  /**
   * 行属性序列化为描述字符串
   *
   * @param cols
   *     行属性
   * @return 描述字符串
   * @see #fromString(String)
   */
  public static String toString(Column[] cols) {
    if (cols == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (Column c : cols) {
      if (c == null) {
        continue;
      }
      if (sb.length() > 0) {
        sb.append(SEPERATOR);
      }
      sb.append(c.getName()).append(DELIMITER).append(c.getType().toString());
    }
    return sb.toString();
  }

  /**
   * 获取行属性的名称数组
   *
   * @param cols
   *     行属性
   * @return 名称数组
   */
  public static String[] getNames(Column[] cols) {
    String[] names = new String[cols.length];
    for (int i = 0; i < cols.length; i++) {
      names[i] = cols[i].getName();
    }
    return names;
  }

  /**
   * 获取行属性的类型数组
   *
   * @param cols
   *     行属性
   * @return 类型数组
   */
  public static OdpsType[] getTypes(Column[] cols) {
    OdpsType[] types = new OdpsType[cols.length];
    for (int i = 0; i < cols.length; i++) {
      types[i] = cols[i].getType();
    }
    return types;
  }

}
