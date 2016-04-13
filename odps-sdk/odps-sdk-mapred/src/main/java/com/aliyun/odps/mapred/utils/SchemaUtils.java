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

import java.util.ArrayList;
import java.util.Arrays;

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
    String remain = str;
    ArrayList<Column> cols = new ArrayList<Column>();
    int pos;
    while (remain.length() > 0) {
      pos = remain.indexOf(SEPERATOR);
      if (pos < 0) {
        // Last one
        pos = remain.length();
      }
      String tok = remain.substring(0, pos);
      String[] knv = tok.split(DELIMITER, 2);
      if (knv.length != 2) {
        throw new IllegalArgumentException(
                "Malformed schema definition, expecting \"name:type\" but was \"" + tok + "\"");
      }
      if (knv[1].toUpperCase().startsWith("MAP")) {
        // We need to find the next SEPARATOR
        pos = remain.indexOf(SEPERATOR, pos + 1);
        if (pos < 0) {
          pos = remain.length();
        }
        tok = remain.substring(0, pos);
        // Re-split
        knv = tok.split(DELIMITER, 2);
      }
      if (pos == remain.length()) {
        remain = "";
      } else {
        remain = remain.substring(pos + 1);
      }
      cols.add(getColumn(knv[0], knv[1]));
    }
    return cols.toArray(new Column[]{});
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
      sb.append(c.getName()).append(DELIMITER).append(getOdpsTypeString(c));
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

  private static String getOdpsTypeString(Column tp) {
    StringBuilder sb = new StringBuilder();
    if (tp.getType() == OdpsType.ARRAY) {
      sb.append("ARRAY<");
      sb.append(tp.getGenericTypeList().get(0).toString());
      sb.append(">");
    } else if (tp.getType() == OdpsType.MAP) {
      sb.append("MAP<");
      sb.append(tp.getGenericTypeList().get(0).toString());
      sb.append(",");
      sb.append(tp.getGenericTypeList().get(1).toString());
      sb.append(">");
    } else {
      sb.append(tp.getType().toString());
    }
    return sb.toString();
  }

  private static Column getColumn(String name, String typeStr) {
    name = name.trim();
    typeStr = typeStr.toUpperCase().trim();
    if (typeStr.startsWith("ARRAY<")) {
      String remain = typeStr.substring(6);
      if (remain.length() < 2 || remain.charAt(remain.length() -1) != '>') {
        throw new IllegalArgumentException("Malformed schema, not a valid array type: " + typeStr);
      }
      OdpsType elementType = OdpsType.valueOf(remain.substring(0, remain.length()-1));
      Column col = new Column(name, OdpsType.ARRAY);
      col.setGenericTypeList(Arrays.asList(elementType));
      return col;
    } else if (typeStr.startsWith("MAP<")) {
      String remain = typeStr.substring(4);
      if (remain.length() < 4 || remain.charAt(remain.length() - 1) != '>') {
        throw new IllegalArgumentException("Malformed schema, not a valid map type: " + typeStr);
      }
      remain = remain.substring(0, remain.length() -1);
      String[] knv = remain.split(",", 2);
      if (knv.length != 2) {
        throw new IllegalArgumentException(
                "Malformed schema , not a valid map type: " + typeStr);
      }
      Column col = new Column(name, OdpsType.MAP);
      col.setGenericTypeList(Arrays.asList(OdpsType.valueOf(knv[0]), OdpsType.valueOf(knv[1])));
      return col;
    } else {
      return new Column(name, OdpsType.valueOf(typeStr));
    }
  }

}
