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

package com.aliyun.odps.local.common.utils;

import com.aliyun.odps.local.common.AnyTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.ColumnOrConstant;
import com.aliyun.odps.local.common.ExceptionCode;

public class SchemaUtils {

  private static final Log LOG = LogFactory.getLog(SchemaUtils.class);
  private static final String KEY_VALUE_SEPARATOR = "=";
  private static final String NAME_TYPE_DELIMITER = ":";
  private static final String COLUMN_SEPARATOR = ",";
  private static final String RESOLVE_SEPARATOR = ",";
  private static final String CONSTANT_TYPE_DELIMITER = "\\|";

  public static boolean existsSchemaFile(File dir) {
    return new File(dir, Constants.SCHEMA_FILE).exists();
  }

  public static void generateSchemaFile(TableMeta table, List<Integer> indexes, File dir) {

    if (table == null || dir == null) {
      throw new IllegalArgumentException("Missing arguments: table|dir");
    }

    if (StringUtils.isBlank(table.getProjName()) || StringUtils.isBlank(table.getTableName())) {
      throw new IllegalArgumentException(
          "Project|table is empty when table.getProjName()|table.getTableName()");
    }

    TableInfo tableInfo = TableInfo.builder().projectName(table.getProjName())
        .tableName(table.getTableName()).build();

    LOG.info("Start to write table scheme : " + tableInfo + "-->" + dir.getAbsolutePath());

    StringBuffer sb = new StringBuffer();
    sb.append("project" + KEY_VALUE_SEPARATOR + table.getProjName());
    sb.append("\n");
    sb.append("table" + KEY_VALUE_SEPARATOR + table.getTableName());
    sb.append("\n");

    StringBuffer sb1 = new StringBuffer();
    Column[] columns = table.getCols();
    int length = indexes == null ? table.getCols().length : indexes.size();
    for (int i = 0; i < length; i++) {
      int index = indexes == null ? i : indexes.get(i);
      Column col = columns[index];
      if (sb1.length() > 0) {
        sb1.append(COLUMN_SEPARATOR);
      }
      sb1.append(col.getName() + NAME_TYPE_DELIMITER + col.getTypeInfo().toString());
    }

    sb.append("columns" + KEY_VALUE_SEPARATOR + sb1.toString());
    sb.append("\n");

    Column[] partitions = table.getPartitions();
    if (partitions != null && partitions.length > 0) {
      sb.append("partitions" + KEY_VALUE_SEPARATOR + toString(partitions));
      sb.append("\n");
    }

    dir.mkdirs();
    File schemaFile = new File(dir, Constants.SCHEMA_FILE);
    LOG.info("generate schema file: " + schemaFile.getAbsolutePath());
    OutputStream out = null;
    try {
      out = new FileOutputStream(schemaFile);
      String result = sb.toString();
      // 去掉最后一个换行符\n
      result = result.substring(0, result.length() - 1);
      out.write(result.getBytes("utf-8"));
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    } finally {
      try {
        out.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    LOG.info("Finished to write table scheme : " + tableInfo + "-->" + dir.getAbsolutePath());
  }

  public static TableMeta readSchema(File dir) {
    if (dir == null || !dir.exists()) {
      return null;
    }

    File schemaFile = new File(dir, Constants.SCHEMA_FILE);
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(schemaFile)));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
          "__schema__ file not exists in direcotry " + dir.getAbsolutePath());
    }
    String line = null;
    try {
      line = br.readLine();
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    String project = null;
    String table = null;
    Column[] cols = null;
    Column[] partitionCols = null;
    while (line != null) {
      line = line.trim();
      if (line.equals("") || line.startsWith("#")) {
        try {
          line = br.readLine();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        continue;
      }

      String[] kv = line.split(KEY_VALUE_SEPARATOR);
      if (kv == null || kv.length != 2 || kv[0] == null || kv[0].trim().isEmpty() || kv[1] == null
          || kv[1].trim().isEmpty()) {
        try {
          line = br.readLine();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        continue;
      }

      kv[0] = kv[0].trim();
      kv[1] = kv[1].trim();

      if (kv[0].equals("project")) {
        project = kv[1];
      } else if (kv[0].equals("table")) {
        table = kv[1];
        if (table == null || table.trim().isEmpty()) {
          throw new RuntimeException("Table schema file '_schema_' must include 'table'");
        }
      } else if (kv[0].equals("columns")) {
        String columns = kv[1];
        if (columns == null || columns.trim().isEmpty()) {
          throw new RuntimeException("Table schema file '_schema_' must include 'columns'");
        }

        cols = fromString(columns);
        if (cols.length == 0) {
          throw new RuntimeException("'columns' in table schema file '_schema_' has invalid value");
        }
      } else if (kv[0].equals("partitions")) {
        String partitions = kv[1];
        if (partitions != null && !partitions.trim().isEmpty()) {
          partitionCols = fromString(partitions);
        }
      }

      try {
        line = br.readLine();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      br.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new TableMeta(project, table, cols, partitionCols);

  }

  /**
   * 获取行属性的名称数组
   *
   * @param cols
   *     行属性
   * @return 名称数组
   */
  public static String[] getColumnNames(Column[] cols) {
    String[] names = new String[cols.length];
    for (int i = 0; i < cols.length; i++) {
      names[i] = cols[i].getName();
    }
    return names;
  }

  /**
   * 行属性序列化为描述字符串
   * @param cols 行属性
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
        sb.append(COLUMN_SEPARATOR);
      }
      sb.append(c.getName()).append(NAME_TYPE_DELIMITER).append(c.getTypeInfo().toString());
    }
    return sb.toString();
  }

  /**
   * 描述字符串解析为列
   * @param str 描述字符串 like c1:MAP<STRING,STRING>,c2:ARRAY<INT>
   * @return column数组
   */
  public static Column[] fromString(String str) {
    List<Column> colList = new ArrayList<Column>();
    String remain = str;
    int pos = 0; //indicate the position we find
    IllegalArgumentException exception = null;
    while (remain.length() > 0) {
      pos = remain.indexOf(COLUMN_SEPARATOR, pos);
      if (pos < 0) { //last one
        pos = remain.length();
      }
      String candidate = remain.substring(0, pos);
      String[] nameType = candidate.split(NAME_TYPE_DELIMITER, 2);
      if (nameType.length != 2) {
        throw new IllegalArgumentException(
          "Malformed schema definition, expecting \"name:type\" but was \"" + nameType + "\"");
      }
      try {
        TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(nameType[1]);
        if (typeInfo != null) { //find a valid type
          exception = null;
          colList.add(new Column(nameType[0], typeInfo));
          if (pos == remain.length()) {
            remain = "";
          } else {
            remain = remain.substring(pos + 1);
            pos = 0;
          }
          continue;
        }
      } catch (IllegalArgumentException e) {
        exception = e;
      }
      if (pos == remain.length()) {
        remain = "";
      } else {
        pos = pos + 1; //try to parse at next position
      }
    }
    if (exception != null) {
      throw exception;
    } else {
      return colList.toArray(new Column[]{});
    }
  }

  /**
   * 解析resolve annotation中的类型信息
   * @param str : like string,array<string>
   */
  public static List<TypeInfo> parseResolveTypeInfo(String str) throws IllegalArgumentException {
    List<TypeInfo> types = new ArrayList<TypeInfo>();
    String remain = str;
    int pos = 0; //indicate the position we find
    IllegalArgumentException exception = null;
    while (remain.length() > 0) {
      pos = remain.indexOf(RESOLVE_SEPARATOR, pos);
      if (pos < 0) { //last one
        pos = remain.length();
      }
      String candidate = remain.substring(0, pos).trim();
      try {
        TypeInfo typeInfo;
        if ("*".equals(candidate) || "ANY".equalsIgnoreCase(candidate)) {
          typeInfo = new AnyTypeInfo();
        } else {
          typeInfo = TypeInfoParser.getTypeInfoFromTypeString(candidate);
        }
        if (typeInfo != null) { //find a valid type
          exception = null;
          types.add(typeInfo);
          if (pos == remain.length()) {
            remain = "";
          } else {
            remain = remain.substring(pos + 1);
            pos = 0;
          }
          continue;
        }
      } catch (IllegalArgumentException e) {
        exception = e;
      }
      if (pos == remain.length()) {
        remain = "";
      } else {
        pos = pos + 1; //try to parse at next position
      }
    }
    if (exception != null) {
      throw exception;
    } else {
      return types;
    }
  }

  /**
   * UDF local run column list split
   * @param str 描述字符串 like c1, ",", ":", 1|bigint
   * @return column数组
   */
  public static String[] splitColumn(String str) {
    List<String> colList = new ArrayList<String>();
    String remain = str;
    int pos = 0; //indicate the position we find
    while (remain.length() > 0) {
      pos = remain.indexOf(COLUMN_SEPARATOR, pos);
      if (pos < 0) { //last one
        pos = remain.length();
      }
      String candidate = remain.substring(0, pos).trim();
      boolean found = true;
      if (candidate.startsWith("\"")) { //handle the case that string constant contains comma
        found = isStringConstant(candidate);
      }
      if (found) {
        colList.add(candidate);
        if (pos == remain.length()) {
          remain = "";
        } else {
          remain = remain.substring(pos + 1);
          pos = 0;
        }
        continue;
      }
      if (pos == remain.length()) {
        remain = "";
      } else {
        pos = pos + 1; //try to parse at next position
      }
    }
    return colList.toArray(new String[]{});
  }

  public static ColumnOrConstant parseColumn(String colStr, TableMeta tableMeta) throws IllegalArgumentException {
    if (StringUtils.isBlank(colStr)) {
      throw new IllegalArgumentException("column or constant is blank");
    }
    if (isStringConstant(colStr)) { //string constant
      String value = colStr.substring(1, colStr.length() - 1);
      TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString("string");
      return new ColumnOrConstant(value, typeInfo);
    }
    String[] constantAndType = colStr.split(CONSTANT_TYPE_DELIMITER);
    if (constantAndType.length == 2) { //other type constant
      String type = constantAndType[1].trim();
      TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(type);
      if (typeInfo == null) {
        throw new IllegalArgumentException("type " + type + " is invalid");
      }
      String constant = constantAndType[0].trim();
      Object value = TypeConvertUtils.fromString(typeInfo, constant, false);
      if (value == null) {
        throw new IllegalArgumentException("convert to constant value failed:" + colStr);
      }
      return new ColumnOrConstant(value, typeInfo);
    } else { //column name
      Integer colIndex = null;
      if (tableMeta != null) {
        for (int j = 0; j < tableMeta.getCols().length; ++j) {
          if (tableMeta.getCols()[j].getName().equals(colStr)) {
            colIndex = j;
            break;
          }
        }
        if (colIndex == null) {
          throw new IllegalArgumentException(ExceptionCode.ODPS_0720021 + " - " + colStr + " in table "
            + tableMeta.getProjName() + "." + tableMeta.getTableName());
        }
      }
      return new ColumnOrConstant(colStr, colIndex);
    }
  }

  public static List<ColumnOrConstant> parseColumnConstant(String[] columns, TableMeta tableMeta) throws IllegalArgumentException {
    if (columns == null || columns.length == 0) {
      return null;
    }
    List<ColumnOrConstant> columnOrConstants = new ArrayList<ColumnOrConstant>(columns.length);
    for (String column : columns) {
      columnOrConstants.add(parseColumn(column, tableMeta));
    }
    return columnOrConstants;
  }

  private static boolean isStringConstant(String str) {
    return str.length() > 2 && str.startsWith("\"") && str.endsWith("\"");
  }

}
