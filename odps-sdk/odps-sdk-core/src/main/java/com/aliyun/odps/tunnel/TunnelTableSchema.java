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

package com.aliyun.odps.tunnel;

import java.util.Arrays;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;

class TunnelTableSchema extends TableSchema {

  public TunnelTableSchema(JSONObject node) {
    JSONArray columns = node.getJSONArray("columns");
    for (int i = 0; i < columns.size(); ++i) {
      JSONObject column = columns.getJSONObject(i);
      Column col = parseColumn(column);
      addColumn(col);
    }

    columns = node.getJSONArray("partitionKeys");
    for (int i = 0; i < columns.size(); ++i) {
      JSONObject column = columns.getJSONObject(i);
      Column col = parseColumn(column);
      addPartitionColumn(col);
    }
  }

  private Column parseColumn(JSONObject column) {
    String name = column.getString("name");
    String type = column.getString("type");
    Column col = null;

    if (type.toUpperCase().startsWith("MAP")) {
      col = new Column(name, OdpsType.MAP);
      col.setGenericTypeList(Arrays.asList(parseMapType(type.toUpperCase())));
    } else if (type.toUpperCase().startsWith("ARRAY")) {
      col = new Column(name, OdpsType.ARRAY);
      col.setGenericTypeList(Arrays.asList(parseArrayType(type.toUpperCase())));
    } else {
      col = new Column(name, OdpsType.valueOf(type.toUpperCase()));
    }
    return col;
  }

  private OdpsType parseArrayType(String typeLiteral) throws IllegalArgumentException {
    if (!typeLiteral.startsWith("ARRAY<")) {
      throw new IllegalArgumentException("Array type should start with ARRAY<, now is " + typeLiteral);
    }

    String subTypeLiteral = typeLiteral.substring("ARRAY<".length(), typeLiteral.length() - 1);
    OdpsType type = OdpsType.valueOf(subTypeLiteral);
    return type;
  }

  private OdpsType[] parseMapType(String typeLiteral) throws IllegalArgumentException {
    if (!typeLiteral.startsWith("MAP<")) {
      throw new IllegalArgumentException("Map type should start with Map<, now is " + typeLiteral);
    }

    String subTypeLiteral = typeLiteral.substring("Map<".length(), typeLiteral.length() - 1);

    String[] subTypes = subTypeLiteral.split(",");
    if (subTypes.length != 2) {
      throw new IllegalArgumentException("Map type's format is Map<keyType,valueType> , now is " + typeLiteral);
    }

    OdpsType[] types = new OdpsType[2];
    types[0] = OdpsType.valueOf(subTypes[0]);
    types[1] = OdpsType.valueOf(subTypes[1]);
    return types;
  }

}
