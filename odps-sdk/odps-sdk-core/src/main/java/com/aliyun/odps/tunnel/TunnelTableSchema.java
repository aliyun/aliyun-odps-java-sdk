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
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;

class TunnelTableSchema extends TableSchema {

  public TunnelTableSchema(JsonNode node) {
    JsonNode columns = node.get("columns");
    if (columns.isArray()) {
      Iterator<JsonNode> it = columns.getElements();
      while (it.hasNext()) {

        JsonNode column = it.next();
        JsonNode name = column.get("name");
        JsonNode type = column.get("type");
        Column col = null;

        if (type.asText().toUpperCase().startsWith("MAP")) {
          col = new Column(name.asText(), OdpsType.MAP);
          col.setGenericTypeList(Arrays.asList(parseMapType(type.asText().toUpperCase())));
        } else if (type.asText().toUpperCase().startsWith("ARRAY")) {
          col = new Column(name.asText(), OdpsType.ARRAY);
          col.setGenericTypeList(Arrays.asList(parseArrayType(type.asText().toUpperCase())));
        } else {
          col = new Column(name.asText(), OdpsType.valueOf(type.asText().toUpperCase()));
        }

        addColumn(col);
      }
    }
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
