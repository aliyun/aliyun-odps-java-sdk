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

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class TunnelTableSchema extends TableSchema {

  public TunnelTableSchema(JsonObject node) {
    JsonArray columns = node.has("columns") ? node.get("columns").getAsJsonArray() : new JsonArray();
    for (int i = 0; i < columns.size(); ++i) {
      JsonObject column = columns.get(i).getAsJsonObject();
      Column col = parseColumn(column);
      addColumn(col);
    }

    columns = node.has("partitionKeys") ? node.get("partitionKeys").getAsJsonArray() : new JsonArray();
    for (int i = 0; i < columns.size(); ++i) {
      JsonObject column = columns.get(i).getAsJsonObject();
      Column col = parseColumn(column);
      addPartitionColumn(col);
    }
  }

  private Column parseColumn(JsonObject column) {
    String name = column.has("name") ? column.get("name").getAsString() : null;
    String type = column.has("type") ? column.get("type").getAsString() : null;
    String comment = column.has("comment") ? column.get("comment").getAsString() : null;
    Column col = null;
    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(type);
    col = new Column(name, typeInfo, comment);

    return col;
  }
}
