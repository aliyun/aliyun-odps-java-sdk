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

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class TunnelTableSchema extends TableSchema {

  private Map<String, Long> columnIdMap = new HashMap<>();

  public TunnelTableSchema(JsonObject node) {
    this(node, false);
  }

  public TunnelTableSchema(JsonObject node, boolean maxStorage) {
    if (maxStorage) {
      if (node.has("DataColumns")) {
        JsonArray dataColumns = node.get("DataColumns").getAsJsonArray();
        for (int i = 0; i < dataColumns.size(); ++i) {
          JsonObject column = dataColumns.get(i).getAsJsonObject();
          addColumn(parseMaxStorageColumn(column));
        }
      }

      if (node.has("PartitionColumns")) {
        JsonArray partitionColumns = node.get("PartitionColumns").getAsJsonArray();
        for (int i = 0; i < partitionColumns.size(); ++i) {
          JsonObject column = partitionColumns.get(i).getAsJsonObject();
          addPartitionColumn(parseMaxStorageColumn(column));
        }
      }
    } else {
      JsonArray
          columns =
          node.has("columns") ? node.get("columns").getAsJsonArray() : new JsonArray();
      for (int i = 0; i < columns.size(); ++i) {
        JsonObject column = columns.get(i).getAsJsonObject();
        Column col = parseColumn(column);
        addColumn(col);
      }

      columns =
          node.has("partitionKeys") ? node.get("partitionKeys").getAsJsonArray() : new JsonArray();
      for (int i = 0; i < columns.size(); ++i) {
        JsonObject column = columns.get(i).getAsJsonObject();
        Column col = parseColumn(column);
        addPartitionColumn(col);
      }
    }
  }



  public long getColumnId(String name) throws TunnelException {
    if (!columnIdMap.containsKey(name)) {
      throw new TunnelException("Cannot get column '" + name + "' id from schema");
    }
    return columnIdMap.get(name);
  }

  private Column parseColumn(JsonObject column) {
    String name = column.has("name") ? column.get("name").getAsString() : null;
    String type = column.has("type") ? column.get("type").getAsString() : null;
    String comment = column.has("comment") ? column.get("comment").getAsString() : null;
    String nullable = column.has("nullable") ? column.get("nullable").getAsString() : null;
    String columnId = column.has("column_id") ? column.get("column_id").getAsString() : null;
    if (!StringUtils.isNullOrEmpty(columnId) && !StringUtils.isNullOrEmpty(name)) {
      columnIdMap.put(name, Long.valueOf(columnId));
    }
    Column col;
    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(type);
    col = new Column(name, typeInfo, comment);
    if (nullable != null) {
      col.setNullable(nullable.equalsIgnoreCase("true"));
    }
    return col;
  }

  private Column parseMaxStorageColumn(JsonObject column) {
    String name = column.has("Name") ? column.get("Name").getAsString() : null;
    String type = column.has("Type") ? column.get("Type").getAsString() : null;
    String comment = column.has("Comment") ? column.get("Comment").getAsString() : null;
    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(type);
    Column col = new Column(name, typeInfo, comment);
    if (column.has("Nullable")) {
      col.setNullable(column.get("Nullable").getAsBoolean());
    }
    return col;
  }
}
