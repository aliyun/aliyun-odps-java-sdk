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

package com.aliyun.odps.storage.internal.serializer;

import java.lang.reflect.Type;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Serializes {@link TableSchema} (and subclasses such as ReadSchema) into the server's
 * DataSchema JSON shape: {"DataColumns": [...], "PartitionColumns": [...],
 * "SystemColumns": [...]}. Column shapes follow what SchemaUtils.parseColumn accepts.
 */
public class TableSchemaSerializer implements JsonSerializer<TableSchema> {

  @Override
  public JsonElement serialize(TableSchema src, Type typeOfSrc,
                               JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.add("DataColumns", columnArray(src.getColumns()));
    json.add("PartitionColumns", columnArray(src.getPartitionColumns()));
    if (src instanceof com.aliyun.odps.storage.internal.models.ReadSchema) {
      json.add("BlobColumns", columnArray(
          ((com.aliyun.odps.storage.internal.models.ReadSchema) src).getBlobColumns()));
      json.add("SystemColumns", columnArray(
          ((com.aliyun.odps.storage.internal.models.ReadSchema) src).getSystemColumns()));
    }
    return json;
  }

  private static JsonArray columnArray(List<Column> columns) {
    JsonArray array = new JsonArray();
    if (columns == null) {
      return array;
    }
    for (Column column : columns) {
      JsonObject json = new JsonObject();
      json.addProperty("Name", column.getName());
      json.addProperty("Type", column.getTypeInfo().getTypeName());
      json.addProperty("Comment", column.getComment() == null ? "" : column.getComment());
      json.addProperty("Nullable", column.isNullable());
      if (column.getColumnId() != null) {
        json.addProperty("ColumnId", column.getColumnId());
      }
      array.add(json);
    }
    return array;
  }
}
