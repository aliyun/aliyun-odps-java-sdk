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
import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.storage.internal.models.ReadSchema;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class ReadSchemaDeserializer implements JsonDeserializer<ReadSchema> {

  @Override
  public ReadSchema deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject schemaObject = json.getAsJsonObject();
    ReadSchema dataSchema = new ReadSchema();

    dataSchema.setColumns(deserializeColumnList(schemaObject, "DataColumns"));
    dataSchema.setPartitionColumns(deserializeColumnList(schemaObject, "PartitionColumns"));
    dataSchema.setSystemColumns(deserializeColumnList(schemaObject, "SystemColumns"));
    dataSchema.setBlobColumns(deserializeColumnList(schemaObject, "BlobColumns"));
    return dataSchema;
  }

  private List<Column> deserializeColumnList(JsonObject schemaObject, String key) {
    List<Column> columnList = new ArrayList<>();
    JsonElement columnsElement = schemaObject.get(key);

    if (columnsElement == null || columnsElement.isJsonNull()) {
      return columnList;
    }

    if (columnsElement.isJsonObject() && columnsElement.getAsJsonObject().size() == 0) {
      return columnList;
    }

    if (columnsElement.isJsonArray()) {
      for (JsonElement columnElement : columnsElement.getAsJsonArray()) {
        JsonObject columnJson = columnElement.getAsJsonObject();

        // 从 JSON 中提取字段
        String name = columnJson.get("Name").getAsString();
        String typeString = columnJson.get("Type").getAsString();
        String comment = columnJson.get("Comment").getAsString();
        boolean isNullable = columnJson.get("Nullable").getAsBoolean();
        long columnId = columnJson.get("ColumnId").getAsLong();

        TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeString);


        Column.ColumnBuilder builder = Column.newBuilder(name, typeInfo)
          .withComment(comment)
          .withColumnId(columnId);

        if (!isNullable) {
          builder.notNull();
        }

        columnList.add(builder.build());
      }
    }

    return columnList;
  }
}

