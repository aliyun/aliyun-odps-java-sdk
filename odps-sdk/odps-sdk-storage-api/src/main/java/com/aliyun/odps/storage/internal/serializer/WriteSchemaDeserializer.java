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
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.storage.internal.models.WriteSchema;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class WriteSchemaDeserializer implements JsonDeserializer<WriteSchema> {

  @Override
  public WriteSchema deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject schemaObject = json.getAsJsonObject();
    WriteSchema dataSchema = new WriteSchema();

    dataSchema.setColumns(deserializeColumnList(schemaObject, "DataColumns"));
    // PartitionColumns 用户无需感知
    // dataSchema.setPartitionColumns(deserializeColumnList(schemaObject, "PartitionColumns"));
    dataSchema.setSystemColumns(deserializeColumnList(schemaObject, "SystemColumns"));
    return dataSchema;
  }

  /**
   * 辅助方法，用于将 JSON 中特定键的列数组反序列化为 List<Column>
   */
  private List<Column> deserializeColumnList(JsonObject schemaObject, String key) {
    List<Column> columnList = new ArrayList<>();
    JsonElement columnsElement = schemaObject.get(key);

    if (columnsElement == null || columnsElement.isJsonNull() || !columnsElement.isJsonArray()) {
      return columnList; // 如果键不存在、为 null 或不是数组，返回空列表
    }

    for (JsonElement columnElement : columnsElement.getAsJsonArray()) {
      JsonObject columnJson = columnElement.getAsJsonObject();

      // 1. 从 columnJson 的顶层获取基本信息
      String comment = getJsonString(columnJson, "comment", "");
      String label = getJsonString(columnJson, "label", "");

      // 2. 解析核心的 columnType 对象
      JsonObject typeInfoJson = columnJson.getAsJsonObject("columnType");
      if (typeInfoJson == null) {
        throw new JsonParseException("Missing 'columnType' object for column json: " + columnJson.getAsString());
      }
      String name = getJsonString(typeInfoJson, "MemberName", "");
      long columnId = getJsonLong(typeInfoJson, "ColumnId", -1);
      if (columnId == -1) {
        throw new JsonParseException("Missing 'ColumnId' object for column json: " + columnJson.getAsString());
      }
      boolean isNullable = getJsonBoolean(typeInfoJson, "Nullable", true);
      boolean hasDefaultValue = getJsonBoolean(typeInfoJson, "HasDefaultValue", false);
      String defaultValue = getJsonString(typeInfoJson, "DefaultValue", "");
      TypeInfo typeInfo = parseTypeInfo(typeInfoJson);


      // 3. 使用 ODPS Column 的 Builder 构建实例
      Column.ColumnBuilder builder = Column.newBuilder(name, typeInfo)
        .withComment(comment)
        .withLabel(label)
        .withColumnId(columnId);

      if (hasDefaultValue) {
        builder.withDefaultValue(defaultValue);
      }

      if (!isNullable) {
        builder.notNull();
      }

      // 处理 extendedLabels
      JsonArray extendedLabelsArray = columnJson.getAsJsonArray("extendedLabels");
      if (extendedLabelsArray != null) {
        List<String> extendedLabels = new ArrayList<>();
        for (JsonElement labelElement : extendedLabelsArray) {
          extendedLabels.add(labelElement.getAsString());
        }
        builder.withExtendedLabels(extendedLabels);
      }

      columnList.add(builder.build());
    }

    return columnList;
  }

  /**
   * 递归地将 columnType 的 JSON 对象解析为 ODPS 的 TypeInfo 对象
   */
  private TypeInfo parseTypeInfo(JsonObject typeInfoJson) throws JsonParseException {
    // C++ enum ColumnType (int)
    int typeCode = getJsonInt(typeInfoJson, "Type", -1);

    switch (typeCode) {
      // Parameterized Types
      case 5: // DECIMAL
        int precision = getJsonInt(typeInfoJson, "Precision", 0);
        int scale = getJsonInt(typeInfoJson, "Scale", 0);
        return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
      case 9: // CHAR
        int charLen = getJsonInt(typeInfoJson, "SpecifiedLength", 0);
        return TypeInfoFactory.getCharTypeInfo(charLen);
      case 10: // VARCHAR
        int varcharLen = getJsonInt(typeInfoJson, "SpecifiedLength", 0);
        return TypeInfoFactory.getVarcharTypeInfo(varcharLen);

      // Complex Types (Recursive)
      case 17: { // ARRAY
        JsonArray subTypesArray = typeInfoJson.getAsJsonArray("SubTypes");
        if (subTypesArray == null || subTypesArray.size() != 1) {
          throw new JsonParseException("ARRAY type must have exactly one sub-type.");
        }
        TypeInfo elementType = parseTypeInfo(subTypesArray.get(0).getAsJsonObject());
        return TypeInfoFactory.getArrayTypeInfo(elementType);
      }
      case 18: { // MAP
        JsonArray subTypesArray = typeInfoJson.getAsJsonArray("SubTypes");
        if (subTypesArray == null || subTypesArray.size() != 2) {
          throw new JsonParseException("MAP type must have exactly two sub-types (key, value).");
        }
        TypeInfo keyType = parseTypeInfo(subTypesArray.get(0).getAsJsonObject());
        TypeInfo valueType = parseTypeInfo(subTypesArray.get(1).getAsJsonObject());
        return TypeInfoFactory.getMapTypeInfo(keyType, valueType);
      }
      case 19: { // STRUCT
        JsonArray subTypesArray = typeInfoJson.getAsJsonArray("SubTypes");
        if (subTypesArray == null) {
          // Empty struct
          return TypeInfoFactory.getStructTypeInfo(new ArrayList<>(), new ArrayList<>());
        }
        List<String> fieldNames = new ArrayList<>();
        List<TypeInfo> fieldTypeInfos = new ArrayList<>();
        for (JsonElement subTypeElement : subTypesArray) {
          JsonObject subTypeJson = subTypeElement.getAsJsonObject();
          String memberName = getJsonString(subTypeJson, "MemberName", "");
          if (memberName.isEmpty()) {
            throw new JsonParseException("Struct member must have a 'MemberName'.");
          }
          fieldNames.add(memberName);
          fieldTypeInfos.add(parseTypeInfo(subTypeJson));
        }
        return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
      }
      default:
        return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.fromInt(typeCode));
    }
  }

  // --- JSON Helper methods to handle missing fields gracefully ---
  private String getJsonString(JsonObject obj, String key, String defaultValue) {
    JsonElement element = obj.get(key);
    return (element != null && !element.isJsonNull()) ? element.getAsString() : defaultValue;
  }

  private boolean getJsonBoolean(JsonObject obj, String key, boolean defaultValue) {
    JsonElement element = obj.get(key);
    return (element != null && !element.isJsonNull()) ? element.getAsBoolean() : defaultValue;
  }

  private int getJsonInt(JsonObject obj, String key, int defaultValue) {
    JsonElement element = obj.get(key);
    return (element != null && !element.isJsonNull()) ? element.getAsInt() : defaultValue;
  }

  private long getJsonLong(JsonObject obj, String key, long defaultValue) {
    JsonElement element = obj.get(key);
    return (element != null && !element.isJsonNull()) ? element.getAsLong() : defaultValue;
  }
}

