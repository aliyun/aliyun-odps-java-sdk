package com.aliyun.odps.utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ColumnUtils {
  public static Column fromJson(String json) {
    JsonParser parser = new JsonParser();
    JsonObject node = parser.parse(json).getAsJsonObject();

    String name =
        node.has("name") ? node.get("name").getAsString() : null;
    String typeString =
        node.has("type") ? node.get("type").getAsString().toUpperCase() : null;
    TypeInfo typeInfo =
        TypeInfoParser.getTypeInfoFromTypeString(typeString);

    String comment =
        node.has("comment") ? node.get("comment").getAsString() : null;
    String label = null;
    if (node.has("label") && (!node.get("label").getAsString().isEmpty())) {
      label = node.get("label").getAsString();
    }

    List<String> extendedLabels = null;
    if (node.has("extendedLabels") && (node.get("extendedLabels").getAsJsonArray().size() != 0)) {
      Iterator<JsonElement> it = node.get("extendedLabels").getAsJsonArray().iterator();
      extendedLabels = new LinkedList<String>();
      while (it.hasNext()) {
        extendedLabels.add(it.next().getAsString());
      }
    }

    Column column = new Column(name, typeInfo, comment, label, extendedLabels);

    if (node.has("isNullable")) {
      column.setNullable(node.get("isNullable").getAsBoolean());
    }

    if (node.has("defaultValue")) {
      column.setDefaultValue(node.get("defaultValue").getAsString());
    }

    return column;
  }

}
