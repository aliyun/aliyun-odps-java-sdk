package com.aliyun.odps.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.expression.GenerationExpressionParser;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ColumnUtils {

  public static Column fromJson(String json) {
    JsonParser parser = new JsonParser();
    JsonObject node = parser.parse(json).getAsJsonObject();

    String name = node.has("name") ? node.get("name").getAsString() : null;
    String typeString = node.has("type") ? node.get("type").getAsString().toUpperCase() : null;
    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeString);

    String comment = node.has("comment") ? node.get("comment").getAsString() : null;
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

    if (node.has("hasGenerateExpression") && node.get("hasGenerateExpression").getAsBoolean()) {
      if (node.has("generateExpression")) {
        String generateExpressionStr = node.get("generateExpression").getAsString();
        column.setGenerateExpression(GenerationExpressionParser.parse(generateExpressionStr));
      }
    }
    return column;
  }

  public static List<String> orderColumns(TableSchema schema, List<String> columns) {
    Set<String> columnSet = new HashSet<>(columns);
    List<Column> dataColumns = schema.getColumns();
    dataColumns.addAll(schema.getPartitionColumns());
    return dataColumns.stream().map(Column::getName).filter(columnSet::contains)
        .collect(Collectors.toList());
  }
}
