package com.aliyun.odps.data;

public interface JsonValue {
  int size();
  boolean isJsonPrimitive();
  boolean isJsonArray();
  boolean isJsonObject();
  boolean isJsonNull();
  JsonValue get(int index);
  JsonValue get(String filedName);
  Boolean getAsBoolean();
  Number getAsNumber();
  String getAsString();
  String toString();
}
