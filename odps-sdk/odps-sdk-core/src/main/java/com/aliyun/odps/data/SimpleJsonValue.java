package com.aliyun.odps.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SimpleJsonValue implements JsonValue {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private JsonNode value;

  public SimpleJsonValue(String value) {
    try {
      this.value = OBJECT_MAPPER.readTree(value);
    } catch (Exception e) {
      throw new IllegalArgumentException("Illegal argument for JsonValue value.");
    }
  }

  public SimpleJsonValue(JsonNode jsonNode) {
    this.value = jsonNode;
  }

  @Override
  public int size() {
    if (!value.isArray()) {
      throw new UnsupportedOperationException();
    }
    ArrayNode arrayNode = (ArrayNode) value;
    return arrayNode.size();
  }

  @Override
  public boolean isJsonPrimitive() {
    return !value.isArray() && !value.isObject();
  }

  @Override
  public boolean isJsonArray() {
    return value.isArray();
  }

  @Override
  public boolean isJsonObject() {
    return this.value.isObject();
  }

  @Override
  public boolean isJsonNull() {
    return this.value.isNull();
  }

  @Override
  public JsonValue get(int index) {
    if (!isJsonArray()) {
      throw new UnsupportedOperationException();
    }
    ArrayNode arrayNode = (ArrayNode) value;
    return new SimpleJsonValue(arrayNode.get(index));
  }

  @Override
  public JsonValue get(String filedName) {
    if (!isJsonObject()) {
      throw new UnsupportedOperationException();
    }
    ObjectNode objectNode = (ObjectNode) value;
    return new SimpleJsonValue(objectNode.get(filedName));
  }

  @Override
  public Boolean getAsBoolean() {
    if (!value.isBoolean()) {
      throw new UnsupportedOperationException();
    }
    return value.asBoolean();
  }

  @Override
  public Number getAsNumber() {
    if (!value.isNumber()) {
      throw new UnsupportedOperationException();
    }
    return new Number() {
      private static final long serialVersionUID = -8816955119176578371L;

      @Override
      public int intValue() {
        return value.asInt();
      }

      @Override
      public long longValue() {
        return value.asLong();
      }

      @Override
      public float floatValue() {
        return (float)value.asDouble();
      }

      @Override
      public double doubleValue() {
        return value.asDouble();
      }
    };
  }

  @Override
  public String getAsString() {
    if (!value.isTextual()) {
      throw new UnsupportedOperationException();
    }
    return this.value.asText();
  }

  @Override
  public String toString() {
    return this.value.toString();
  }
}
