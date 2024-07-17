package com.aliyun.odps.data;

import java.io.Serializable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SimpleJsonValue implements JsonValue, Serializable {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String stringValue;
  private transient JsonNode value;

  public SimpleJsonValue(String value) {
    this(value, false);
  }

  public SimpleJsonValue(String value, boolean validate) {
    this.stringValue = value;
    if (validate) {
      lazyLoad();
    }
  }

  public SimpleJsonValue(JsonNode jsonNode) {
    this.stringValue = jsonNode.toString();
    this.value = jsonNode;
  }

  @Override
  public int size() {
    lazyLoad();
    if (!value.isArray()) {
      throw new UnsupportedOperationException();
    }
    ArrayNode arrayNode = (ArrayNode) value;
    return arrayNode.size();
  }

  private void lazyLoad() {
    if (value == null) {
      try {
        this.value = OBJECT_MAPPER.readTree(stringValue);
      } catch (Exception e) {
        throw new IllegalArgumentException("Illegal argument for JsonValue value.");
      }
    }
  }

  @Override
  public boolean isJsonPrimitive() {
    lazyLoad();
    return !value.isArray() && !value.isObject();
  }

  @Override
  public boolean isJsonArray() {
    lazyLoad();
    return value.isArray();
  }

  @Override
  public boolean isJsonObject() {
    lazyLoad();
    return this.value.isObject();
  }

  @Override
  public boolean isJsonNull() {
    lazyLoad();
    return this.value.isNull();
  }

  @Override
  public JsonValue get(int index) {
    lazyLoad();
    if (!isJsonArray()) {
      throw new UnsupportedOperationException();
    }
    ArrayNode arrayNode = (ArrayNode) value;
    return new SimpleJsonValue(arrayNode.get(index));
  }

  @Override
  public JsonValue get(String filedName) {
    lazyLoad();
    if (!isJsonObject()) {
      throw new UnsupportedOperationException();
    }
    ObjectNode objectNode = (ObjectNode) value;
    return new SimpleJsonValue(objectNode.get(filedName));
  }

  @Override
  public Boolean getAsBoolean() {
    lazyLoad();
    if (!value.isBoolean()) {
      throw new UnsupportedOperationException();
    }
    return value.asBoolean();
  }

  @Override
  public Number getAsNumber() {
    lazyLoad();
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
        return (float) value.asDouble();
      }

      @Override
      public double doubleValue() {
        return value.asDouble();
      }
    };
  }

  @Override
  public String getAsString() {
    lazyLoad();
    if (!value.isTextual()) {
      throw new UnsupportedOperationException();
    }
    return this.value.asText();
  }

  @Override
  public String toString() {
    return stringValue;
  }
}
