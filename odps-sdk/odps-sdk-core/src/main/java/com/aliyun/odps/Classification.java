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

package com.aliyun.odps;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class Classification extends LazyLoad {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeHierarchyAdapter(AttributeDefinition.class, new AttributeDeserializer())
      .registerTypeHierarchyAdapter(AttributeDefinition.class, new AttributeSerializer())
      .registerTypeAdapter(ClassificationModel.class, new ClassificationModelDeserializer())
      .registerTypeAdapter(ClassificationModel.class, new ClassificationModelSerializer())
      .create();

  public static abstract class AttributeDefinition {
    public enum AttributeType {
      STRING,
      INTEGER,
      BOOLEAN,
      ENUM
    }

    AttributeType type;

    boolean isRequired;
    boolean isUnique;
    String pattern;
    Integer maxLength;
    Integer minLength;
    Integer maximum;
    Integer minimum;
    Set<String> elements;

    abstract public static class Builder<T extends Builder<T>> {

      boolean isRequired = false;
      boolean isUnique = true;

      Builder required() {
        this.isRequired = true;
        return self();
      }

      public abstract AttributeDefinition build();

      protected abstract T self();
    }

    public AttributeDefinition(Builder<?> builder) {
      this.isRequired = builder.isRequired;
      this.isUnique = builder.isUnique;
    }

    public boolean isRequired() {
      return this.isRequired;
    }
  }

  public static class StringAttributeDefinition extends AttributeDefinition {

    public static class Builder extends AttributeDefinition.Builder<Builder> {

      String pattern;
      Integer maxLength;
      Integer minLength;

      // TODO: server side unsupported yet
//      Builder pattern(String pattern) {
//        this.pattern = Objects.requireNonNull(pattern);
//        return self();
//      }

      public Builder maxLength(int maxLength) {
        this.maxLength = maxLength;
        return self();
      }

      public Builder minLength(int minLength) {
        this.minLength = minLength;
        return self();
      }

      @Override
      public AttributeDefinition build() {
        return new StringAttributeDefinition(this);
      }

      @Override
      protected Builder self() {
        return this;
      }
    }

    public StringAttributeDefinition(Builder builder) {
      super(builder);
      this.type = AttributeType.STRING;
      if (builder.pattern != null) {
        this.pattern = builder.pattern;
      }
      if (builder.maxLength != null) {
        this.maxLength = builder.maxLength;
      }
      if (builder.minLength != null) {
        this.minLength = builder.minLength;
      }
    }

    // TODO: server side unsupported
//    public String getPattern() {
//      return this.pattern;
//    }

    public Integer getMaxLength() {
      return this.maxLength;
    }

    public Integer getMinLength() {
      return this.minLength;
    }
  }

  public static class IntegerAttributeDefinition extends AttributeDefinition {

    public static class Builder extends AttributeDefinition.Builder<Builder> {

      Integer maximum;
      Integer minimum;

      @Override
      public AttributeDefinition build() {
        return new IntegerAttributeDefinition(this);
      }

      public Builder maximum(int maximum) {
        this.maximum = maximum;
        return self();
      }

      public Builder minimum(int minimum) {
        this.minimum = minimum;
        return self();
      }

      @Override
      protected Builder self() {
        return this;
      }
    }

    public IntegerAttributeDefinition(Builder builder) {
      super(builder);
      this.type = AttributeType.INTEGER;
      if (builder.maximum != null) {
        this.maximum = builder.maximum;
      }
      if (builder.minimum != null) {
        this.minimum = builder.minimum;
      }
    }

    public Integer getMaximum() {
      return this.maximum;
    }

    public Integer getMinimum() {
      return this.minimum;
    }
  }

  public static class EnumAttributeDefinition extends AttributeDefinition {

    public static class Builder extends AttributeDefinition.Builder<Builder> {

      Set<String> elements;

      public Builder element(String element) {
        if (this.elements == null) {
          this.elements = new HashSet<>();
        }
        this.elements.add(Objects.requireNonNull(element));
        return self();
      }

      @Override
      public AttributeDefinition build() {
        return new EnumAttributeDefinition(this);
      }

      @Override
      protected Builder self() {
        return this;
      }
    }

    public EnumAttributeDefinition(Builder builder) {
      super(builder);
      this.type = AttributeType.ENUM;
      if (builder.elements != null) {
        this.elements = new HashSet<>(builder.elements);
      }
    }

    public Set<String> getElements() {
      return this.elements;
    }
  }

  public static class BooleanAttributeDefinition extends AttributeDefinition {

    public static class Builder extends AttributeDefinition.Builder<Builder> {

      @Override
      public AttributeDefinition build() {
        return new BooleanAttributeDefinition(this);
      }

      @Override
      protected Builder self() {
        return this;
      }
    }

    public BooleanAttributeDefinition(Builder builder) {
      super(builder);
      this.type = AttributeType.BOOLEAN;
    }
  }

  static class AttributeSerializer implements JsonSerializer<AttributeDefinition> {
    @Override
    public JsonElement serialize(AttributeDefinition src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject attribute = new JsonObject();
      attribute.addProperty("Type", src.type.name().toLowerCase());
      JsonObject constraints = new JsonObject();
      constraints.addProperty("IsRequired", src.isRequired);
      constraints.addProperty("Unique", true);

      if (src.pattern != null) {
        constraints.addProperty("Pattern", src.pattern);
      }
      if (src.maxLength != null) {
        constraints.addProperty("MaxLength", src.maxLength);
      }
      if (src.minLength != null) {
        constraints.addProperty("MinLength", src.minLength);
      }
      if (src.maximum != null) {
        constraints.addProperty("Maximum", src.maximum);
      }
      if (src.minimum != null) {
        constraints.addProperty("Minimum", src.minimum);
      }
      if (src.elements != null) {
        JsonArray elements = new JsonArray();
        for (String element : src.elements) {
          elements.add(new JsonPrimitive(element));
        }
        constraints.add("Elements", elements);
      }

      attribute.add("Constraints", constraints);

      return attribute;
    }
  }

  static class AttributeDeserializer implements JsonDeserializer {
    @Override
    public Object deserialize(
        JsonElement json,
        Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      AttributeDefinition attributeDefinition = null;
      JsonObject src = json.getAsJsonObject();

      AttributeDefinition.AttributeType type =
          AttributeDefinition.AttributeType.valueOf(src.get("Type").getAsString().toUpperCase());
      switch (type) {
        case STRING: {
          StringAttributeDefinition.Builder builder = new StringAttributeDefinition.Builder();
          if (src.has("Constraints")) {
            JsonObject constraints = src.get("Constraints").getAsJsonObject();
            if (constraints.has("MaxLength")) {
              builder.maxLength(constraints.get("MaxLength").getAsInt());
            }
            if (constraints.has("MinLength")) {
              builder.minLength(constraints.get("MinLength").getAsInt());
            }
            // TODO: server side unsupported
//            if (constraints.has("Pattern")) {
//              builder.pattern(constraints.get("Pattern").getAsString());
//            }
          }
          attributeDefinition = builder.build();
          break;
        }
        case INTEGER: {
          IntegerAttributeDefinition.Builder builder = new IntegerAttributeDefinition.Builder();
          if (src.has("Constraints")) {
            JsonObject constraints = src.get("Constraints").getAsJsonObject();
            if (constraints.has("Maximum")) {
              builder.maximum(constraints.get("Maximum").getAsInt());
            }
            if (constraints.has("Minimum")) {
              builder.minimum(constraints.get("Minimum").getAsInt());
            }
          }
          attributeDefinition = builder.build();
          break;
        }
        case ENUM: {
          EnumAttributeDefinition.Builder builder = new EnumAttributeDefinition.Builder();
          if (src.has("Constraints")) {
            JsonObject constraints = src.get("Constraints").getAsJsonObject();
            if (constraints.has("Elements")) {
              JsonArray elements = constraints.get("Elements").getAsJsonArray();
              elements.forEach(e -> builder.element(e.getAsString()));
            }
            attributeDefinition = builder.build();
          }
          break;
        }
        case BOOLEAN: {
          BooleanAttributeDefinition.Builder builder = new BooleanAttributeDefinition.Builder();
          attributeDefinition = builder.build();
          break;
        }
        default:
      }
      return attributeDefinition;
    }
  }

  static class ClassificationModelSerializer implements JsonSerializer<ClassificationModel> {

    @Override
    public JsonElement serialize(
        ClassificationModel model,
        Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject classification = new JsonObject();

      if (model.attributes != null) {
        JsonObject attributes = new JsonObject();
        for (Entry<String, AttributeDefinition> entry : model.attributes.entrySet()) {
          attributes.add(entry.getKey(), context.serialize(entry.getValue()));
        }
        classification.add("Attributes", attributes);
      }

      classification.addProperty("DatabaseName", model.project);
      classification.addProperty("Name", model.name);
      if (model.owner != null) {
        classification.addProperty("Owner", model.owner);
      }

      return classification;
    }
  }

  static class ClassificationModelDeserializer implements JsonDeserializer<ClassificationModel> {

    @Override
    public ClassificationModel deserialize(
        JsonElement json,
        Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {

      ClassificationModel model = new ClassificationModel();
      JsonObject src = json.getAsJsonObject();
      if (src.has("Attributes")) {
        model.attributes.clear();
        JsonObject attributes = src.get("Attributes").getAsJsonObject();
        for (Entry<String, JsonElement> entry : attributes.entrySet()) {
          model.attributes.put(
              entry.getKey(), context.deserialize(entry.getValue(), AttributeDefinition.class));
        }
      }
      if (src.has("Owner")) {
        model.owner = src.get("Owner").getAsString();
      }
      if (src.has("CreateTime")) {
        model.createdTime = new Date(src.get("CreateTime").getAsLong());
      }
      if (src.has("UpdateTime")) {
        model.lastModifiedTime = new Date(src.get("UpdateTime").getAsLong());
      }
      if (src.has("DatabaseName")) {
        model.project = src.get("DatabaseName").getAsString();
      }
      if (src.has("Name")) {
        model.name = src.get("Name").getAsString();
      }

      return model;
    }
  }

  static class ClassificationModel {
    String project;
    String name;
    String owner;
    Date lastModifiedTime;
    Date createdTime;
    Map<String, AttributeDefinition> attributes = new HashMap<>();

    @Override
    public ClassificationModel clone() {
      ClassificationModel model = new ClassificationModel();
      model.project = project;
      model.name = name;
      model.owner = owner;
      model.lastModifiedTime = lastModifiedTime;
      model.createdTime = createdTime;
      model.attributes = new HashMap<>(attributes);

      return model;
    }
  }

  ClassificationModel model;
  RestClient client;
  Odps odps;
  Tags tags;

  Classification(ClassificationModel model, Odps odps) {
    this.model = Objects.requireNonNull(model);
    this.odps = Objects.requireNonNull(odps);
    this.client = odps.getRestClient();
    this.tags = new Tags(odps, this);
  }

  public String getProject() {
    lazyLoad();
    return model.project;
  }

  public String getName() {
    lazyLoad();
    return model.name;
  }

  public Date getCreatedTime() {
    lazyLoad();
    return model.createdTime;
  }

  public Date getLastModifiedTime() {
    lazyLoad();
    return model.lastModifiedTime;
  }

  public String getOwner() {
    lazyLoad();
    return model.owner;
  }

  public void addAttributeDefinition(
      String attribute,
      AttributeDefinition definition) throws OdpsException {

    lazyLoad();
    Objects.requireNonNull(attribute);
    Objects.requireNonNull(definition);
    if (model.attributes.containsKey(attribute)) {
      throw new IllegalArgumentException("Attribute '" + attribute + "' already exists");
    }
    if (definition.isRequired) {
      throw new IllegalArgumentException("Adding a required attribute is not allowed");
    }

    String resource = ResourceBuilder.buildClassificationResource(
        odps.getDefaultProject(),
        Objects.requireNonNull(getName()));
    ClassificationModel model = this.model.clone();

    // Update classification api only supports adding an attributes. And the input should only
    // contain new attributes.
    model.attributes.clear();
    model.attributes.put(attribute, definition);

    JsonObject root = new JsonObject();
    root.add("ClassificationInput", GSON.toJsonTree(model));

    String body = GSON.toJson(root);
    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/json");
    client.request(resource, "PUT", null, headers, body.getBytes());
    reload();
  }

  public void setOwner(String owner) {
    lazyLoad();
    model.owner = Objects.requireNonNull(owner);
  }

  public Map<String, AttributeDefinition> getAttributeDefinitions() {
    lazyLoad();
    return new HashMap<>(model.attributes);
  }

  public Tags tags() {
    lazyLoad();
    return new Tags(odps, this);
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildClassificationResource(model.project, model.name);
    Response resp =
        client.request(resource, "GET", null, null, null);

    JsonParser parser = new JsonParser();
    String json = new String(resp.getBody(), StandardCharsets.UTF_8);
    JsonElement jsonElement = parser.parse(json);
    if (!jsonElement.isJsonObject()) {
      throw new ReloadException("Expect a JsonObject, but got: " + json);
    }

    JsonObject jsonObject = jsonElement.getAsJsonObject();
    if (!jsonObject.has("Classification")) {
      throw new ReloadException("Expect member 'Classification', but got: " + json);
    }

    this.model = GSON.fromJson(jsonObject.get("Classification"), ClassificationModel.class);
    setLoaded(true);
  }
}
