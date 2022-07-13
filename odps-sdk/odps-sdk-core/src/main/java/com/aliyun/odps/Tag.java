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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.aliyun.odps.Tags.TagBuilder;
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

public class Tag extends LazyLoad {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(TagModel.class, new TagModelDeserializer())
      .create();

  static class TagModelSerializer implements JsonSerializer<TagModel> {

    @Override
    public JsonElement serialize(
        TagModel src,
        Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject tag = new JsonObject();
      tag.addProperty("DatabaseName", src.project);
      tag.addProperty("Classification", src.classification);
      tag.addProperty("Name", src.name);

      if (src.policy != null) {
        JsonObject extendedInfo = new JsonObject();
        extendedInfo.addProperty("Policy", src.policy);
        tag.add("ExtendedInfo", extendedInfo);
      }

      JsonObject tagValues = new JsonObject();
      for (Entry<String, String> entry : src.attributes.entrySet()) {
        tagValues.addProperty(entry.getKey(), entry.getValue());
      }
      tag.add("TagValues", tagValues);

      if (!src.attributesToRemove.isEmpty()) {
        JsonArray attributesToRemove = new JsonArray();
        for (String attribute : src.attributesToRemove) {
          attributesToRemove.add(new JsonPrimitive(attribute));
        }
        tag.add("TagKeys", attributesToRemove);
      }
      return tag;
    }
  }

  static class TagModelDeserializer implements JsonDeserializer<TagModel> {

    @Override
    public TagModel deserialize(
        JsonElement json,
        Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {

      TagModel model = new TagModel();
      JsonObject tag = json.getAsJsonObject();
      model.project = tag.get("DatabaseName").getAsString();
      model.classification = tag.get("Classification").getAsString();
      model.name = tag.get("Name").getAsString();
      model.createdTime = new Date(tag.get("CreateTime").getAsLong());
      model.lastModifiedTime = new Date(tag.get("UpdateTime").getAsLong());
      model.owner = tag.get("Owner").getAsString();
      if (tag.has("ExtendedInfo")) {
        JsonObject extendedInfo = tag.get("ExtendedInfo").getAsJsonObject();
        if (extendedInfo.has("Policy")) {
          model.policy = extendedInfo.get("Policy").getAsString();
        }
      }

      model.attributes.clear();
      JsonObject tagValues = tag.get("TagValues").getAsJsonObject();
      for (Entry<String, JsonElement> entry : tagValues.entrySet()) {
        model.attributes.put(entry.getKey(), entry.getValue().getAsString());
      }

      return model;
    }
  }

  static class TagModel {
    String project;
    String classification;
    String name;
    String owner;
    Date lastModifiedTime;
    Date createdTime;
    Map<String, String> attributes = new HashMap<>();
    List<String> attributesToRemove = new LinkedList<>();
    String policy;
  }

  TagModel model;
  Odps odps;
  RestClient client;

  Tag(TagBuilder builder) {
    TagModel model = new TagModel();
    model.project = builder.classification.getProject();
    model.classification = builder.classification.getName();
    model.name = builder.name;
    model.attributes = new HashMap<>(builder.attributes);
    model.policy = builder.policy;

    this.model = model;
    this.odps = builder.classification.odps;
    this.client = odps.getRestClient();
  }

  Tag(TagModel model, Odps odps) {
    this.model = Objects.requireNonNull(model);
    this.odps = Objects.requireNonNull(odps);
    this.client = odps.getRestClient();
  }

  public String getProject() {
    return model.project;
  }

  public String getClassification() {
    return model.classification;
  }

  public String getName() {
    return model.name;
  }

  public String getOwner() {
    lazyLoad();
    return model.owner;
  }

  public Date getLastModifiedTime() {
    lazyLoad();
    return model.lastModifiedTime;
  }

  public Date getCreatedTime() {
    lazyLoad();
    return model.createdTime;
  }

  public Map<String, String> getAttributes() {
    lazyLoad();
    return new HashMap<>(model.attributes);
  }

  public void setAttribute(String name, String value) {
    lazyLoad();
    model.attributes.put(Objects.requireNonNull(name), Objects.requireNonNull(value));
  }

  public void removeAttribute(String name) {
    lazyLoad();
    if (!model.attributes.containsKey(name)) {
      throw new IllegalArgumentException("Attribute '" + name + "' does not exist");
    }
    model.attributesToRemove.add(Objects.requireNonNull(name));
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildTagResource(
        model.project,
        model.classification,
        model.name);
    Response resp = client.request(resource, "GET", null, null, null);

    JsonParser parser = new JsonParser();
    String json = new String(resp.getBody(), StandardCharsets.UTF_8);
    JsonObject jsonObject = parser.parse(json).getAsJsonObject();

    this.model = GSON.fromJson(jsonObject.get("Tag"), TagModel.class);
    setLoaded(true);
  }
}
