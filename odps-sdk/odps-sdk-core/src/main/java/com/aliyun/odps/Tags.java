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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.aliyun.odps.Tag.TagModel;
import com.aliyun.odps.Tag.TagModelDeserializer;
import com.aliyun.odps.Tag.TagModelSerializer;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

public class Tags implements Iterable<Tag> {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(TagModel.class, new TagModelSerializer())
      .registerTypeAdapter(TagModel.class, new TagModelDeserializer())
      .create();

  private class TagIterator extends ListIterator<Tag> {

    Map<String, String> params = new HashMap<>();

    private String project;
    private String classification;

    TagIterator(String project, String classification) {
      this.project = Objects.requireNonNull(project);
      this.classification = Objects.requireNonNull(classification);
    }

    @Override
    protected List<Tag> list() {
      List<Tag> tags = new LinkedList<>();

      String lastMarker = params.get("NextPageToken");

      // End of list
      if (params.containsKey("NextPageToken") && lastMarker.isEmpty()) {
        return null;
      }

      String resource = ResourceBuilder.buildTagsResource(project, classification);
      try {
        Response resp = client.request(resource, "GET", params, null, null);
        String respBody = new String(resp.getBody(), StandardCharsets.UTF_8);
        JsonParser parser = new JsonParser();
        JsonElement parsedRespBody = parser.parse(respBody);

        if (!parsedRespBody.isJsonObject()) {
          throw new JsonParseException("Expect a JsonObject, but got: " + respBody);
        }

        JsonObject jsonObject = parsedRespBody.getAsJsonObject();
        if (jsonObject.has("NextPageToken")) {
          params.put("NextPageToken", jsonObject.get("NextPageToken").getAsString());
        }

        if (jsonObject.has("Tags")) {
          if (!jsonObject.get("Tags").isJsonArray()) {
            throw new JsonParseException(
                "Expect a JsonArray, but got: " + jsonObject.get("Tags").toString());
          }

          JsonArray tagElements = jsonObject.get("Tags").getAsJsonArray();
          for (JsonElement element : tagElements) {
            TagModel model = GSON.fromJson(element, TagModel.class);
            tags.add(new Tag(model, odps));
          }
        }
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      return tags;
    }
  }

  public static class TagBuilder {
    Classification classification;
    String name;
    Map<String, String> attributes = new HashMap<>();
    String policy;

    public TagBuilder(Classification classification, String name) {
      this.classification = Objects.requireNonNull(classification);
      this.name = Objects.requireNonNull(name);
    }

    public TagBuilder attribute(String attributeName, String attribute) {
      if (!classification.getAttributeDefinitions().containsKey(attributeName)) {
        throw new IllegalArgumentException(
            "Attribute name not defined in this classification: " + attributeName);
      }
      this.attributes.put(Objects.requireNonNull(attributeName), Objects.requireNonNull(attribute));
      return this;
    }

    public TagBuilder policy(String policy) {
      this.policy = Objects.requireNonNull(policy);
      return this;
    }
  }

  private Odps odps;
  private Classification classification;
  private RestClient client;

  Tags(Odps odps, Classification classification) {
    this.classification = Objects.requireNonNull(classification);
    this.odps = Objects.requireNonNull(odps);
    this.client = odps.getRestClient();
  }

  public void create(TagBuilder tagBuilder, boolean ifNotExists) throws OdpsException {
    Objects.requireNonNull(tagBuilder);

    TagModel model = new TagModel();
    model.project = classification.getProject();
    model.classification = classification.getName();
    model.name = tagBuilder.name;
    model.attributes = new HashMap<>(tagBuilder.attributes);
    model.policy = tagBuilder.policy;

    JsonObject root = new JsonObject();
    root.add("TagInput", GSON.toJsonTree(model));
    root.addProperty("IfNotExists", ifNotExists);

    String resource = ResourceBuilder.buildTagsResource(
        classification.getProject(),
        classification.getName());
    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/json");
    String body = GSON.toJson(root);

    client.request(resource, "POST", null, headers, body.getBytes());
  }

  public void update(Tag tag) throws OdpsException {
    Objects.requireNonNull(tag);

    JsonObject root = new JsonObject();
    root.add("UpdateTagInput", GSON.toJsonTree(tag.model));

    String resource = ResourceBuilder.buildTagResource(
        classification.getProject(),
        classification.getName(),
        tag.getName());
    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/json");
    String body = GSON.toJson(root);

    client.request(resource, "PUT", null, headers, body.getBytes());
  }

  public boolean exists(String name) throws OdpsException {
    try {
      Tag tag = get(name);
      tag.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  public Tag get(String name) {
    TagModel model = new TagModel();
    model.project = classification.getProject();
    model.classification = classification.getName();
    model.name = Objects.requireNonNull(name);
    return new Tag(model, odps);
  }

  @Override
  public Iterator<Tag> iterator() {
    return new TagIterator(classification.getProject(), classification.getName());
  }
}
