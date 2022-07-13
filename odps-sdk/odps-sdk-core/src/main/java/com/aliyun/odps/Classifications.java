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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.aliyun.odps.Classification.AttributeDefinition;
import com.aliyun.odps.Classification.AttributeDeserializer;
import com.aliyun.odps.Classification.AttributeSerializer;
import com.aliyun.odps.Classification.ClassificationModel;
import com.aliyun.odps.Classification.ClassificationModelDeserializer;
import com.aliyun.odps.Classification.ClassificationModelSerializer;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public class Classifications implements Iterable<Classification> {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeHierarchyAdapter(AttributeDefinition.class, new AttributeDeserializer())
      .registerTypeHierarchyAdapter(AttributeDefinition.class, new AttributeSerializer())
      .registerTypeAdapter(ClassificationModel.class, new ClassificationModelDeserializer())
      .registerTypeAdapter(ClassificationModel.class, new ClassificationModelSerializer())
      .create();

  private class ClassificationIterator extends ListIterator<Classification> {

    Map<String, String> params = new HashMap<>();
    private String project;

    ClassificationIterator(String project) {
      this.project = Objects.requireNonNull(project);
    }

    @Override
    protected List<Classification> list() {
      List<Classification> classifications = new LinkedList<>();

      String lastMarker = params.get("NextPageToken");

      // End of list
      if (params.containsKey("NextPageToken") && lastMarker.isEmpty()) {
        return null;
      }

      String resource = ResourceBuilder.buildClassificationsResource(project);
      try {
        Response resp = client.request(resource, "GET", params, null, null);
        String respBody = new String(resp.getBody(), StandardCharsets.UTF_8);
        JsonParser parser = new JsonParser();
        JsonElement parsedRespBody = parser.parse(respBody);

        JsonObject jsonObject = parsedRespBody.getAsJsonObject();
        if (jsonObject.has("NextPageToken")) {
          params.put("NextPageToken", jsonObject.get("NextPageToken").getAsString());
        }

        if (jsonObject.has("Classifications")) {
          Type t = new TypeToken<List<ClassificationModel>>() {}.getType();
          List<ClassificationModel> classificationModels =
              GSON.fromJson(jsonObject.get("Classifications").getAsJsonArray(), t);
          classifications.addAll(
              classificationModels
                  .stream()
                  .map(m -> new Classification(m, odps))
                  .collect(Collectors.toList())
          );
        }
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      return classifications;
    }
  }

  RestClient client;
  Odps odps;

  Classifications(Odps odps) {
    this.odps = Objects.requireNonNull(odps);
    this.client = odps.getRestClient();
  }

  public void create(
      String name,
      Map<String, AttributeDefinition> attributes,
      boolean ifNotExists) throws OdpsException {
    ClassificationModel model = new ClassificationModel();
    model.project = odps.getDefaultProject();
    model.name = Objects.requireNonNull(name);
    model.attributes = Objects.requireNonNull(attributes);
    Classification classification = new Classification(model, odps);

    JsonObject root = new JsonObject();
    root.add("ClassificationInput", GSON.toJsonTree(classification.model));
    root.addProperty("IfNotExists", ifNotExists);

    String resource = ResourceBuilder.buildClassificationsResource(odps.getDefaultProject());
    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/json");
    String body = GSON.toJson(root);

    client.request(resource, "POST", null, headers, body.getBytes());
  }

  public void delete(String name) throws OdpsException {
    String resource = ResourceBuilder.buildClassificationResource(
        odps.getDefaultProject(),
        Objects.requireNonNull(name));
    client.request(resource, "DELETE", null, null, null);
  }

  public boolean exists(String name) throws OdpsException {
    try {
      Classification classification = get(name);
      classification.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  public Classification get(String name) {
    ClassificationModel model = new ClassificationModel();
    model.project = odps.getDefaultProject();
    model.name = Objects.requireNonNull(name);

    return new Classification(model, odps);
  }

  @Override
  public Iterator<Classification> iterator() {
    return new ClassificationIterator(odps.getDefaultProject());
  }
}
