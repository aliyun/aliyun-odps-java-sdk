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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

@Deprecated
public class Tenant extends LazyLoad {

  public enum TenantState {
    NORMAL,
    SUSPENDED,
    DELETING,
    DELETED
  }

  static class TenantModel {
    public String name;
    public String ownerId;
    public String tenantId;
    public TenantState tenantState;
    public Date creationTime;
    public Date lastModifiedTime;
    public String tenantProperties;
  }

  static class TenantAdapter extends TypeAdapter<TenantModel> {
    @Override
    public void write(JsonWriter out, TenantModel value) throws IOException {
      // No need to implement
    }

    @Override
    public TenantModel read(JsonReader in) throws IOException {
      TenantModel model = new TenantModel();

      in.beginObject();
      while (in.hasNext()) {
        String name = in.nextName();
        switch (name) {
          case "Name":
            model.name = in.nextString();
            break;
          case "OwnerId":
            model.ownerId = in.nextString();
            break;
          case "TenantId":
            model.tenantId = in.nextString();
            break;
          case "State":
            model.tenantState = TenantState.valueOf(in.nextString().toUpperCase());
            break;
          case "CreateTime":
            model.creationTime = new Date(in.nextLong());
            break;
          case "UpdateTime":
            model.lastModifiedTime = new Date(in.nextLong());
            break;
          case "TenantMeta":
            model.tenantProperties = in.nextString();
            break;
          default:
            in.skipValue();
        }
      }
      in.endObject();
      return model;
    }
  }

  private TenantModel model;
  private RestClient client;

  Tenant(Odps odps) {
    this.model = new TenantModel();
    this.client = odps.getRestClient();
  }

  @Override
  public void reload() throws OdpsException {
    String resource = "/tenants/" + ResourceBuilder.encodeObjectName(model.tenantId);
    Response response = client.request(resource, "GET", null, null, null);

    String json = new String(response.getBody(), StandardCharsets.UTF_8);
    loadFromJson(json);
  }

  public void loadFromJson(String json) {
    // TODO: avoid multiple Gson instances
    Gson gson = new GsonBuilder()
        .disableHtmlEscaping()
        .registerTypeAdapter(TenantModel.class, new TenantAdapter())
        .create();

    JsonParser parser = new JsonParser();
    JsonObject jsonObject = parser.parse(json).getAsJsonObject();

    model = gson.fromJson(jsonObject.get("Tenant"), TenantModel.class);
    setLoaded(true);
  }

  public TenantSecurityManager getTenantSecurityManager() {
    return new TenantSecurityManager(client);
  }
}
