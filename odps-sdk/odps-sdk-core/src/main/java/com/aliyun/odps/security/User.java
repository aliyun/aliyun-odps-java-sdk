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

package com.aliyun.odps.security;

import com.aliyun.odps.LazyLoad;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

public class User extends LazyLoad {

  @Root(name = "User", strict = false)
  static class UserModel {

    @Element(name = "ID", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String id;

    @Element(name = "DisplayName", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String displayName;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;
  }

  private UserModel model;
  private String project;
  private RestClient client;

  User(UserModel model, String project, RestClient client) {
    this.model = model;
    this.project = project;
    this.client = client;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildUserResource(project, model.id);
    model = client.request(UserModel.class, resource, "GET");
    setLoaded(true);
  }

  public String getID() {
    return model.id;
  }

  public String getDisplayname() {
    return model.displayName;
  }

  public String getComment() {
    lazyLoad();
    return model.comment;
  }
}
