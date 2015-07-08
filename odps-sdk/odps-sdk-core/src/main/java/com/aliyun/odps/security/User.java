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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.LazyLoad;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.rest.RestClient;

public class User extends LazyLoad {

  @XmlRootElement(name = "User")
  static class UserModel {

    @XmlElement(name = "ID")
    String id;

    @XmlElement(name = "DisplayName")
    String displayName;

    @XmlElement(name = "Comment")
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
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project).append("/users/")
        .append(model.id);
    model = client.request(UserModel.class, resource.toString(), "GET");
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
