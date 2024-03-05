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
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

public class Role extends LazyLoad {

  @Root(name = "Role", strict = false)
  static class RoleModel {

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;

    @Element(name = "Type", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String type;
  }

  private RoleModel model;
  private String project;
  private RestClient client;

  Role(RoleModel model, String project, RestClient client) {
    this.model = model;
    this.project = project;
    this.client = client;
  }

  @Override
  public void reload() throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project).append("/roles/")
        .append(model.name);
    model = client.request(RoleModel.class, resource.toString(), "GET");
    setLoaded(true);
  }

  public String getName() {
    return model.name;
  }

  public String getType() {
    lazyLoad();

    return model.type;
  }

  public String getComment() {
    lazyLoad();
    return model.comment;
  }
}
