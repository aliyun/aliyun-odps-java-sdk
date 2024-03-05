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

import java.util.Date;
import java.util.Map;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

public class Schema extends LazyLoad {

  //TODO external schema not support yet
  enum SchemaType {
    MANAGED,
    EXTERNAL
  }

  @Root(name = "Schema", strict = false)
  static class SchemaModel {

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Project", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String project;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;

    @Element(name = "Type", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String type;

    @Element(name = "CreateTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createTime;

    @Element(name = "ModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date modifiedTime;

    @Element(name = "IfNotExists", required = false)
    Boolean ifNotExists;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;
  }

  RestClient client;
  Odps odps;
  SchemaModel model;

  Schema(SchemaModel model, String project, Odps odps) {
    this.model = model;
    this.model.project = project;
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * Return the schema name
   */
  public String getName() {
    lazyLoad();

    return model.name;
  }

  /**
   * Return the project name
   */
  public String getProjectName() {
    lazyLoad();

    return model.project;
  }

  /**
   * Return the schema comment
   */
  public String getComment() {
    lazyLoad();

    return model.comment;
  }

  /**
   * Return the schema owner
   */
  public String getOwner() {
    lazyLoad();

    return model.owner;
  }

  /**
   * Return the schema type
   */
  public String getType() {
    lazyLoad();

    return model.type;
  }

  /**
   * Return the schema createTime
   */
  public Date getCreateTime() {
    lazyLoad();

    return model.createTime;
  }

  /**
   * Return the schema modifiedTime
   */
  public Date getModifiedTime() {
    lazyLoad();

    return model.modifiedTime;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildSchemaResource(model.project, model.name);
    Response resp = client.request(resource, "GET", null, null, null);
    try {
      model = SimpleXmlUtils.unmarshal(resp, SchemaModel.class);
      Map<String, String> headers = resp.getHeaders();
      model.createTime = DateUtils.parseRfc822Date(headers.get(Headers.ODPS_CREATION_TIME));
      model.modifiedTime = DateUtils.parseRfc822Date(headers.get(Headers.LAST_MODIFIED));
    } catch (Exception e) {
      throw new OdpsException("Can't bind xml to " + SchemaModel.class, e);
    }
    setLoaded(true);
  }

}
