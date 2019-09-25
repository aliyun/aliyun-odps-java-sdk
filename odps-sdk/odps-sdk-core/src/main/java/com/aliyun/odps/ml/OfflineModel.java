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

package com.aliyun.odps.ml;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;

import com.aliyun.odps.LazyLoad;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * OfflineModel表示ODPS中的离线模型
 *
 * @author chao.liu@alibaba-inc.com
 */
public class OfflineModel extends LazyLoad {

  @Root(name = "OfflineModel", strict = false)
  static class OfflineModelDesc {

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String modelName;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModifiedTime;

    @Element(name = "Type", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String type;
  }

  private OfflineModelDesc modelDesc;
  private RestClient client;
  private String project;

  OfflineModel(OfflineModelDesc desc, String project, Odps odps) {
    this.modelDesc = desc;
    this.project = project;
    this.client = odps.getRestClient();
  }

  /**
   * 获取离线模型所属Project名称
   *
   * @return Project名称
   */
  public String getProject() {
    return this.project;
  }

  /**
   * 获取离线模型名
   *
   * @return 离线模型名称
   */
  public String getName() {
    return this.modelDesc.modelName;
  }

  /**
   * 获取离线模型所属用户
   *
   * @return 所属用户
   */
  public String getOwner() {
    if (this.modelDesc.owner == null && client != null) {
      lazyLoad();
    }
    return this.modelDesc.owner;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildOfflineModelResource(project,
                                                                modelDesc.modelName);
    modelDesc = client.request(OfflineModelDesc.class, resource, "GET");
  }

  public String getModel() throws OdpsException {
    String resource = ResourceBuilder.buildOfflineModelResource(
        project, modelDesc.modelName);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("data", null);
    Response response = client.request(resource, "GET", params, null, null, 0);
    try {
      return new String(response.getBody(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  /**
   * 获取离线模型 创建时间
   *
   * @return
   */
  public Date getCreatedTime() {
    if (modelDesc.createdTime == null && client != null) {
      lazyLoad();
    }
    return modelDesc.createdTime;
  }

  /**
   * 获取离线模型 修改时间
   *
   * @return
   */
  public Date getLastModifiedTime() {
    if (modelDesc.lastModifiedTime == null && client != null) {
      lazyLoad();
    }
    return modelDesc.lastModifiedTime;
  }

  /**
   * 获取离线模型 类型:经典机器学习模型/深度学习模型
   *
   * @return
   */
  public String getType() {
    if(modelDesc.type == null && client != null) {
      lazyLoad();
    }
    return modelDesc.type;
  }
}
