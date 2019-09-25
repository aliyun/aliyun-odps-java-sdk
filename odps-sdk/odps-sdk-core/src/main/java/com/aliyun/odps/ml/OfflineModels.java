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
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.*;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.ml.OfflineModel.OfflineModelDesc;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;


/**
 * OfflineModels表示ODPS中所有离线模型的集合
 *
 * @author chao.liu@alibaba-inc.com
 */
public class OfflineModels implements Iterable<OfflineModel> {

  @Root(name = "OfflineModels", strict = false)
  private static class ListOfflineModelsResponse {

    @ElementList(entry = "OfflineModel", inline = true, required = false)
    private List<OfflineModelDesc> offlineModelDescs = new ArrayList<OfflineModelDesc>();

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;
  }

  private RestClient client;
  private Odps odps;

  public OfflineModels(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * 获得指定的离线模型信息
   *
   * @param modelName
   *     离线模型名
   * @return{@link OfflineModel}对象
   */
  public OfflineModel get(String modelName) {
    return get(getDefaultProjectName(), modelName);
  }

  /**
   * 获得指定模型信息
   *
   * @param projectName
   *     所在Project名称
   * @param modelName
   *     离线模型名
   * @return{@link OfflineModel}对象
   */
  public OfflineModel get(String projectName, String modelName) {
    OfflineModelDesc desc = new OfflineModelDesc();
    desc.modelName = modelName;
    return new OfflineModel(desc, projectName, odps);
  }

  /**
   * 判断指定离线模型是否存在
   *
   * @param modelName
   *     离线模型名
   * @return 存在返回true, 否则返回false
   * @throws OdpsException
   */
  public boolean exists(String modelName) throws OdpsException {
    return exists(getDefaultProjectName(), modelName);
  }

  /**
   * 判断指定离线模型是否存在
   *
   * @param projectName
   *     所在Project名称
   * @param modelName
   *     离线模型名
   * @return 存在返回true, 否则返回flase
   * @throws OdpsException
   */
  public boolean exists(String projectName, String modelName)
      throws OdpsException {
    try {
      OfflineModel m = get(projectName, modelName);
      m.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  /**
   * 获取默认Project的所有模型信息迭代器
   *
   * @return 模型迭代器
   */
  @Override
  public Iterator<OfflineModel> iterator() {
    return iterator(getDefaultProjectName(), null);
  }

  /**
   * 获取离线模型信息迭代器
   *
   * @param projectName
   *     指定Project名称
   * @return 模型迭代器
   */
  public Iterator<OfflineModel> iterator(final String projectName) {
    return iterator(projectName, null);
  }

  /**
   * 获取默认Project的离线模型信息迭代器
   *
   * @param filter
   *     过滤条件
   * @return 离线模型迭代器
   */
  public Iterator<OfflineModel> iterator(final OfflineModelFilter filter) {
    return iterator(getDefaultProjectName(), filter);
  }

  /**
   * 获得离线模型信息迭代器
   *
   * @param projectName
   *     所在Project名称
   * @param filter
   *     过滤条件
   * @return 离线模型迭代器
   */
  public Iterator<OfflineModel> iterator(final String projectName,
                                         final OfflineModelFilter filter) {
    return new ListIterator<OfflineModel>() {

      Map<String, String> params = new HashMap<String, String>();

      @Override
      protected List<OfflineModel> list() {
        ArrayList<OfflineModel> models = new ArrayList<OfflineModel>();
        params.put("expectmarker", "true"); // since sprint-11

        String lastMarker = params.get("marker");
        if (params.containsKey("marker") && lastMarker.length() == 0) {
          return null;
        }

        if (filter != null) {
          if (filter.getName() != null) {
            params.put("name", filter.getName());
          }

          if (filter.getOwner() != null) {
            params.put("owner", filter.getOwner());
          }
        }

        String resource = ResourceBuilder.buildOfflineModelResource(projectName);
        try {
          ListOfflineModelsResponse resp = client.request(ListOfflineModelsResponse.class,
                                                          resource, "GET", params);

          for (OfflineModelDesc desc : resp.offlineModelDescs) {
            OfflineModel m = new OfflineModel(desc, projectName, odps);
            models.add(m);
          }
          params.put("marker", resp.marker);
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }

        return models;
      }
    };
  }

  /**
   * 创建离线模型, 返回负责创建离线模型的XmodelTask的instance
   * @return Instance
   * @param modelInfo
   */
  public Instance create(OfflineModelInfo modelInfo) throws OdpsException {
    return create(getDefaultProjectName(), modelInfo);
  }

  /**
   * 创建离线模型, 返回负责创建离线模型的XmodelTask的instance
   * @return Odps Instance
   * @param project
   * @param modelInfo
   */
  public Instance create(String project, OfflineModelInfo modelInfo) throws OdpsException {
    String xml = null;
    try {
      xml = SimpleXmlUtils.marshal(modelInfo);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    String resource = ModelResourceBuilder.buildOfflineModelResource(project);
    Response resp = client.stringRequest(resource, "POST", null, headers, xml);

    String location = resp.getHeaders().get(Headers.LOCATION);
    if (location == null || location.trim().length() == 0) {
      throw new OdpsException("Invalid response, Location header required.");
    }
    // location:service_name/projectname/instance/instanceid
    location = location.trim();
    String instId = location.substring(location.lastIndexOf('/') + 1);
    if (instId.trim().length() == 0) {
      throw new OdpsException("Create offlinemodel failed: Instance id not found, " + location);
    }

    if (odps.instances().exists(project, instId)) {
      return odps.instances().get(project, instId);
    } else {
      throw new OdpsException("Create offlinemodel failed: Instance not found, " + instId);
    }
  }

  /**
   * 创建离线模型, 返回负责复制离线模型的XmodelTask的instance
   * @return Odps Instance
   * @param modelInfo
   */
  public Instance copy(String project, OfflineModelInfo modelInfo) throws OdpsException {
    String xml = null;
    try {
      xml = SimpleXmlUtils.marshal(modelInfo);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    String resource = ModelResourceBuilder.buildOfflineModelResource(project);
    Response resp = client.stringRequest(resource, "POST", null, headers, xml);

    String location = resp.getHeaders().get(Headers.LOCATION);
    if (location == null || location.trim().length() == 0) {
      throw new OdpsException("Invalid response, Location header required.");
    }
    // location:service_name/projectname/instance/instanceid
    location = location.trim();
    String instId = location.substring(location.lastIndexOf('/') + 1);
    if (instId.trim().length() == 0) {
      throw new OdpsException("Copy offlinemodel failed: Instance id not found, " + location);
    }

    if (odps.instances().exists(project, instId)) {
      return odps.instances().get(project, instId);
    } else {
      throw new OdpsException("Copy offlinemodel failed: Instance not found, " + instId);
    }
  }

  /**
   * 删除离线模型
   *
   * @param modelName
   *     离线模型名
   * @throws OdpsException
   */
  public void delete(String modelName) throws OdpsException {
    delete(client.getDefaultProject(), modelName);
  }

  /**
   * 删除离线模型
   *
   * @param projectName
   *     离线模型所在Project
   * @param modelName
   *     离线模型名
   * @throws OdpsException
   */
  public void delete(String projectName, String modelName) throws OdpsException {
    String resource = ResourceBuilder.buildOfflineModelResource(projectName, modelName);
    client.request(resource, "DELETE", null, null, null);
  }

  /* private */
  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }
}
