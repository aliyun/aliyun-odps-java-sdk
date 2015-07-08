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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.Function.FunctionModel;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * 表示ODPS内所有{@link Function}的集合
 *
 * @author zhemin.nizm@alibaba-inc.com
 */
public class Functions implements Iterable<Function> {

  private RestClient client;
  private Odps odps;

  /* only for function listing */
  @XmlRootElement(name = "Functions")
  private static class ListFunctionsResponse {

    @XmlElement(name = "Function")
    private List<FunctionModel> functions = new ArrayList<FunctionModel>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;
  }

  /* create a new instance with RestClient */
  Functions(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * 创建函数
   *
   * @param func
   *     函数结构 {@link Function}
   * @throws OdpsException
   *     创建函数失败
   */
  public void create(Function func) throws OdpsException {
    create(getDefaultProjectName(), func);
  }

  /**
   * 创建函数
   *
   * @param projectName
   *     函数所在{@link Project}名称
   * @param func
   *     函数结构 {@link Function}
   * @throws OdpsException
   *     创建函数失败
   */
  public void create(String projectName, Function func) throws OdpsException {
    String resource = ResourceBuilder.buildFunctionsResource(projectName);
    HashMap<String, String> header = new HashMap<String, String>();
    header.put("Content-Type", "application/xml");

    String ret;
    try {
      ret = JAXBUtils.marshal(func.model, FunctionModel.class);
    } catch (JAXBException e) {
      throw new OdpsException(e);
    }
    client.stringRequest(resource, "POST", null, header, ret);
  }

  /**
   * 删除函数
   *
   * @param name
   *     函数名称
   * @throws OdpsException
   *     删除函数失败
   */
  public void delete(String name) throws OdpsException {
    delete(getDefaultProjectName(), name);
  }

  /**
   * 删除函数
   *
   * @param projectName
   *     函数所在{@link Project}名称
   * @param name
   *     函数名称
   * @throws OdpsException
   *     删除函数失败
   */
  public void delete(String projectName, String name) throws OdpsException {
    String resource = ResourceBuilder.buildFunctionResource(projectName, name);
    client.request(resource, "DELETE", null, null, null);
  }

  /**
   * 返回默认Project下所有函数的迭代器
   *
   * @return {@link Function}迭代器
   */
  @Override
  public Iterator<Function> iterator() {
    return iterator(getDefaultProjectName());
  }

  /**
   * 返回指定Project下所有函数的迭代器
   *
   * @param projectName
   *     Project名称
   * @return {@link Function}迭代器
   */
  public Iterator<Function> iterator(final String projectName) {
    return new ListIterator<Function>() {

      Map<String, String> params = new HashMap<String, String>();

      @Override
      protected List<Function> list() {
        ArrayList<Function> functions = new ArrayList<Function>();

        params.put("expectmarker", "true"); // since sprint-11

        String lastMarker = params.get("marker");
        if (params.containsKey("marker") && lastMarker.length() == 0) {
          return null;
        }

        String resource = ResourceBuilder.buildFunctionsResource(projectName);
        try {

          ListFunctionsResponse resp = client.request(
              ListFunctionsResponse.class, resource, "GET", params);

          for (FunctionModel model : resp.functions) {
            Function t = new Function(model, projectName, odps);
            functions.add(t);
          }

          params.put("marker", resp.marker);
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }

        return functions;
      }
    };
  }

  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }
}
