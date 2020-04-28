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

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.utils.StringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Function.FunctionModel;
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
  @Root(name = "Functions", strict = false)
  static class ListFunctionsResponse {

    @ElementList(entry = "Function", inline = true, required = false)
    List<FunctionModel> functions = new ArrayList<FunctionModel>();

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String marker;

    @Element(name = "MaxItems", required = false)
    Integer maxItems;
  }

  /* create a new instance with RestClient */
  Functions(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }


  /**
   * 获取 Function 对象
   *
   * @param functionName Function 名字
   * @return
   * @throws OdpsException
   */
  public Function get(String functionName)  throws OdpsException {
    return get(getDefaultProjectName(), functionName);
  }

  /**
   * 获取 Function 对象
   *
   * @param projectName Project 名
   * @param functionName Function 名字
   * @return
   */
  public Function get(String projectName, String functionName) {
    FunctionModel model = new FunctionModel();
    model.name = functionName;
    return new Function(model, projectName, odps);
  }

  /**
   * 判断 Function 是否存在
   *
   * @param functionName Function 名字
   * @return
   */
  public boolean exists(String functionName) throws OdpsException {
    return exists(odps.getDefaultProject(), functionName);
  }

  /**
   * 判断 Function 是否存在
   *
   * @param projectName Project 名
   * @param functionName Function 名字
   * @return
   */
  public boolean exists(String projectName, String functionName) throws OdpsException {
    Function function = get(projectName, functionName);
    try {
      function.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  /**
   * 更新 Function
   *
   * @param func
   * @throws OdpsException
   */
  public void update(Function func) throws OdpsException {
    update(getDefaultProjectName(), func);
  }

  /**
   * 更新 Function
   *
   * @param projectName
   * @param func
   * @throws OdpsException
   */
  public void update(String projectName, Function func) throws OdpsException {
    String resource = ResourceBuilder.buildFunctionResource(projectName, func.getName());
    HashMap<String, String> header = new HashMap<String, String>();
    header.put(Headers.CONTENT_TYPE, "application/xml");

    String ret;
    try {
      ret = SimpleXmlUtils.marshal(func.model);
    } catch (Exception e) {
      throw new OdpsException(e);
    }
    client.stringRequest(resource, "PUT", null, header, ret);
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
    header.put(Headers.CONTENT_TYPE, "application/xml");

    String ret;
    try {
      ret = SimpleXmlUtils.marshal(func.model);
    } catch (Exception e) {
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
   * @return {@link Function}迭代器
   */

  public Iterable<Function> iterable() {
    return new Iterable<Function>() {
      @Override
      public Iterator<Function> iterator() {
        return new FunctionListIterator(getDefaultProjectName());
      }
    };
  }

  /**
   * 返回指定Project下所有函数的迭代器
   *
   * @param projectName
   *     Project名称
   * @return {@link Function}迭代器
   */

  public Iterable<Function> iterable(final String projectName) {
    return new Iterable<Function>() {
      @Override
      public Iterator<Function> iterator() {
        return new FunctionListIterator(projectName);
      }
    };
  }

  /**
   * 返回指定Project下所有函数的迭代器
   *
   * @param projectName
   *     Project名称
   * @return {@link Function}迭代器
   */
  public Iterator<Function> iterator(final String projectName) {
    return new FunctionListIterator(projectName);
  }

  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }

  private class FunctionListIterator extends ListIterator<Function> {
    Map<String, String> params = new HashMap<String, String>();
    String projectName;

    public FunctionListIterator(final String projectName) {
      this.projectName = projectName;
    }

    @Override
    protected List<Function> list() {
      ArrayList<Function> functions = new ArrayList<Function>();

      params.put("expectmarker", "true"); // since sprint-11

      String lastMarker = params.get("marker");
      if (params.containsKey("marker") && StringUtils.isNullOrEmpty(lastMarker)) {
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
  }
}
