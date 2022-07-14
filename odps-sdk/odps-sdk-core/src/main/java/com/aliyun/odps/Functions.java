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
import com.aliyun.odps.utils.NameSpaceSchemaUtils;
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
    return get(odps.getDefaultProject(), functionName);
  }

  /**
   * 获取 Function 对象
   *
   * @param projectName Project 名
   * @param functionName Function 名字
   * @return
   */
  public Function get(String projectName, String functionName) {
    return get(projectName, odps.getCurrentSchema(), functionName);
  }

  /**
   * Get designated function.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param functionName Function name.
   * @return {@link Function}
   */
  public Function get(String projectName, String schemaName, String functionName) {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }

    if (StringUtils.isNullOrEmpty(functionName)) {
      throw new IllegalArgumentException("Argument 'functionName' cannot be null or empty");
    }

    FunctionModel model = new FunctionModel();
    model.schemaName = schemaName;
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
    return exists(projectName, odps.getCurrentSchema(), functionName);
  }

  /**
   * Check if designated function exists.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param functionName Function name.
   * @return True if the function exists, else false.
   * @throws OdpsException
   */
  public boolean exists(
      String projectName,
      String schemaName,
      String functionName) throws OdpsException {
    Function function = get(projectName, schemaName, functionName);
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
    update(odps.getDefaultProject(), func);
  }

  /**
   * 更新 Function
   *
   * @param projectName
   * @param func
   * @throws OdpsException
   */
  public void update(String projectName, Function func) throws OdpsException {
    update(projectName, odps.getCurrentSchema(), func);
  }

  /**
   * Update designated function.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param function {@link Function}
   * @throws OdpsException
   */
  public void update(
      String projectName,
      String schemaName,
      Function function) throws OdpsException {

    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }
    if (StringUtils.isNullOrEmpty(function.getName())) {
      throw new IllegalArgumentException(
          "Argument 'function' is invalid. Its name is null or empty");
    }

    String resource = ResourceBuilder.buildFunctionResource(projectName, function.getName());
    HashMap<String, String> header = new HashMap<>();
    header.put(Headers.CONTENT_TYPE, "application/xml");
    Map<String, String> params = NameSpaceSchemaUtils.initParamsWithSchema(schemaName);
    String body;
    try {
      body = SimpleXmlUtils.marshal(function.model);
    } catch (Exception e) {
      throw new OdpsException(e);
    }
    client.stringRequest(resource, "PUT", params, header, body);
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
    create(odps.getDefaultProject(), func);
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
    create(projectName, odps.getCurrentSchema(), func);
  }

  /**
   * Create a function.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param function {@link Function}
   */
  public void create(
      String projectName,
      String schemaName,
      Function function) throws OdpsException {

    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }
    if (StringUtils.isNullOrEmpty(function.getName())) {
      throw new IllegalArgumentException(
          "Argument 'function' is invalid. Its name is null or empty");
    }

    String resource = ResourceBuilder.buildFunctionsResource(projectName);
    Map<String, String> header = new HashMap<>();
    header.put(Headers.CONTENT_TYPE, "application/xml");
    Map<String, String> params = NameSpaceSchemaUtils.initParamsWithSchema(schemaName);
    String body;
    try {
      body = SimpleXmlUtils.marshal(function.model);
    } catch (Exception e) {
      throw new OdpsException(e);
    }
    client.stringRequest(resource, "POST", params, header, body);
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
    delete(odps.getDefaultProject(), name);
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
   * Delete designated function.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @param functionName Function name.
   */
  public void delete(
      String projectName,
      String schemaName,
      String functionName) throws OdpsException {
    if (StringUtils.isNullOrEmpty(projectName)) {
      throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
    }
    if (StringUtils.isNullOrEmpty(functionName)) {
      throw new IllegalArgumentException("Argument 'functionName' cannot be null or empty");
    }

    String resource = ResourceBuilder.buildFunctionResource(projectName, functionName);
    Map<String, String> params = NameSpaceSchemaUtils.initParamsWithSchema(schemaName);
    client.request(resource, "DELETE", params, null, null);
  }

  /**
   * 返回默认Project下所有函数的迭代器
   *
   * @return {@link Function}迭代器
   */
  @Override
  public Iterator<Function> iterator() {
    return iterator(odps.getDefaultProject());
  }

  /**
   * 返回指定Project下所有函数的迭代器
   *
   * @param projectName
   *     Project名称
   * @return {@link Function}迭代器
   */
  public Iterator<Function> iterator(final String projectName) {
    return iterator(projectName, odps.getCurrentSchema());
  }

  /**
   * Get a function iterator of the given schema in the given project.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @return A function iterator.
   */
  public Iterator<Function> iterator(final String projectName, String schemaName) {
    return new FunctionListIterator(projectName, schemaName);
  }


  /**
   * 返回指定Project下所有函数的迭代器
   *
   * @return {@link Function}迭代器
   */
  public Iterable<Function> iterable() {
    return iterable(odps.getDefaultProject());
  }

  /**
   * 返回指定Project下所有函数的迭代器
   *
   * @param projectName
   *     Project名称
   * @return {@link Function}迭代器
   */
  public Iterable<Function> iterable(final String projectName) {
    return iterable(projectName, odps.getCurrentSchema());
  }

  /**
   * Get a function iterable of the given schema in the given project.
   *
   * @param projectName Project name.
   * @param schemaName Schema name. Null or empty string means using the default schema.
   * @return A function iterable.
   */
  public Iterable<Function> iterable(final String projectName, String schemaName) {
    return () -> new FunctionListIterator(projectName, schemaName);
  }

  private class FunctionListIterator extends ListIterator<Function> {
    Map<String, String> params = new HashMap<>();
    String projectName;
    String schemaName;

    public FunctionListIterator(final String projectName, String schemaName) {
      if (StringUtils.isNullOrEmpty(projectName)) {
        throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
      }

      this.projectName = projectName;
      this.schemaName = schemaName;
      params = NameSpaceSchemaUtils.initParamsWithSchema(schemaName);
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
          model.schemaName = schemaName;
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
