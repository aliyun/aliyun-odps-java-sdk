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

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Resource.ResourceModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * Function表示ODPS中的函数
 *
 * @author zhemin.nizm@alibaba-inc.com
 */
public class Function extends LazyLoad {

  /**
   * Function model
   */
  @Root(name = "Function", strict = false)
  static class FunctionModel {

    @Element(name = "Alias", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "ClassType", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String classType;

    @ElementList(name = "Resources", entry = "ResourceName", required = false)
    ArrayList<String> resources;
  }

  FunctionModel model;
  String project;
  RestClient client;
  Odps odps;

  public Function() {
    this.model = new FunctionModel();
  }

  /* package */
  Function(FunctionModel functionModel, String project, Odps odps) {
    this.model = functionModel;
    this.project = project;
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * 获得函数名
   *
   * @return 函数名
   */
  public String getName() {
    if (model.name == null && client != null) {
      lazyLoad();
    }
    return model.name;
  }

  /**
   * 设置函数名
   *
   * @param name
   *     函数名
   */
  public void setName(String name) {
    model.name = name;
  }

  /**
   * 获得函数所属用户
   *
   * @return 用户名
   */
  public String getOwner() {
    if (model.owner == null && client != null) {
      lazyLoad();
    }
    return model.owner;
  }

  /**
   * 获取函数创建时间
   *
   * @return 函数创建时间
   */
  public Date getCreatedTime() {
    if (model.createdTime == null) {
      lazyLoad();
    }
    return model.createdTime;
  }


  /**
   * 使用getClassPath替代
   */
  @Deprecated
  public String getClassType() {
    if (model.classType == null && client != null) {
      lazyLoad();
    }
    return model.classType;
  }

  /**
   * 获得函数使用的类名
   *
   * @return 函数使用的类名
   */
  public String getClassPath() {
    if (model.classType == null && client != null) {
      lazyLoad();
    }
    return model.classType;
  }

  /**
   * 建议使用setClassPath替代
   */
  @Deprecated
  public void setClassType(String classType) {
    model.classType = classType;
  }

  /**
   * 设置函数使用的类名
   *
   * @param classPath
   *     函数使用的类名
   */
  public void setClassPath(String classPath) {
    model.classType = classPath;
  }


  /**
   * 获得函数相关的资源列表。UDF所用到的资源列表，这个里面必须包括UDF代码所在的资源。如果用户UDF中需要读取其他资源文件，这个列表中还得包括UDF所读取的资源文件列表。
   *
   * @return 资源列表
   */
  public List<Resource> getResources() {
    if (model.resources == null && client != null) {
      lazyLoad();
    }

    /* copy */
    ArrayList<Resource> resources = new ArrayList<Resource>();
    if (model.resources != null) {
      Map<String, String> resourceNames = parseResourcesName(model.resources);

      for (Map.Entry<String, String> entry : resourceNames.entrySet()) {
        ResourceModel rm = new ResourceModel();
        rm.name = entry.getKey();
        Resource res = Resource.getResource(rm, entry.getValue(), odps);
        resources.add(res);
      }
    }

    return resources;
  }

  /**
   * 解析UDF所用到的资源的名字列表, 资源名字的格式为 <project_name>/resources/<resource_name>
   *
   * @param resources
   *     function 对应资源的名字列表
   * @return 资源名称与其所属 project 对: Map<resource_name, project_name>
   */
  private Map<String, String> parseResourcesName(List<String> resources) {
    Map<String, String> resourceMap = new HashMap<String, String>();

    for (String r : resources) {
      String[] splits = r.split("/resources/");
      String resourceProject = null;
      String resourceName = null;

      if (splits.length > 1) {
        resourceProject = splits[0];
        resourceName = splits[1];
      } else {
        resourceProject = this.project;
        resourceName = r;
      }

      resourceMap.put(resourceName, resourceProject);
    }
    return resourceMap;
  }

  /**
   * 获得函数相关的资源名称列表。
   *
   * @return 资源名称列表
   *     若该资源与 UDF 在同一 project, 则返回 resourcename
   *     若该资源与 UDF 在不同 project, 返回格式为 projectname/resourcename
   */
  public List<String> getResourceNames() {
    if (model.resources == null && client != null) {
      lazyLoad();
    }
    List<String> resourceNames = new ArrayList<String>();

    if (model.resources != null) {
      Map<String, String> resources = parseResourcesName(model.resources);

      for (Map.Entry<String, String> entry : resources.entrySet()) {
        if (entry.getValue().equals(this.project)) {
          resourceNames.add(entry.getKey());
        } else {
          resourceNames.add(entry.getValue() + "/" + entry.getKey());
        }
      }
    }
    return resourceNames;
  }

  /**
   * 设置函数依赖的相关资源
   *
   * @param resources
   *     资源列表
   */
  public void setResources(List<String> resources) {
    model.resources = new ArrayList<String>();
    model.resources.addAll(resources);
  }

  /**
   * 获取函数所在{@link Project}名称
   *
   * @return Project名称
   */
  public String getProject() {
    return project;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildFunctionResource(project, model.name);
    model = client.request(FunctionModel.class, resource, "GET", null);
  }

  public void updateOwner(String newOwner) throws OdpsException {
    String method = "PUT";
    String resource = ResourceBuilder.buildFunctionResource(project, model.name);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("updateowner", null);
    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.ODPS_OWNER, newOwner);
    FunctionModel model = new FunctionModel();
    client.request(resource, method, params, headers, null);
    this.model.owner = newOwner;
  }
}
