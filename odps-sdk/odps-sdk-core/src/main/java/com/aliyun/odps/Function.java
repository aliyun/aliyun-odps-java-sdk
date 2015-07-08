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
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.Resource.ResourceModel;
import com.aliyun.odps.rest.JAXBUtils;
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
  @XmlRootElement(name = "Function")
  @XmlAccessorType(XmlAccessType.FIELD)
  static class FunctionModel {

    @XmlElement(name = "Alias")
    String name;

    @XmlElement(name = "Owner")
    String owner;

    @XmlElement(name = "CreationTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date createdTime;

    @XmlElement(name = "ClassType")
    String classType;

    @XmlElementWrapper(name = "Resources")
    @XmlElement(name = "ResourceName")
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
      for (String r : model.resources) {
        ResourceModel rm = new ResourceModel();
        rm.name = r;
        Resource res = Resource.getResource(rm, project, odps);
        resources.add(res);
      }
    }

    return resources;
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

  @Override
  public void reload() throws OdpsException {
    throw new OdpsException("Get function not supported yet.");
  }
}
