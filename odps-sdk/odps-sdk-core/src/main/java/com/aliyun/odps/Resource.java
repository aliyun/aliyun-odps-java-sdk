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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.Resources.ResourceHeaders;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * Resource表示ODPS中的资源
 *
 * @author zhemin.nizm@alibaba-inc.com
 */
public class Resource extends LazyLoad {

  /**
   * 表示ODPS中资源的类型
   *
   * @author zhemin.nizm@alibaba-inc.com
   */
  public static enum Type {
    /**
     * 文件类型资源
     */
    FILE,

    /**
     * JAR类型资源
     */
    JAR,
    /**
     * PY类型资源
     */
    PY,
    /**
     * ARCHIVE类型资源。ODPS通过资源名称中的后缀识别压缩类型，支持的压缩文件类型包括：.zip/.tgz/.tar.gz/.tar/.jar。
     */
    ARCHIVE,
    /**
     * 表类型资源
     */
    TABLE,

    VOLUMEFILE,

    /**
     * 用户设置的或系统暂不支持的资源类型
     */
    UNKOWN
  }

  /**
   * Resource model
   */
  @XmlRootElement(name = "Resource")
  @XmlAccessorType(XmlAccessType.FIELD)
  static class ResourceModel {

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "Owner")
    String owner;

    @XmlElement(name = "Comment")
    String comment;

    @XmlElement(name = "ResourceType")
    String type;

    @XmlElement(name = "CreationTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date createdTime;

    @XmlElement(name = "LastModifiedTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date lastModifiedTime;

    String sourceTableName;
    String contentMD5;

    boolean isTempResource;
  }

  ResourceModel model;
  String project;
  RestClient client;
  Odps odps;

  /**
   * 创建Resource类的对象
   */
  public Resource() {
    model = new ResourceModel();
  }

  Resource(ResourceModel model, String project, Odps odps) {
    this.model = model;
    this.project = project;
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  static Resource createResource(Type type) {
    Resource resource;
    switch (type) {
      case ARCHIVE:
        resource = new ArchiveResource();
        break;
      case FILE:
        resource = new FileResource();
        break;
      case JAR:
        resource = new JarResource();
        break;
      case PY:
        resource = new PyResource();
        break;
      case TABLE:
        resource = new TableResource();
        break;
      case UNKOWN:
        resource = new Resource();
      default:
        resource = new Resource();
    }
    return resource;
  }

  static Resource getResource(ResourceModel model, String project, Odps odps) {
    if (model.type == null) {
      Resource resource = new Resource(model, project, odps);
      try {
        resource.reload();
        model = resource.model;
      } catch (OdpsException e) {
        return resource;
      }
    }

    Type type = Type.valueOf(model.type.toUpperCase());
    Resource resource = createResource(type);
    resource.model = model;
    resource.project = project;
    resource.odps = odps;
    resource.client = odps.getRestClient();
    return resource;
  }

  /**
   * 获得资源名称
   *
   * @return 资源名称
   */
  public String getName() {
    if (model.name == null && client != null) {
      lazyLoad();
    }
    return model.name;
  }

  /**
   * 设置资源名称
   *
   * @param name
   *     资源名称
   */
  public void setName(String name) {
    model.name = name;
  }

  /**
   * 获取资源注释信息
   *
   * @return 注释信息
   */
  public String getComment() {
    if (model.comment == null && client != null) {
      lazyLoad();
    }
    return model.comment;
  }

  /**
   * 设置资源注释信息
   *
   * @param comment
   *     注释信息
   */
  public void setComment(String comment) {
    model.comment = comment;
  }

  /**
   * 获取资源所属用户
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
   * 获取资源类型
   *
   * @return 资源类型
   */
  public Type getType() {
    if (model.type == null && client != null) {
      lazyLoad();
    }

    try {
      return Type.valueOf(model.type.toUpperCase());
    } catch (IllegalArgumentException e) {
      return Type.UNKOWN;
    }
  }

  /**
   * 获取资源创建时间
   *
   * @return 创建时间
   */
  public Date getCreatedTime() {
    if (model.createdTime == null && client != null) {
      lazyLoad();
    }
    return model.createdTime;
  }

  /**
   * 获取资源最后修改时间
   *
   * @return 最后修改时间
   */
  public Date getLastModifiedTime() {
    if (model.lastModifiedTime == null && client != null) {
      lazyLoad();
    }
    return model.lastModifiedTime;
  }

  /**
   * 获取资源所在{@link Project}名称
   *
   * @return Project名称
   */
  public String getProject() {
    return project;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildResourceResource(project, model.name);
    Response rp = client.request(resource, "HEAD", null, null, null);
    Map<String, String> headers = rp.getHeaders();
    model.owner = headers.get(ResourceHeaders.X_ODPS_OWNER);
    model.type = headers.get(ResourceHeaders.X_ODPS_RESOURCE_TYPE);
    model.comment = headers.get(ResourceHeaders.X_ODPS_COMMENT);

    try {
      model.createdTime = DateUtils.parseRfc822Date(headers
                                                        .get("x-odps-creation-time"));
      model.lastModifiedTime = DateUtils.parseRfc822Date(headers
                                                             .get("Last-Modified"));
    } catch (Exception e) {
      throw new OdpsException("Invalid date format", e);
    }

    if (model.type.equalsIgnoreCase("table")) {
      model.sourceTableName = headers
          .get(ResourceHeaders.X_ODPS_COPY_TABLE_SOURCE);
    } else {
      model.contentMD5 = headers.get(Headers.CONTENT_MD5);
    }

    setLoaded(true);
  }

}
