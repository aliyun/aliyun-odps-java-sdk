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
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
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
  public enum Type {
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
    VOLUMEARCHIVE,

    /**
     * 用户设置的或系统暂不支持的资源类型
     */
    UNKOWN
  }

  /**
   * Resource model
   */
  @Root(name = "Resource", strict = false)
  static class ResourceModel {

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;

    @Element(name = "ResourceType", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String type;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModifiedTime;

    @Element(name = "LastUpdator", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String lastUpdator;

    @Element(name = "ResourceSize", required = false)
    Long size;

    @Element(name = "TableName", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String sourceTableName;

    String contentMD5;

    boolean isTempResource;
    String volumePath;
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
      case VOLUMEFILE:
        resource = new VolumeFileResource();
        break;
      case VOLUMEARCHIVE:
        resource = new VolumeArchiveResource();
        break;
      case UNKOWN:
        resource = new Resource();
        break;
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

    if (model.type == null) {
      return Type.UNKOWN;
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
   * 获得资源最后更新者
   *
   * @return 资源最后更新者
   */
  public String getLastUpdator() {
    if (model.lastUpdator == null && client != null) {
      lazyLoad();
    }
    return model.lastUpdator;
  }

  /**
   * 获得资源大小
   *
   * 注意: 表 和 volumn 返回 null
   * @return 资源大小
   */
  public Long getSize() {
    if (model.size == null && client != null) {
      lazyLoad();
    }
    return model.size;
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
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("meta", null);
    Response rp = client.request(resource, "GET", params, null, null);
    Map<String, String> headers = rp.getHeaders();
    model.owner = headers.get(Headers.ODPS_OWNER);
    model.type = headers.get(Headers.ODPS_RESOURCE_TYPE);
    model.comment = headers.get(Headers.ODPS_COMMENT);
    model.lastUpdator = headers.get(Headers.ODPS_RESOURCE_LAST_UPDATOR);

    String sizeStr = headers.get(Headers.ODPS_RESOURCE_SIZE);
    try {
      model.size = (sizeStr == null ? null : Long.parseLong(sizeStr));
    } catch (NumberFormatException e) {
      throw new OdpsException("Invalid resource size format" + sizeStr, e);
    }

    try {
      model.createdTime = DateUtils.parseRfc822Date(headers
                                                        .get(Headers.ODPS_CREATION_TIME));
      model.lastModifiedTime = DateUtils.parseRfc822Date(headers
                                                             .get(Headers.LAST_MODIFIED));
    } catch (Exception e) {
      throw new OdpsException("Invalid date format", e);
    }

    model.sourceTableName = headers.get(Headers.ODPS_COPY_TABLE_SOURCE);
    model.volumePath = headers.get(Headers.ODPS_COPY_FILE_SOURCE);
    model.contentMD5 = headers.get(Headers.CONTENT_MD5);
    setLoaded(true);
  }

  /**
   * 更新 资源的 owner
   *    需要是 project owner
   *
   * @param newOwner
   * @throws OdpsException
   */
  public void updateOwner(String newOwner) throws OdpsException {
    String method = "PUT";
    String resource = ResourceBuilder.buildResourceResource(project, model.name);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("updateowner", null);
    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.ODPS_OWNER, newOwner);

    client.request(resource, method, params, headers, null);

    model.owner = newOwner;
  }
}
