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


import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.simpleframework.xml.convert.Converter;
import com.aliyun.odps.simpleframework.xml.stream.InputNode;
import com.aliyun.odps.simpleframework.xml.stream.OutputNode;

/**
 * ODPS项目空间
 */
public class Project extends LazyLoad {

  /**
   * 项目空间状态
   */
  public static enum Status {
    /**
     * 正常
     */
    AVAILABLE,
    /**
     * 只读
     */
    READONLY,
    /**
     * 删除
     */
    DELETING,
    /**
     * 冻结
     */
    FROZEN,
    /**
     * 未知
     */
    UNKOWN
  }

  /**
   * Project model
   */
  @Root(name = "Project", strict = false)
  static class ProjectModel {

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date creationTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModified;

    @Element(name = "ProjectGroupName", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String projectGroupName;

    @Element(name = "Properties", required = false)
    @Convert(PropertyConverter.class)
    LinkedHashMap<String, String> properties;

    @Element(name = "ExtendedProperties", required = false)
    @Convert(PropertyConverter.class)
    LinkedHashMap<String, String> extendedProperties;

    @Element(name = "State", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String state;

    @Element(name = "Clusters", required = false)
    Clusters clusters;
  }

  public static class Cluster {

    Cluster() {
    }

    public Cluster(String name, String quotaID) {
      if (StringUtils.isNullOrEmpty(name) || StringUtils.isNullOrEmpty(quotaID)) {
        throw new IllegalArgumentException("Missing arguments: name, quotaID");
      }
      this.name = name;
      this.quotaID = quotaID;
    }

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "QuotaID", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String quotaID;

    public String getName() {
      return name;
    }

    public String getQuotaID() {
      return quotaID;
    }
  }

  static class Clusters {
    @ElementList(entry = "Cluster", inline = true, required = false)
    List<Cluster> entries = new ArrayList<Cluster>();
  }

  @Root(name = "Property", strict = false)
  static class Property {

    Property() {
    }

    Property(String name, String value) {
      this.name = name;
      this.value = value;
    }

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Value", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String value;
  }

  @Root(name = "Properties", strict = false)
  static class Properties {
    @ElementList(entry = "Property", inline = true, required = false)
    List<Property> entries = new ArrayList<Property>();
  }

  static class PropertyConverter implements Converter<LinkedHashMap<String, String>> {
    @Override
    public void write(OutputNode outputNode, LinkedHashMap<String, String> properties) throws Exception {
      for (Entry<String, String> entry : properties.entrySet()) {
        String name = entry.getKey();
        String value = entry.getValue();
        SimpleXmlUtils.marshal(new Property(name, value), outputNode);
      }

      outputNode.commit();
    }

    @Override
    public LinkedHashMap<String, String> read(InputNode inputNode) throws Exception {
      LinkedHashMap<String, String> properties = new LinkedHashMap<String, String>();
      Properties props = SimpleXmlUtils.unmarshal(inputNode, Properties.class);
      for (Property entry : props.entries) {
        properties.put(entry.name, entry.value);
      }
      return properties;
    }
  }

  private ProjectModel model;
  private RestClient client;

  private Map<String, String> properties = new LinkedHashMap<String, String>();
  private Map<String, String> allProperties;
  private SecurityManager securityManager = null;
  private Clusters clusters;

  Project(ProjectModel model, RestClient client) {
    this.model = model;
    this.client = client;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(model.name);
    Response resp = client.request(resource, "GET", null, null, null);
    try {
      model = SimpleXmlUtils.unmarshal(resp, ProjectModel.class);
      Map<String, String> headers = resp.getHeaders();
      model.owner = headers.get(Headers.ODPS_OWNER);
      model.creationTime = DateUtils.parseRfc822Date(headers
                                                         .get("x-odps-creation-time"));
      model.lastModified = DateUtils.parseRfc822Date(headers
                                                         .get("Last-Modified"));

      properties = model.properties;
      clusters = model.clusters;
    } catch (Exception e) {
      throw new OdpsException("Can't bind xml to " + ProjectModel.class, e);
    }
    setLoaded(true);
  }

  /**
   * 获取Project名称
   *
   * @return Project名称
   */
  public String getName() {
    return model.name;
  }

  /**
   * 获取Project注释
   *
   * @return Project注释
   */
  public String getComment() {
    if (model.comment == null) {
      lazyLoad();
    }
    return model.comment;
  }

  /**
   * 设置Project注释
   *
   * @param comment Project注释
   */
  void setComment(String comment) {
    model.comment = comment;
  }

  /**
   * 获得Project所属用户
   *
   * @return Project所属用户
   */
  public String getOwner() {
    if (model.owner == null) {
      lazyLoad();
    }
    return model.owner;
  }

  /**
   * 获取Project创建时间
   *
   * @return Project创建时间
   */
  public Date getCreatedTime() {
    if (model.creationTime == null) {
      lazyLoad();
    }
    return model.creationTime;
  }

  /**
   * 获取Project最后修改时间
   *
   * @return Project最后修改时间
   */
  public Date getLastModifiedTime() {
    if (model.lastModified == null) {
      lazyLoad();
    }
    return model.lastModified;
  }

  String getProjectGroupName() {
    lazyLoad();
    return model.projectGroupName;
  }

  /**
   * 获取Project当前状态
   *
   * @return {@link Status}
   */
  public Status getStatus() {
    if (model.state == null) {
      lazyLoad();
    }

    Status status = null;

    try {
      status = Status.valueOf(model.state.toUpperCase());
    } catch (Exception e) {
      return Status.UNKOWN;
    }
    return status;
  }

  /**
   * 获取 Project 已配置的属性
   *
   * @return 以key, value保存的配置信息
   */
  public Map<String, String> getProperties() {
    lazyLoad();
    return properties;
  }

  /**
   * 获取 Project 全部可配置的属性, 包含从 group 继承来的配置信息
   *
   * @return 以key, value报错的配置信息
   */
  public Map<String, String> getAllProperties() throws OdpsException {
    if (allProperties == null) {

      String resource = ResourceBuilder.buildProjectResource(model.name);
      Map<String, String> params = new LinkedHashMap<String, String>();
      params.put("properties", "all");
      Response resp = client.request(resource, "GET", params, null, null);
      try {
        ProjectModel model = SimpleXmlUtils.unmarshal(resp, ProjectModel.class);

        allProperties = model.properties;
      } catch (Exception e) {
        throw new OdpsException("Can't bind xml to " + ProjectModel.class, e);
      }
    }

    return allProperties;
  }



  /**
   * 获取Project所属集群信息
   *
   * @return Project所属集群信息
   */
  List<Cluster> getClusters() {
    lazyLoad();
    return clusters == null ? null : clusters.entries;
  }

  /**
   * 查询Project指定配置信息
   *
   * @param key
   *     配置项
   * @return 配置项对应的值
   */
  public String getProperty(String key) {
    lazyLoad();
    return properties == null ? null : properties.get(key);
  }

  /**
   * 获取 Project 扩展配置信息
   *
   * @return 以 key, value 保存的配置信息
   */
  public Map<String, String> getExtendedProperties() throws OdpsException {
    Map<String, String> param = new LinkedHashMap<String, String>();
    param.put("extended", null);
    String resource = ResourceBuilder.buildProjectResource(model.name);
    ProjectModel extendedModel = client.request(ProjectModel.class, resource, "GET", param);
    return extendedModel.extendedProperties;
  }

  /**
   * 获取{@link SecurityManager}对象
   *
   * @return {@link SecurityManager}对象
   */
  public SecurityManager getSecurityManager() {
    if (securityManager == null) {
      securityManager = new SecurityManager(model.name, client);
    }
    return securityManager;
  }

  public String getTunnelEndpoint() throws OdpsException {
    String protocol;
    try {
      URI u = new URI(client.getEndpoint());
      protocol = u.getScheme();
    } catch (URISyntaxException e) {
      throw new OdpsException(e.getMessage(), e);
    }

    String resource = ResourceBuilder.buildProjectResource(model.name).concat("/tunnel");
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("service", null);
    Response resp = client.request(resource, "GET", params, null, null);

    String tunnel;
    if (resp.isOK()) {
      tunnel = new String(resp.getBody());
    } else {
      throw new OdpsException("Can't get tunnel server address: " + resp.getStatus());
    }

    return protocol + "://" + tunnel;
  }

}
