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
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;

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
  @XmlRootElement(name = "Project")
  static class ProjectModel {

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "Comment")
    String comment;

    @XmlElement(name = "Owner")
    String owner;

    @XmlElement(name = "CreationTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date creationTime;

    @XmlElement(name = "LastModifiedTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date lastModified;

    @XmlElement(name = "ProjectGroupName")
    String projectGroupName;

    @XmlElement(name = "Properties")
    @XmlJavaTypeAdapter(PropertyAdapter.class)
    HashMap<String, String> properties;

    @XmlElement(name = "ExtendedProperties")
    @XmlJavaTypeAdapter(PropertyAdapter.class)
    HashMap<String, String> extendedProperties;


    @XmlElement(name = "State")
    String state;

    @XmlElement(name = "Clusters")
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

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "QuotaID")
    String quotaID;

    public String getName() {
      return name;
    }

    public String getQuotaID() {
      return quotaID;
    }
  }

  static class Clusters {

    @XmlElement(name = "Cluster")
    List<Cluster> entries = new ArrayList<Cluster>();
  }

  static class Property {

    Property() {
    }

    Property(String name, String value) {
      this.name = name;
      this.value = value;
    }

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "Value")
    String value;
  }

  static class Properties {

    @XmlElement(name = "Property")
    List<Property> entries = new ArrayList<Property>();
  }

  static class PropertyAdapter extends XmlAdapter<Properties, HashMap<String, String>> {

    @Override
    public HashMap<String, String> unmarshal(Properties in) throws Exception {
      if (in == null) {
        return null;
      }
      HashMap<String, String> hashMap = new HashMap<String, String>();
      for (Property entry : in.entries) {
        hashMap.put(entry.name, entry.value);
      }
      return hashMap;
    }

    @Override
    public Properties marshal(HashMap<String, String> map) throws Exception {
      if (map == null) {
        return null;
      }
      Properties props = new Properties();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        props.entries.add(new Property(entry.getKey(), entry.getValue()));
      }
      return props;
    }
  }

  private ProjectModel model;
  private RestClient client;

  private HashMap<String, String> properties = new HashMap<String, String>();
  private HashMap<String, String> allProperties;
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
      model = JAXBUtils.unmarshal(resp, ProjectModel.class);
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
   * @param Project注释
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
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("properties", "all");
      Response resp = client.request(resource, "GET", params, null, null);
      try {
        ProjectModel model = JAXBUtils.unmarshal(resp, ProjectModel.class);

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
    HashMap<String, String> param = new HashMap<String, String>();
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
