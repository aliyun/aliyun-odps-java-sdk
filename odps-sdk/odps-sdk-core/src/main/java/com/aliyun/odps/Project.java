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
import java.nio.charset.StandardCharsets;
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
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.simpleframework.xml.convert.Converter;
import com.aliyun.odps.simpleframework.xml.stream.InputNode;
import com.aliyun.odps.simpleframework.xml.stream.OutputNode;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
   * 项目类型
   */
  public static enum ProjectType {
    /**
     * 普通 Odps 项目
     */
    managed,
    /**
     * 映射到 Odps 的外部项目，例如 hive
     */
    external
  }

  /**
   * Project model
   */
  @Root(name = "Project", strict = false)
  static class ProjectModel {

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Type", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String type;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "SuperAdministrator", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String superAdministrator;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date creationTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModified;

    @Element(name = "ProjectGroupName", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String projectGroupName;

    @Element(name = "DefaultQuotaNickname", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String defaultQuotaNickname;

    @Element(name = "DefaultQuotaRegion", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String defaultQuotaRegion;

    @Element(name = "Properties", required = false)
    @Convert(PropertyConverter.class)
    LinkedHashMap<String, String> properties;

    @Element(name = "Property", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String propertyJsonString;

    @Element(name = "ExtendedProperties", required = false)
    @Convert(PropertyConverter.class)
    LinkedHashMap<String, String> extendedProperties;

    @Element(name = "State", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String state;

    @Element(name = "DefaultCluster", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String defaultCluster;

    @Element(name = "Clusters", required = false)
    Clusters clusters;

    @Element(name = "Region", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String regionId;
  }

  public static class ExternalProjectProperties {
    private JsonObject rootObj;
    private JsonObject networkObj;

    public ExternalProjectProperties(String source) {
      rootObj = new JsonObject();
      networkObj = new JsonObject();
      rootObj.addProperty("source", source);
      rootObj.add("network", networkObj);
    }

    public void addNetworkProperty(String name, String value) {
      networkObj.addProperty(name, value);
    }

    public void addProperty(String name, String value) {
      rootObj.addProperty(name, value);
    }

    public void addProperty(String name, JsonObject object) { rootObj.add(name, object);}

    public String toJson() {
      Gson gson = new Gson();
      return  gson.toJson(rootObj);
    }
  }

  @Root(name = "Cluster", strict = false)
  public static class Cluster {

    @Root(name = "OptionalQuota", strict = false)
    public static class OptionalQuota {
      // Required by SimpleXML
      OptionalQuota() {
      }

      public OptionalQuota(String quotaID, Map<String, String> properties) {
        this.quotaID = quotaID;
        this.properties = new LinkedHashMap<>(properties);
      }

      @Element(name = "QuotaID", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      String quotaID;

      @Element(name = "Properties", required = false)
      @Convert(PropertyConverter.class)
      LinkedHashMap<String, String> properties;

      public String getQuotaID() {
        return quotaID;
      }

      public Map<String, String> getProperties() {
        return properties;
      }
    }

    // Required by SimpleXML
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

    @ElementList(name = "Quotas", entry = "Quota", required = false)
    List<OptionalQuota> optionalQuotas;

    public String getName() {
      return name;
    }

    public String getQuotaID() {
      return quotaID;
    }

    public List<OptionalQuota> getOptionalQuotas() {
      return optionalQuotas;
    }

    public void setOptionalQuotas(List<OptionalQuota> optionalQuotas) {
      this.optionalQuotas = optionalQuotas;
    }
  }

  @Root(name = "Clusters", strict = false)
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
    public void write(OutputNode outputNode, LinkedHashMap<String, String> properties)
        throws Exception {
      if (properties != null) {
        for (Entry<String, String> entry : properties.entrySet()) {
          String name = entry.getKey();
          String value = entry.getValue();
          SimpleXmlUtils.marshal(new Property(name, value), outputNode);
        }
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

  // For compatibility. The static class 'Cluster' had strict schema validation. Unmarshalling will
  // failed because of the new xml tag 'Quotas'.
  private boolean usedByGroupApi = false;

  Project(ProjectModel model, RestClient client) {
    this.model = model;
    this.client = client;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(model.name);

    Map<String, String> params = null;
    if (usedByGroupApi) {
      params = new HashMap<>();
      params.put("isGroupApi", "true");
    }

    Response resp = client.request(resource, "GET", params, null, null);
    try {
      model = SimpleXmlUtils.unmarshal(resp, ProjectModel.class);
      Map<String, String> headers = resp.getHeaders();
      model.owner = headers.get(Headers.ODPS_OWNER);
      model.creationTime = DateUtils.parseRfc822Date(headers
                                                         .get(Headers.ODPS_CREATION_TIME));
      model.lastModified = DateUtils.parseRfc822Date(headers
                                                         .get(Headers.LAST_MODIFIED));

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
   * 获取Project类型
   *
   * @return Project类型
   */
  public ProjectType getType() throws OdpsException {
    if (model.type == null) {
      lazyLoad();
    }

    if (model.type != null) {
      try {
        return ProjectType.valueOf(model.type.toLowerCase());
      } catch (Exception e) {
        throw new OdpsException("Unknown project type: " + model.type);
      }
    } else {
      // no 'type' returned by Odps Api - we are targeting an old version Odps.
      return ProjectType.managed;
    }
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
   * 获取 project 所属 region
   * @return region id
   */
  public String getRegionId() {
    if (model.regionId == null) {
      lazyLoad();
    }

    return model.regionId;
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

  public String getDefaultQuotaNickname() {
    lazyLoad();
    return model.defaultQuotaNickname;
  }

  public String getDefaultQuotaRegion() {
    lazyLoad();
    return model.defaultQuotaRegion;
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
   * Get default cluster. This is an internal method for group-api.
   *
   * @return Default cluster when called by group owner, otherwise ,null.
   */
  String getDefaultCluster() {
    usedByGroupApi = true;
    lazyLoad();
    return model.defaultCluster;
  }

  /**
   * Get information of clusters owned by this project. This is an internal method for group-api.
   *
   * @return List of {@link Cluster}
   */
  List<Cluster> getClusters() {
    usedByGroupApi = true;
    lazyLoad();
    return clusters == null ? null : clusters.entries;
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
      return getTunnelEndpoint(null);
  }

  public String getTunnelEndpoint(String quotaName) throws OdpsException {
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
    if (quotaName != null && quotaName.length() != 0) {
      params.put("quotaName", quotaName);
    }
    Response resp = client.request(resource, "GET", params, null, null);

    String tunnel;
    if (resp.isOK()) {
      tunnel = new String(resp.getBody());
    } else {
      throw new OdpsException("Can't get tunnel server address: " + resp.getStatus());
    }

    return protocol + "://" + tunnel;
  }

  private String getAutoMvMeta() throws Exception {
    String resource = ResourceBuilder.buildProjectResource(model.name).concat("/automvmeta");
    HashMap<String, String> paras = new HashMap<>();
    Response response = client.request(resource, "GET", paras, null, null);

    String autoMvMeta;
    if (response.isOK()) {
      autoMvMeta = new String(response.getBody());
    } else {
      throw new OdpsException("Can't get autoMvMeta: " + response.getStatus());
    }

    return SimpleXmlUtils.unmarshal(autoMvMeta.getBytes(StandardCharsets.UTF_8), String.class);
  }

  public boolean triggerAutoMvCreation() throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(model.name).concat("/automvcreation");
    HashMap<String, String> paras = new HashMap<>();
    Response response = client.request(resource, "POST", paras, null, null);

    if (response.isOK()) {
      return true;
    } else {
      throw new OdpsException("Can't trigger autoMv: " + response.getStatus());
    }
  }

  public Map<String, String> showAutoMvMeta() {
    Map<String, String> autoMvMeta = new HashMap<>();

    try {
      JsonObject tree = new JsonParser().parse(getAutoMvMeta()).getAsJsonObject();

      if (tree.has("fileSize")) {
        autoMvMeta.put("fileSize", tree.get("fileSize").getAsString());
      }

      if (tree.has("tableNum")) {
        autoMvMeta.put("tableNum", tree.get("tableNum").getAsString());
      }

      if (tree.has("updateTime")) {
        autoMvMeta.put("updateTime", tree.get("updateTime").getAsString());
      }

      if (tree.has("lastAutoMvCreationStartTime")) {
        autoMvMeta.put("lastAutoMvCreationStartTime",
                       tree.get("lastAutoMvCreationStartTime").getAsString());
      }

      if (tree.has("lastAutoMvCreationFinishTime")) {
        autoMvMeta.put("lastAutoMvCreationFinishTime",
                       tree.get("lastAutoMvCreationFinishTime").getAsString());
      }

    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    return autoMvMeta;
  }

}
