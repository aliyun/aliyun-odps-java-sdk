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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.commons.util.OSUtils;
import com.aliyun.odps.commons.util.SvnRevisionUtils;
import com.aliyun.odps.task.*;


/**
 * ODPS的Task定义
 *
 * @author shenggong.wang@alibaba-inc.com
 */
@XmlType(propOrder = {"name", "comment", "properties"})
@XmlSeeAlso(value = {
    GraphTask.class,
    LOTTask.class,
    SqlPlanTask.class,
    SQLTask.class,
    SQLRTTask.class,
    XLibTask.class,
    SQLCostTask.class,
    StreamTask.class,
    MoyeTask.class,
    GalaxyTask.class,
    CupidTask.class,
    MergeTask.class,
    AlgoTask.class
})
public abstract class Task {
  private static final Map<String, String> DEFAULT_SETTINGS = new HashMap<String, String>();
  static {
    DEFAULT_SETTINGS.put(
        "odps.idata.userenv",
        "JavaSDK Revision:" + SvnRevisionUtils.getSvnRevision() +
        ",Version:" + SvnRevisionUtils.getMavenVersion() +
        ",JavaVersion:" + SvnRevisionUtils.getJavaVersion() +
        ",IP:" + OSUtils.getIpAddress() +
        ",MAC:" + OSUtils.getMacAddress());
  }

  /**
   * Task property
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"name", "value"})
  public static class Property {

    @XmlElement(name = "Name", required = true)
    private String name;

    @XmlElement(name = "Value", required = true)
    private String value;

    Property() {
    }

    /**
     * 构造一个新的{@link Property}实例。
     *
     * @param name
     *     属性名，不能为null。
     * @param value
     *     属性值。
     */
    public Property(String name, String value) {
      setName(name);
      setValue(value);
    }

    /**
     * 返回属性名。
     *
     * @return 属性名。
     */
    public String getName() {
      return name;
    }

    /**
     * 设置属性值。
     *
     * @param name
     *     属性名。不能为null。
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * 返回属性值。
     *
     * @return 属性值。
     */
    public String getValue() {
      return value;
    }

    /**
     * 设置属性值。
     *
     * @param value
     *     属性值。
     */
    public void setValue(String value) {
      this.value = value;
    }
  }

  /**
   * Task properties
   */
  @XmlRootElement(name = "Config")
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class Properties {

    @XmlElement(name = "Property")
    Set<Property> properties = new LinkedHashSet<Property>();

    public void addProperty(Property property) {
      properties.add(property);
    }

    public void removeProperty(Property property) {
      properties.remove(property);
    }
  }

  private String name;

  private String comment;

  @XmlElement(name = "Config")
  @XmlJavaTypeAdapter(XmlPropertyMapAdapter.class)
  private LinkedHashMap<String, String> properties = new LinkedHashMap<String, String>();

  /**
   * 获取{@link Task}的名称
   *
   * @return Task名称
   */
  public String getName() {
    return name;
  }

  /**
   * 设置{@link Task}的名称
   *
   * @param name
   *     Task名称
   */
  @XmlElement(name = "Name")
  public void setName(String name) {
    this.name = name;
  }

  /**
   * 获取注释信息
   *
   * @return 注释信息
   */
  public String getComment() {
    return comment;
  }

  /**
   * 设置注释信息
   *
   * @param comment
   *     注释信息
   */
  @XmlElement(name = "Comment")
  public void setComment(String comment) {
    this.comment = comment;
  }

  /**
   * 获取所有配置信息
   *
   * @return 配置信息
   */
  public final Map<String, String> getProperties() {
    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
    map.putAll(properties);
    return map;
  }

  /**
   * 设置所有配置信息
   *
   * @param properties
   *     配置项名称
   */
  @XmlTransient
  public final void setProperties(Map<String, String> properties) {
    this.properties = new LinkedHashMap<String, String>();
    this.properties.putAll(properties);
  }

  /**
   * 设置配置
   *
   * @param name
   *     配置项名称
   * @param value
   *     配置项的值
   */
  public void setProperty(String name, String value) {
    properties.put(name, value);
  }

  /**
   * 获取 Task 命令信息
   *
   * @return Task 命令信息
   */
  public String getCommandText() {
    if (getProperties().containsKey("commandText")) {
      return getProperties().get("commandText");
    } else {
      return "";
    }
  }

  void loadDefaultSettings() {
    JsonObject settings;
    if (properties.containsKey("settings")) {
      JsonParser parser = new JsonParser();
      settings = parser.parse(properties.get("settings")).getAsJsonObject();
    } else {
      settings = new JsonObject();
    }
    for (Entry<String, String> setting : DEFAULT_SETTINGS.entrySet()) {
      settings.addProperty(setting.getKey(), setting.getValue());
    }

    properties.put("settings", settings.toString());
  }
}
