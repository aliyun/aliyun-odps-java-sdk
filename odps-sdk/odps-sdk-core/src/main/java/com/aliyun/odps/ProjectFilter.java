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

/**
 *
 */
package com.aliyun.odps;

import com.aliyun.odps.utils.StringUtils;

import java.util.Map;
import java.util.TreeMap;

/**
 * ProjectFilter用于查询所有项目时根据条件过滤表
 *
 * <p>
 *
 * 例如:<br />
 *
 * <pre>
 * <code>
 * ProjectFilter filter = new ProjectFilter();
 * filter.setOwner("my_project_owner");
 *
 * for (Project t : odps.projects().iterator(filter)) {
 *     // do somthing on the Table object
 * }
 * </code>
 * </pre>
 * </p>
 *
 * @author zhenhong.gzh@alibaba-inc.com
 */
public class ProjectFilter {

  private String owner = null;

  private String user = null;

  private String groupName = null;

  private String tenantId = null;

  private String regionId = null;

  private String quotaNickname = null;

  private String name = null;
  /**
   * 获得表所有者
   *
   * @return 表所有者
   */
  public String getOwner() {
    return owner;
  }

  /**
   * 设置表所有者
   *
   * @param owner
   *     表所有者
   */
  public void setOwner(String owner) {
    this.owner = owner;
  }

  /**
   * 获得表使用者
   *
   * @return 表使用者
   */
  public String getUser() {
    return user;
  }

  /**
   * 设置表使用者
   *
   * @param user
   *     表使用者
   */
  public void setUser(String user) {
    this.user = user;
  }

  /**
   * 设置 group 名称
   *
   * @param groupName
   *    group 名称
   */
  public void setGroup(String groupName) {
    this.groupName = groupName;
  }


  /**
   * 获取 group 名称
   *
   * @return group 名称
   */
  public String getGroup() {
    return groupName;
  }


  /**
   * 获取租户 ID
   * @return 租户 ID
   */
  public String getTenantId() {
    return tenantId;
  }

  /**
   * 设置租户 ID
   * @param tenantId
   */
  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  /**
   * 获取 region id
   * @return region id
   */
  public String getRegionId() {
    return regionId;
  }

  /**
   * 设置 region id
   * @param regionId
   */
  public void setRegionId(String regionId) {
    this.regionId = regionId;
  }

  public String getQuotaNickname() {
    return quotaNickname;
  }

  public void setQuotaNickname(String quotaNickname) {
    this.quotaNickname = quotaNickname;
  }

  /**
   * 获得表前缀
   *
   * @return 表前缀
   */
  public String getName() {
    return name;
  }

  /**
   * 设置表前缀
   *
   * @param name
   *     表前缀
   */
  public void setName(String name) {
    this.name = name;
  }


  /**
   * Put this to @params as key-value. Replace value if key exists and value not empty.
   */
  public void addTo(Map<String, String> params) {
    if (!StringUtils.isNullOrEmpty(owner)) {
      params.put("owner", owner);
    }
    if (!StringUtils.isNullOrEmpty(user)) {
      params.put("user", user);
    }
    if (!StringUtils.isNullOrEmpty(groupName)) {
      params.put("group", groupName);
    }
    if (!StringUtils.isNullOrEmpty(tenantId)) {
      params.put("tenant", tenantId);
    }
    if (!StringUtils.isNullOrEmpty(regionId)) {
      params.put("region", regionId);
    }
    if (!StringUtils.isNullOrEmpty(quotaNickname)) {
      params.put("quotanickname", quotaNickname);
    }
    if (!StringUtils.isNullOrEmpty(name)) {
      params.put("name", name);
    }
  }
}
