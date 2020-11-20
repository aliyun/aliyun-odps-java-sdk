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


}
