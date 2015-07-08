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

import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.Project.ProjectModel;
import com.aliyun.odps.rest.RestClient;

/**
 * Projects表示ODPS中所有{@link Project}的集合
 */
public class Projects {

  /**
   * the response of list projects call
   */
  @XmlRootElement(name = "Projects")
  private static class ListProjectResponse {

    @XmlElement(name = "Project")
    private List<ProjectModel> projects = new LinkedList<ProjectModel>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private int maxItems;
  }

  private RestClient client;

  Projects(RestClient client) {
    this.client = client;
  }

  /**
   * 获得默认{@link Project}对象
   *
   * @return {@link Project}
   * @throws NoSuchObjectException
   *     Project不存在
   * @throws OdpsException
   */
  public Project get() throws OdpsException {
    return get(getDefaultProjectName());
  }

  /**
   * 获取指定{@link Project}
   *
   * @param projectName
   *     {@link Project}名称
   * @return {@link Project}对象
   * @throws NoSuchObjectException
   *     Project不存在
   * @throws OdpsException
   */
  public Project get(String projectName) throws OdpsException {
    ProjectModel model = new ProjectModel();
    model.name = projectName;
    Project prj = new Project(model, client);

    return prj;
  }

  /**
   * 检查{@link Project}是否存在
   *
   * @param projectName
   *     Project名称
   * @return Project在ODPS中存在返回true, 否则返回false
   * @throws OdpsException
   */
  public boolean exists(String projectName) throws OdpsException {
    try {
      Project project = get(projectName);
      project.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }
}
