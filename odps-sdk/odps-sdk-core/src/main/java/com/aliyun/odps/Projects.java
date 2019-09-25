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
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import com.aliyun.odps.Project.ProjectModel;
import com.aliyun.odps.account.AccountFormat;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * Projects表示ODPS中所有{@link Project}的集合
 */
public class Projects {

  /**
   * the response of list projects call
   */
  @Root(name = "Projects", strict = false)
  static class ListProjectResponse {

    @ElementList(entry = "Project", inline = true, required = false)
    List<ProjectModel> projects = new LinkedList<ProjectModel>();

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String marker;

    @Element(name = "MaxItems", required = false)
    int maxItems;
  }

  private Odps odps;
  private RestClient client;

  Projects(RestClient client) {
    this.client = client;
  }

  Projects(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
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


  /**
   * 更新 Project
   *
   * @param projectName
   * @param properties
   *     Project 属性。目前该API仅能够设置Project的属性。Project属性列表在每个ODPS版本下会有不同，
   *     建议用户直接忽略此参数（忽略此参数时，会依据系统默认值填充）。如果有明确的特殊需求，可以寻求技术支持。
   *     目前，Project的属性：
   *         a. odps.security.ip.whitelist:能否访问Project的Ip白名单列表；
   *         b. READ_TABLE_MAX_ROW:select语句返回数据的最大行数；
   * @throws OdpsException
   */
  public void updateProject(final String projectName, final Map<String, String> properties)
      throws OdpsException {
    updateProject(projectName, null, null, null, properties, null);
  }

  /**
   * 更新 Project，暂时不直接向用户开放
   *
   * @param projectName
   * @param status
   *    Project状态，包括：AVAILABLE | DELETING | FROZEN。
   *    AVAILABLE 表示Project可以正常工作，AVAILABLE Project可以FROZEN。
   *    FROZEN 表示Project被冻结，冻结后Project拒绝任何API访问。FROZEN Project可以AVAILABLE或者DELETING。
   *    DELETING Project不可以转变为其他状态，表示15天后Project会被系统回收，数据全部清除。
   * @param owner
   *    Project所有者账号
   * @param comment
   * @param properties
   *     Project 属性。目前该API仅能够设置Project的属性。Project属性列表在每个ODPS版本下会有不同，
   *     建议用户直接忽略此参数（忽略此参数时，会依据系统默认值填充）。如果有明确的特殊需求，可以寻求技术支持。
   *     目前，Project的属性：
   *         a. odps.security.ip.whitelist:能否访问Project的Ip白名单列表；
   *         b. READ_TABLE_MAX_ROW:select语句返回数据的最大行数；
   * @param clusters
   *    Project对应的计算集群列表
   * @throws OdpsException
   */
  private void updateProject(final String projectName, final Project.Status status,
                                   final String owner, final String comment,
                                   final Map<String, String> properties,
                                   List<Project.Cluster> clusters)
      throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(projectName);

    String xml =
        marshal(projectName, owner, null, status, comment, properties, clusters);

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put("Content-Type", "application/xml");

    client.stringRequest(resource, "PUT", null, headers, xml);
  }

  /**
   * 获取 Project 列表
   *
   * @param owner
   *
   */
  public Iterator<Project> iterator(String owner) {
    return new ProjectListIterator(owner, null, null);
  }

  /**
   * 获取 Project 列表的 iterable 接口
   */
  public Iterable<Project> iterable(final String owner) {
    return new Iterable<Project>() {
      @Override
      public Iterator<Project> iterator() {
        return new ProjectListIterator(owner, null, null);
      }
    };
  }

  private class ProjectListIterator extends ListIterator<Project> {
    Map<String, String> params = new HashMap<String, String>();

    private String projectOwner;
    private String user;
    private String groupName;

    ProjectListIterator(String projectOwner, String user, String groupName) {
      this.projectOwner = projectOwner;
      this.user = user;
      this.groupName = groupName;
    }

    @Override
    protected List<Project> list() {
      try {
        ArrayList<Project> projects = new ArrayList<Project>();

        String lastMarker = params.get("marker");

        if (params.containsKey("marker") && lastMarker.length() == 0) {
          return null;
        }

        if (projectOwner != null) {
          params.put("owner", projectOwner);
        }

        if (user != null) {
          params.put("user", user);
        }

        if (groupName != null) {
          params.put("group", groupName);
        }

        AccountFormat.setParam(odps.getAccountFormat(), params);

        ListProjectResponse resp = client.request(ListProjectResponse.class,
                                                  ResourceBuilder.buildProjectsResource(),
                                                  "GET", params);

        for (Project.ProjectModel model : resp.projects) {
          Project t = new Project(model, client);
          projects.add(t);
        }

        params.put("marker", resp.marker);

        return projects;
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  private static String marshal(final String projectName, final String projectOwner,
                                final String groupName, final Project.Status status,
                                final String comment, final Map<String, String> properties,
                                List<Project.Cluster> clusters)
      throws OdpsException {
    Project.ProjectModel model = new Project.ProjectModel();

    model.name = projectName;

    // Owner
    if (projectOwner != null) {
      model.owner = projectOwner.trim();
    } else {
      model.owner = null;
    }

    // State
    if (status != null) {
      model.state = status.toString().toUpperCase();
    }

    // Comment
    if (comment != null) {
      model.comment = comment;
    }

    // ProjectGroupName
    model.projectGroupName = groupName;

    // Properties
    if (properties != null) {
      model.properties = new LinkedHashMap<String, String>(properties);
    }

    if (clusters != null) {
      model.clusters = new Project.Clusters();
      model.clusters.entries = clusters;
    }

    String xml = null;
    try {
      xml = SimpleXmlUtils.marshal(model);
    } catch (Exception e) {
      throw new OdpsException("Marshal project model failed", e);
    }
    return xml;
  }
}
