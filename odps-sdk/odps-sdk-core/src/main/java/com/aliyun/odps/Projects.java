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

import com.aliyun.odps.commons.transport.Headers;
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
   * @throws NoSuchObjectException Project不存在
   */
  public Project get() throws OdpsException {
    return get(getDefaultProjectName());
  }

  /**
   * 获取指定{@link Project}
   *
   * @param projectName {@link Project}名称
   * @return {@link Project}对象
   * @throws NoSuchObjectException Project不存在
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
   * @param projectName Project名称
   * @return Project在ODPS中存在返回true, 否则返回false
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
   * 创建 external 项目
   * @param projectName 项目名称
   * @param comment 项目 comment
   * @param refProjectName 项目引用的 managed 项目名称
   * @param extProperties 映射 external 项目需要的属性，当前支持 hive external 项目
   *           <pre>{@code
   *             Project.ExternalProjectProperties extProps = new Project.ExternalProjectProperties("hive");
   *             // required properties for 'hive' source
   *             extProps.addProperty("hms.ips", "10.0.0.1:5300,10.0.0.2:5300");
   *             extProps.addProperty("hive.database.name", "odps");
   *             extProps.addProperty("hdfs.namenode.ips", "192.168.0.12:3829,192.168.0.5:3389");
   *             // network properties
   *             extProps.addNetworkProperty("odps.external.net.vpc", "false");
   *             // if vpc network is enabled by setting 'odps.external.net.vpc' to 'true', the following three properties must
   *             // also be set:
   *             // extProps.addNetworkProperty("odps.vpc.id", "vpc-ajfiewojlfew");
   *             // extProps.addNetworkProperty("odps.vpc.region", "cn-shanghai");
   *             // extProps.addNetworkProperty("odps.vpc.access.ips", "192.168.0.200-210:8900,192.168.0.10:8399");
   *           }</pre>
   * @throws OdpsException OdpsException
   */
  public void createExternalProject(final String projectName,
                                    final String comment,
                                    final String refProjectName,
                                    final Project.ExternalProjectProperties extProperties)
          throws OdpsException {

    if (extProperties == null || refProjectName == null) {
      throw new OdpsException("External project must specify refProjectName and extProperties");
    }

    String resource = ResourceBuilder.buildProjectsResource();
    HashMap<String, String> properties = new HashMap<>();
    properties.put("external_project_properties", extProperties.toJson());
    properties.put("external_project_ref_project", refProjectName);

    String xml = marshal(projectName, Project.ProjectType.external, null,
                         null, null, comment, properties, null);

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    client.stringRequest(resource, "POST", null, headers, xml);
  }

  /**
   * 删除 external 项目，只允许 project owner 删除。
   * @param projectName 项目名称
   * @throws OdpsException OdpsException
   */
  public void deleteExternalProject(final String projectName)
          throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(projectName);

    HashMap<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");
    client.stringRequest(resource, "DELETE", null, headers, "");
  }

  /**
   * 更新 Project
   *
   * @param properties Project 属性。目前该API仅能够设置Project的属性。Project属性列表在每个ODPS版本下会有不同，
   *                   建议用户直接忽略此参数（忽略此参数时，会依据系统默认值填充）。如果有明确的特殊需求，可以寻求技术支持。 目前，Project的属性： a.
   *                   odps.security.ip.whitelist:能否访问Project的Ip白名单列表； b. READ_TABLE_MAX_ROW:select语句返回数据的最大行数；
   */
  public void updateProject(final String projectName, final Map<String, String> properties)
          throws OdpsException {
    updateProject(projectName, null, null, null, properties, null);
  }

  /**
   * 更新 Project，暂时不直接向用户开放
   *
   * @param status     Project状态，包括：AVAILABLE | DELETING | FROZEN。 AVAILABLE
   *                   表示Project可以正常工作，AVAILABLE Project可以FROZEN。 FROZEN 表示Project被冻结，冻结后Project拒绝任何API访问。FROZEN
   *                   Project可以AVAILABLE或者DELETING。 DELETING Project不可以转变为其他状态，表示15天后Project会被系统回收，数据全部清除。
   * @param owner      Project所有者账号
   * @param properties Project 属性。目前该API仅能够设置Project的属性。Project属性列表在每个ODPS版本下会有不同，
   *                   建议用户直接忽略此参数（忽略此参数时，会依据系统默认值填充）。如果有明确的特殊需求，可以寻求技术支持。 目前，Project的属性： a.
   *                   odps.security.ip.whitelist:能否访问Project的Ip白名单列表； b. READ_TABLE_MAX_ROW:select语句返回数据的最大行数；
   * @param clusters   Project对应的计算集群列表
   */
  private void updateProject(final String projectName, final Project.Status status,
                             final String owner, final String comment,
                             final Map<String, String> properties,
                             List<Project.Cluster> clusters)
          throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(projectName);

    String xml =
            marshal(projectName, null, owner, null, status, comment, properties, clusters);

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    client.stringRequest(resource, "PUT", null, headers, xml);
  }

  public Iterator<Project> iteratorByFilter(ProjectFilter filter) {
      return new ProjectListIterator(filter);
  }

  /**
   * 获取 Project 列表
   */
  public Iterator<Project> iterator(String owner) {
    ProjectFilter filter = new ProjectFilter();
    filter.setOwner(owner);
    return new ProjectListIterator(filter);
  }

  /**
   * 获取 Project 列表的 iterable 接口
   */
  public Iterable<Project> iterable(final String owner) {
    return new Iterable<Project>() {
      @Override
      public Iterator<Project> iterator() {
        ProjectFilter filter = new ProjectFilter();
        filter.setOwner(owner);
        return new ProjectListIterator(filter);
      }
    };
  }

  private class ProjectListIterator extends ListIterator<Project> {
    Map<String, String> params = new HashMap<String, String>();

    private ProjectFilter filter;

    ProjectListIterator(ProjectFilter filter) {
      this.filter = filter;
    }

    @Override
    public List<Project> list(String marker, long maxItems) {
      if (marker != null) {
        params.put("marker", marker);
      }
      if (maxItems >= 0) {
        params.put("maxitems", String.valueOf(maxItems));
      }
      return list();
    }

    @Override
    public String getMarker() {
      return params.get("marker");
    }

    @Override
    protected List<Project> list() {
      try {
        ArrayList<Project> projects = new ArrayList<Project>();

        String lastMarker = params.get("marker");

        if (params.containsKey("marker") && lastMarker.length() == 0) {
          return null;
        }

        if (filter != null) {
          filter.addTo(params);
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

  private static String marshal(final String projectName, final Project.ProjectType projectType,
                                final String projectOwner, final String groupName,
                                final Project.Status status, final String comment,
                                final Map<String, String> properties, List<Project.Cluster> clusters)
          throws OdpsException {
    Project.ProjectModel model = new Project.ProjectModel();

    model.name = projectName;

    // type
    if (projectType != null) {
      model.type = projectType.toString().toLowerCase();
    }

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
