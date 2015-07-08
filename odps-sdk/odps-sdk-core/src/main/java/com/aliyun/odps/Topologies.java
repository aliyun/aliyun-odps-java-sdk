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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.Topology.TopologyModel;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.task.StreamTask;

/**
 * Topologies表示ODPS中所有Topology的集合
 *
 * @author edward.wang@alibaba-inc.com
 */
public class Topologies implements Iterable<Topology> {

  @XmlRootElement(name = "Topologies")
  private static class ListTopologiesResponse {

    @XmlElement(name = "Topology")
    private List<TopologyModel> topologies = new ArrayList<TopologyModel>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;
  }

  RestClient client;
  Odps odps;

  public Topologies(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * 获得指定Topology信息
   *
   * @param topologyName
   *     Topology名
   * @return
   */
  public Topology get(String topologyName) {
    return get(getDefaultProjectName(), topologyName);
  }

  /**
   * 获得指定Topology信息
   *
   * @param projectName
   *     所在Project名称
   * @param topologyName
   *     Topology名
   * @return
   */
  public Topology get(String projectName, String topologyName) {
    TopologyModel model = new TopologyModel();
    model.name = topologyName;
    Topology t = new Topology(model, projectName, client);
    return t;
  }

  /**
   * 创建Topology
   *
   * @param topologyName
   *     所要创建的Topology名
   * @param description
   *     Topology的JSON描述
   * @throws OdpsException
   */
  public void create(String topologyName, String description) throws OdpsException {
    create(getDefaultProjectName(), topologyName, description);
  }

  /**
   * 创建Topology
   *
   * @param projectName
   *     Topology所在Project名称
   * @param topologyName
   *     所要创建的Topology名
   * @param description
   *     Topology的JSON描述
   * @throws OdpsException
   */
  public void create(String projectName, String topologyName, String description)
      throws OdpsException {
    if (projectName == null || topologyName == null) {
      throw new IllegalArgumentException();
    }
    StreamTask task = new StreamTask();
    task.setName("CREATE_TOPOLOGY_TASK");
    task.setProperty("operation", "CREATE_TOPOLOGY");
    task.setProperty("topology", topologyName);
    task.setProperty("description", description);

    odps.instances().create(projectName, task).waitForSuccess();
  }

  /**
   * 删除Topology
   *
   * @param topologyName
   *     Topology名
   * @throws OdpsException
   */
  public void delete(String topologyName) throws OdpsException {
    delete(client.getDefaultProject(), topologyName);
  }

  /**
   * 删除Topology
   *
   * @param projectName
   *     Topology所在Project
   * @param topologyName
   *     Topology名
   * @throws OdpsException
   */
  public void delete(String projectName, String topologyName) throws OdpsException {
    StreamTask task = new StreamTask();
    task.setName("DELETE_TOPOLOGY_TASK");
    task.setProperty("operation", "DELETE_TOPOLOGY");
    task.setProperty("topology", topologyName);
    odps.instances().create(projectName, task).waitForSuccess();
  }

  /**
   * 获取默认Project的所有Topology信息迭代器
   *
   * @return Topology迭代器
   */
  @Override
  public Iterator<Topology> iterator() {
    return iterator(getDefaultProjectName());
  }

  /**
   * 获取Topology信息迭代器
   *
   * @param projectName
   *     指定Project名称
   * @return Topology迭代器
   */
  public Iterator<Topology> iterator(final String projectName) {
    return new ListIterator<Topology>() {

      Map<String, String> params = new HashMap<String, String>();

      @Override
      protected List<Topology> list() {
        ArrayList<Topology> topologies = new ArrayList<Topology>();
        params.put("expectmarker", "true");

        String lastMarker = params.get("marker");
        if (params.containsKey("marker") && (lastMarker == null || lastMarker.length() == 0)) {
          return null;
        }

        String resource = ResourceBuilder.buildTopologiesResource(projectName);
        try {

          ListTopologiesResponse
              resp =
              client.request(ListTopologiesResponse.class, resource, "GET", params);

          for (TopologyModel model : resp.topologies) {
            Topology t = new Topology(model, projectName, client);
            topologies.add(t);
          }

          params.put("marker", resp.marker);
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }

        return topologies;
      }
    };
  }

  /* private */
  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }
}
