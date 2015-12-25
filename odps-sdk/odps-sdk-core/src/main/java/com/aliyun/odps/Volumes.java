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

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.Volume.VolumeModel;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

public class Volumes implements Iterable<Volume> {

  @XmlRootElement(name = "Volumes")
  private static class ListVolumesResponse {

    @XmlElement(name = "Volume")
    private List<VolumeModel> volumes = new ArrayList<VolumeModel>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;
  }

  private RestClient client;

  Volumes(RestClient client) {
    this.client = client;
  }

  /**
   * 获得指定Volume信息
   *
   * @param volumeName
   *     Volume名
   * @return
   */
  public Volume get(String volumeName) {
    return get(getDefaultProjectName(), volumeName);
  }

  /**
   * 获得指定Volume信息
   *
   * @param projectName
   *     所在Project名称
   * @param volumeName
   *     Volume名
   * @return
   */
  public Volume get(String projectName, String volumeName) {
    VolumeModel model = new VolumeModel();
    model.name = volumeName;
    Volume t = new Volume(model, projectName, client);
    return t;
  }

  /**
   * 判断指定Volume是否存在
   *
   * @param volumeName
   *     Volume名
   * @return 存在返回true, 否则返回false
   * @throws OdpsException
   */
  public boolean exists(String volumeName) throws OdpsException {
    return exists(getDefaultProjectName(), volumeName);
  }

  /**
   * 判断指定Volume是否存在
   *
   * @param projectName
   *     所在Project名称
   * @param volumeName
   *     volume名
   * @return 存在返回true, 否则返回flase
   * @throws OdpsException
   */
  public boolean exists(String projectName, String volumeName)
      throws OdpsException {
    try {
      Volume v = get(projectName, volumeName);
      v.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  /**
   * 获取默认Project的所有Volume信息迭代器
   *
   * @return Volume迭代器
   */
  @Override
  public Iterator<Volume> iterator() {
    return iterator(getDefaultProjectName(), null);
  }

  /**
   * 获取Volume信息迭代器
   *
   * @param projectName
   *     指定Project名称
   * @return Volume迭代器
   */
  public Iterator<Volume> iterator(final String projectName) {
    return iterator(projectName, null);
  }

  /**
   * 获取Volume信息迭代器
   *
   * @param filter
   *     过滤条件
   * @return Volume迭代器
   */
  public Iterator<Volume> iterator(final VolumeFilter filter) {
    return iterator(getDefaultProjectName(), filter);
  }

  /**
   * 获取Volume信息迭代器
   *
   * @param projectName
   *     所在Project名称
   * @param filter
   *     过滤条件
   * @return Volume迭代器
   */
  public Iterator<Volume> iterator(final String projectName, final VolumeFilter filter) {
    return new ListIterator<Volume>() {

      Map<String, String> params = new HashMap<String, String>();

      @Override
      protected List<Volume> list() {
        ArrayList<Volume> volumes = new ArrayList<Volume>();
        params.put("expectmarker", "true");

        String lastMarker = params.get("marker");
        if (params.containsKey("marker") && lastMarker.length() == 0) {
          return null;
        }

        if (filter != null) {
          if (filter.getName() != null) {
            params.put("name", filter.getName());
          }
        }

        String resource = ResourceBuilder.buildVolumesResource(projectName);
        try {

          ListVolumesResponse resp = client.request(ListVolumesResponse.class,
                                                    resource, "GET", params);

          for (VolumeModel model : resp.volumes) {
            Volume t = new Volume(model, projectName, client);
            volumes.add(t);
          }

          params.put("marker", resp.marker);
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }

        return volumes;
      }
    };
  }

  /**
   * 创建Volume
   *
   * @param volumeName
   *     volume名
   * @param comment
   * @throws OdpsException
   * @deprecated call #create(String, String, Volume.TYPE) instead.
   */
  @Deprecated
  public void create(String volumeName, String comment)
      throws OdpsException {
    create(client.getDefaultProject(), volumeName, comment);
  }

  /**
   * 创建Volume
   *
   * @param projectName
   *     目标表所在Project名称
   * @param volumeName
   *     所要创建的volume名
   * @param comment
   * @throws OdpsException
   * @deprecated call #create(String, String, String, Volume.TYPE) instead.
   */
  @Deprecated
  public void create(String projectName, String volumeName, String comment
  ) throws OdpsException {
    if (projectName == null || volumeName == null) {
      throw new IllegalArgumentException();
    }
    String resource = ResourceBuilder.buildVolumesResource(projectName);

    Volume.VolumeModel model = new Volume.VolumeModel();
    model.name = volumeName;
    model.comment = comment;

    String xml = null;
    try {
      xml = JAXBUtils.marshal(model, Volume.VolumeModel.class);
    } catch (JAXBException e) {
      throw new OdpsException(e.getMessage(), e);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put("Content-Type", "application/xml");

    client.stringRequest(resource, "POST", null, headers, xml);
  }
  
  /**
   * 创建Volume
   *
   * @param volumeName volume名
   * @param comment
   * @param type 创建原有Volume传入 {@link Volume}.Type.Old,创建新VolumeFS功能的volume传入{@link Volume}
   *        .Type.New。VolumeFS特性需要Project开启该功能才可使用
   * @throws OdpsException
   */
  public void create(String volumeName, String comment, Volume.Type type) throws OdpsException {
    create(client.getDefaultProject(), volumeName, comment, type);
  }

  /**
   * 创建Volume
   *
   * @param projectName 目标表所在Project名称
   * @param volumeName 所要创建的volume名
   * @param comment
   * @param type 创建原有Volume传入 {@link Volume}.Type.Old,创建新VolumeFS功能的volume传入{@link Volume}
   *        .Type.New。VolumeFS特性需要Project开启该功能才可使用
   * @throws OdpsException
   */
  public void create(String projectName, String volumeName, String comment, Volume.Type type) throws OdpsException {
    if (projectName == null || volumeName == null) {
      throw new IllegalArgumentException();
    }
    String resource = ResourceBuilder.buildVolumesResource(projectName);

    Volume.VolumeModel model = new Volume.VolumeModel();
    model.name = volumeName;
    model.comment = comment;
    if(type!=null)
      model.type = type.name().toLowerCase();

    String xml = null;
    try {
      xml = JAXBUtils.marshal(model, Volume.VolumeModel.class);
    } catch (JAXBException e) {
      throw new OdpsException(e.getMessage(), e);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put("Content-Type", "application/xml");

    client.stringRequest(resource, "POST", null, headers, xml);
  }

  /**
   * 删除Volume
   *
   * @param volumeName
   *     表名
   * @throws OdpsException
   */
  public void delete(String volumeName) throws OdpsException {
    delete(client.getDefaultProject(), volumeName);
  }

  /**
   * 删除Volume
   *
   * @param projectName
   *     表所在Project
   * @param volumeName
   *     表名
   * @throws OdpsException
   */
  public void delete(String projectName, String volumeName)
      throws OdpsException {
    if (projectName == null || volumeName == null) {
      throw new IllegalArgumentException();
    }

    String resource = ResourceBuilder.buildVolumeResource(projectName, volumeName);
    client.request(resource, "DELETE", null, null, null);
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
