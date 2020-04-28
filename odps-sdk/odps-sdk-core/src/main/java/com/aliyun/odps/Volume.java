/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.utils.StringUtils;

import com.aliyun.odps.VolumePartition.VolumePartitionModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * Volume表示ODPS中的volume <b>暂未开放，仅限内部使用<b/>
 *
 * @author lu.lu@alibaba-inc.com
 */
public class Volume extends LazyLoad {

  public static enum Type {
    NEW, OLD
  }

  @Root(name = "Meta", strict = false)
  static class VolumeModel {

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;

    @Element(name = "Type", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String type;

    @Element(name = "Length", required = false)
    Long length;

    @Element(name = "FileNumber", required = false)
    Integer fileNumber;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "Lifecycle", required = false)
    Long lifecycle;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModifiedTime;
  }

  private VolumeModel model;
  private String project;
  private String name;
  private RestClient client;

  Volume(VolumeModel model, String project, RestClient client) {
    this.model = model;
    this.name = model.name;
    this.project = project;
    this.client = client;
  }

  /**
   * 获取 volume 所在{@link Project}名称
   *
   * @return Project名称
   */
  public String getProject() {
    return project;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildVolumeResource(project, this.name);
    Map<String, String> params = new HashMap<String, String>();
    params.put("meta", null);
    Response resp = client.request(resource, "GET", params, null, null);

    try {
      model = SimpleXmlUtils.unmarshal(resp, VolumeModel.class);
      model.lastModifiedTime = DateUtils.parseRfc822Date(resp.getHeader("Last_Modified"));
      model.createdTime = DateUtils.parseRfc822Date(resp.getHeader(Headers.ODPS_CREATION_TIME));
      model.owner = resp.getHeader(Headers.ODPS_OWNER);
    } catch (Exception e) {
      throw new OdpsException("Can't bind xml to " + VolumeModel.class, e);
    }

    setLoaded(true);
  }

  public VolumeFSFile getVolumeFSFile(String path) throws VolumeException {
    return new VolumeFSFile(project, path, client);
  }

  /**
   * 获取volume名
   *
   * @return volume名称
   */
  public String getName() {
    return this.name;
  }


  public Type getType() {
    if (model.type == null) {
      lazyLoad();
    }
    if (StringUtils.isBlank(model.type)) {
      return Type.OLD;
    }
    Type type;
    try {
      type = Type.valueOf(model.type.toUpperCase());
    } catch (Exception e) {
      type = Type.OLD;
    }
    return type;
  }

  /**
   * 获取注释
   *
   * @return volume的相关注释信息
   */
  public String getComment() {
    if (model.comment == null) {
      lazyLoad();
    }
    return model.comment;
  }

  /**
   * 获取volume所属用户
   *
   * @return 所属用户
   */
  public String getOwner() {
    if (model.owner == null) {
      lazyLoad();
    }
    return model.owner;
  }

  /**
   * 获取创建时间
   *
   * @return 创建时间
   */
  public Date getCreatedTime() {
    if (model.createdTime == null) {
      lazyLoad();
    }
    return model.createdTime;
  }

  /**
   * 获取volume最后修改时间
   *
   * @return 最后修改时间
   */
  public Date getLastModifiedTime() {
    if (model.lastModifiedTime == null) {
      lazyLoad();
    }
    return model.lastModifiedTime;
  }


  /**
   * 获取内部存储大小
   *
   * @return 存储大小
   */
  public long getLength() {
    if (model.length == null) {
      lazyLoad();
    }
    return model.length;
  }

  /**
   * 获取生命周期
   *
   * @return 生命周期
   */
  public Long getLifecycle() {
    if (model.lifecycle == null) {
      lazyLoad();
    }

    return model.lifecycle;
  }

  /**
   * 设置生命周期
   *
   * @param  lifecycle 生命周期。 若为0， 表示 disable 生命周期
   */
  public void setLifecycle(long lifecycle) {
    this.model.lifecycle = lifecycle;
  }

  /**
   * 获取内部文件个数
   *
   * @return 文件个数
   */
  public int getFileCount() {
    if (model.fileNumber == null) {
      lazyLoad();
    }
    return model.fileNumber;
  }

  /**
   * 获取指定分区信息
   *
   * @param partitionName
   * @return 分区信息
   */
  public VolumePartition getVolumePartition(String partitionName) {
    VolumePartition.VolumePartitionModel model = new VolumePartition.VolumePartitionModel();
    model.name = partitionName;
    return new VolumePartition(model, project, name, client);
  }

  /**
   * 删除指定分区
   *
   * @param partitionName
   * @throws OdpsException
   */
  public void deleteVolumePartition(String partitionName) throws OdpsException {
    String resource =
        ResourceBuilder.buildVolumePartitionResource(project, this.name, partitionName);
    client.request(resource, "DELETE", null, null, null);
  }

  @Root(name = "Partitions", strict = false)
  private static class Partitions {

    @ElementList(entry = "Partition", inline = true, required = false)
    List<VolumePartitionModel> partitions = new ArrayList<VolumePartitionModel>();
  }

  // for list partition response
  @Element(name = "Volume", required = false)
  private static class ListPartitionsResponse {

    @Element(name = "Partitions", required = false)
    private Partitions partitions;

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;
  }

  /**
   * 获取分区迭代器
   *
   * @return VolumePartition迭代器
   */
  public Iterator<VolumePartition> getPartitionIterator() {
    return getPartitionIterator(null);
  }


  /**
   * 获取volume partition迭代器
   *
   * @param filter 过滤条件
   * @return VolumePartition迭代器
   */
  public Iterator<VolumePartition> getPartitionIterator(final VolumeFilter filter) {
    return new ListIterator<VolumePartition>() {

      Map<String, String> params = new HashMap<String, String>();

      @Override
      protected List<VolumePartition> list() {
        ArrayList<VolumePartition> partitions = new ArrayList<VolumePartition>();
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

        String resource = ResourceBuilder.buildVolumeResource(project, name);
        try {

          ListPartitionsResponse resp =
              client.request(ListPartitionsResponse.class, resource, "GET", params);

          for (VolumePartitionModel model : resp.partitions.partitions) {
            VolumePartition t = new VolumePartition(model, project, name, client);
            partitions.add(t);
          }

          params.put("marker", resp.marker);
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }

        return partitions;
      }
    };
  }

  /**
   * Check whether the path has a volume part
   * 
   * @param path
   * @return
   */
  public static boolean checkPathHasVolume(String path) {
    if (path == null || !path.startsWith("/")) {
      return false;
    }
    String[] strArray = path.split("/");
    if (strArray.length < 2) {
      return false;
    } else {
      return StringUtils.isNotBlank(strArray[1]);
    }
  }
}
