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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.VolumeFile.VolumeFileModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * VolumePartition表示ODPS中的volume partition
 * <b>暂未开放，仅限内部使用<b/>
 *
 * @author lu.lu@alibaba-inc.com
 */
public class VolumePartition extends LazyLoad {

  @XmlRootElement(name = "Meta")
  static class VolumePartitionModel {

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "Comment")
    String comment;

    @XmlElement(name = "Length")
    Long length;

    @XmlElement(name = "FileNumber")
    Integer fileNumber;

    @XmlElement(name = "Owner")
    String owner;

    @XmlElement(name = "CreationTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date createdTime;

    @XmlElement(name = "LastModifiedTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date lastModifiedTime;
  }

  private VolumePartitionModel model;
  private String project;
  private String volume;
  private String name;

  private RestClient client;

  VolumePartition(VolumePartitionModel model, String project, String volume, RestClient client) {
    this.model = model;
    this.project = project;
    this.volume = volume;
    this.name = model.name;
    this.client = client;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildVolumePartitionResource(project, this.volume, this.name);
    Map<String, String> params = new HashMap<String, String>();
    params.put("meta", null);
    Response resp = client.request(resource, "GET", params, null, null);

    try {
      model = JAXBUtils.unmarshal(resp, VolumePartitionModel.class);
      model.lastModifiedTime = DateUtils.parseRfc822Date(resp.getHeader("Last_Modified"));
      model.createdTime = DateUtils.parseRfc822Date(resp.getHeader(Headers.ODPS_CREATION_TIME));
      model.owner = resp.getHeader(Headers.ODPS_OWNER);
    } catch (Exception e) {
      throw new OdpsException("Can't bind xml to " + VolumePartitionModel.class, e);
    }

    setLoaded(true);
  }

  /**
   * 获取volume partition名
   *
   * @return volume partition名称
   */
  public String getName() {
    return this.name;
  }

  /**
   * 获取注释
   *
   * @return volume partition的相关注释信息
   */
  public String getComment() {
    if (model.comment == null) {
      lazyLoad();
    }
    return model.comment;
  }

  /**
   * 获取volume partition所属用户
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
   * 获取volume partition最后修改时间
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

  // for list file response
  @XmlRootElement(name = "Items")
  private static class ListFilesResponse {

    @XmlElement(name = "Item")
    private List<VolumeFileModel> items = new ArrayList<VolumeFileModel>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;
  }

  /**
   * 获取文件迭代器
   *
   * @return 文件迭代器
   */
  public Iterator<VolumeFile> getFileIterator() {
    return getFileIterator(null);
  }

  /**
   * 获取文件迭代器
   *
   * @param path
   *     文件路径
   * @return 文件迭代器
   */
  public Iterator<VolumeFile> getFileIterator(String path) {
    return new PathListIterator(path);
  }

  class PathListIterator extends ListIterator<VolumeFile> {

    String path;
    Map<String, String> params = new HashMap<String, String>();

    PathListIterator(String path) {
      this.path = path;
    }

    @Override
    protected List<VolumeFile> list() {
      ArrayList<VolumeFile> files = new ArrayList<VolumeFile>();
      params.put("expectmarker", "true");

      String lastMarker = params.get("marker");
      if (params.containsKey("marker") && lastMarker.length() == 0) {
        return null;
      }
      if (this.path != null) {
        params.put("path", this.path);
      }
      String resource = ResourceBuilder.buildVolumePartitionResource(project, volume, name);
      try {
        ListFilesResponse resp = client.request(ListFilesResponse.class,
                                                resource, "GET", params);

        for (VolumeFileModel item : resp.items) {
          VolumeFile t = new VolumeFile(item);
          files.add(t);
        }

        params.put("marker", resp.marker);
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      return files;
    }
  }
};
