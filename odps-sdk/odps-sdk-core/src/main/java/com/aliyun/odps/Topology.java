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

import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * Topologies表示ODPS中所有Topology的集合
 *
 * @author edward.wang@alibaba-inc.com
 */
public class Topology extends LazyLoad {

  private String project;
  private TopologyModel model;
  private RestClient client;

  public Topology(TopologyModel model, String projectName, RestClient client) {
    this.project = projectName;
    this.model = model;
    this.client = client;
  }

  /**
   * Topology model
   */
  @XmlRootElement(name = "Topology")
  static class TopologyModel {

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "Owner")
    String owner;

    @XmlElement(name = "Comment")
    String comment;

    @XmlElement(name = "CreationTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date createdTime;

    @XmlElement(name = "LastModifiedTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date lastModifiedTime;

    @XmlElement(name = "Definition")
    String definition;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildTopologyResource(project, model.name);
    Response resp = client.request(resource, "GET", null, null, null);
    try {
      model = JAXBUtils.unmarshal(resp, TopologyModel.class);
    } catch (Exception e) {
      throw new OdpsException("Can't bind xml to " + TopologyModel.class, e);
    }
    try {
      model.lastModifiedTime = DateUtils.parseRfc822Date(resp.getHeader("Last_Modified"));
      model.createdTime = DateUtils.parseRfc822Date(resp.getHeader("x-odps-creation-time"));
      model.owner = resp.getHeader("x-odps-owner");
    } catch (Exception e) {
      // MAY NOT EXIST
    }
    setLoaded(true);
  }

  /**
   * 获取Topology名
   *
   * @return Topology名称
   */
  public String getName() {
    return model.name;
  }

  /**
   * 获取注释
   *
   * @return Topology的相关注释信息
   */
  public String getComment() {
    if (model.comment == null) {
      lazyLoad();
    }
    return model.comment;
  }

  /**
   * 获取Topology所属用户
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
   * 获取 Topology 的JSON描述文件
   *
   * @return JSON描述
   */
  public String getDescription() {
    if (model.definition == null) {
      lazyLoad();
    }
    return model.definition;
  }
}
