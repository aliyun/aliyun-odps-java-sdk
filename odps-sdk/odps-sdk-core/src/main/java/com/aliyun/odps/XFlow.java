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
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;

import com.aliyun.odps.Resources.ResourceHeaders;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.ResourceBuilder;

public class XFlow extends LazyLoad {

  private Odps odps;
  private XFlowModel model;
  private String project;

  // XXX 先不会解析 XFlow, 直接利用 xml
  @Root(name = "xflow", strict = false)
  public static class XFlowModel {
    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModifiedTime;

    private String xmlSource;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getXmlSource() {
      return xmlSource;
    }

    public void setXmlSource(String xmlSource) {
      this.xmlSource = xmlSource;
    }
  }

  XFlow(XFlowModel model, String project, Odps odps) {
    this.model = model;
    this.odps = odps;
    this.project = project;
  }


  /**
   * 获取 XFlow 名字
   *
   * @return XFlow 名字
   */
  public String getName() {
    return model.name;
  }

  /**
   * 获取 XFlow 原始 xml
   *
   * @return xml
   */
  public String getSourceXml() {
    if (model.getXmlSource() == null) {
      lazyLoad();
    }
    return model.getXmlSource();
  }

  /**
   * 获取 XFlow 所属用户
   *
   * @return 用户名
   */
  public String getOwner() {
    if (model.owner == null) {
      lazyLoad();
    }
    return model.owner;
  }


  /**
   * 获取 XFlow 创建时间
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
   * 获取 XFLow 最后修改时间
   *
   * @return 最后修改时间
   */
  public Date getLastModifiedTime() {
    if (model.lastModifiedTime == null) {
      lazyLoad();
    }
    return model.lastModifiedTime;
  }

  private static final String CHARSET = "UTF-8";

  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildXFlowResource(project, getName());
    Response response = odps.getRestClient().request(resource, "GET", null, null, null);

    try {
      model.setXmlSource(new String(response.getBody(), CHARSET));
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException(e.getMessage(), e);
    }

    Map<String, String> headers = response.getHeaders();
    model.owner = headers.get(ResourceHeaders.X_ODPS_OWNER);

    try {
      model.createdTime = DateUtils.parseRfc822Date(headers.get("x-odps-creation-time"));
      model.lastModifiedTime = DateUtils.parseRfc822Date(headers.get("Last-Modified"));
    } catch (Exception e) {
      throw new OdpsException("Invalid date format", e);
    }
  }
}
