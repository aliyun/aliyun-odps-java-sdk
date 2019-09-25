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

package com.aliyun.odps.security;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Attribute;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.LazyLoad;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.utils.StringUtils;

/**
 * 本类用于获取和设置 ODPS Project 的安全相关选项。
 *
 * 目前，ODPS 中存在两种互斥的授权模型：
 * 经典的授权模型由 supportAcl、supportPolicy 和 supportPackage 构成；
 * V2 版的授权模型由 supportAclV2 和 supportPackageV2 构成；
 * 两种授权模型不能同时开启。
 */
public class SecurityConfiguration extends LazyLoad {

  @Root(name = "SecurityConfiguration", strict = false)
  static class SecurityConfigurationModel {

    @Element(name = "CheckPermissionUsingAcl", required = false)
    boolean checkPermissionUsingAcl;

    @Element(name = "CheckPermissionUsingPolicy", required = false)
    boolean checkPermissionUsingPolicy;

    @Element(name = "LabelSecurity", required = false)
    boolean labelSecurity;

    @Element(name = "ObjectCreatorHasAccessPermission", required = false)
    boolean objectCreatorHasAccessPermission;

    @Element(name = "ObjectCreatorHasGrantPermission", required = false)
    boolean objectCreatorHasGrantPermission;

    @Root(name = "ProjectProtection", strict = false)
    static class ProjectProtection {

      @Attribute(name = "Protected", required = false)
      String protectedFlag;

      @Element(name = "Exceptions", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      String exceptionPolicy;
    }

    @Element(name = "ProjectProtection", required = false)
    ProjectProtection projectProtection;

    @Element(name = "CheckPermissionUsingAclV2", required = false)
    boolean checkPermissionUsingAclV2;

    @Element(name = "CheckPermissionUsingPackageV2", required = false)
    boolean checkPermissionUsingPackageV2;

    @Element(name = "SupportACL", required = false)
    boolean supportAcl;

    @Element(name = "SupportPolicy", required = false)
    boolean supportPolicy;

    @Element(name = "SupportPackage", required = false)
    boolean supportPackage;

    @Element(name = "SupportACLV2", required = false)
    boolean supportAclV2;

    @Element(name = "SupportPackageV2", required = false)
    boolean supportPackageV2;

    @Element(name = "CheckPermissionUsingPackage", required = false)
    boolean checkPermissionUsingPackage;

    @Element(name = "CreatePackage", required = false)
    boolean createPackage;

    @Element(name = "CreatePackageV2", required = false)
    boolean createPackageV2;

    @Element(name = "AuthorizationVersion", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String authorizationVersion;
  }

  private SecurityConfigurationModel model;
  private String project;
  private RestClient client;
  /**
   * If strip is true, 'Exception Policy' will not be returned
   */
  private boolean strip;

  SecurityConfiguration(String project, RestClient client) {
    this(project, client, false);
  }

  SecurityConfiguration(String project, RestClient client, boolean strip) {
    this.model = null;
    this.project = project;
    this.client = client;
    this.strip = strip;
    lazyLoad();
  }

  @Override
  public void reload() throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project);
    Map<String, String> params = new HashMap<String, String>();
    if (!strip) {
      params.put("security_configuration", null);
    } else {
      params.put("security_configuration_without_exception_policy", null);
    }
    model = client.request(SecurityConfigurationModel.class,
                           resource.toString(), "GET", params, null, null);
    setLoaded(true);
  }

  void update(String supervisionToken) throws OdpsException {
    try {
      StringBuilder resource = new StringBuilder();
      resource.append("/projects/").append(project);
      Map<String, String> params = new HashMap<String, String>();
      params.put("security_configuration", null);
      String xmlSecurityConfiguration = SimpleXmlUtils.marshal(model);
      HashMap<String, String> headers = null;
      if (supervisionToken != null) {
        headers = new HashMap<String, String>();
        headers.put("odps-x-supervision-token", supervisionToken);
      }
      client.stringRequest(resource.toString(), "PUT", params, headers,
                           xmlSecurityConfiguration);
    } catch (OdpsException e) {
      throw e;
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

  }

  public boolean checkPermissionUsingAcl() {
    return model.checkPermissionUsingAcl;
  }

  public void enableCheckPermissionUsingAcl() {
    model.checkPermissionUsingAcl = true;
  }

  public void disableCheckPermissionUsingAcl() {
    model.checkPermissionUsingAcl = false;
  }

  public boolean checkPermissionUsingPolicy() {
    return model.checkPermissionUsingPolicy;
  }

  public void enableCheckPermissionUsingPolicy() {
    model.checkPermissionUsingPolicy = true;
  }

  public void disableCheckPermissionUsingPolicy() {
    model.checkPermissionUsingPolicy = false;
  }

  public boolean labelSecurity() {
    return model.labelSecurity;
  }

  public void enableLabelSecurity() {
    model.labelSecurity = true;
  }

  public void disableLabelSecurity() {
    model.labelSecurity = false;
  }

  public boolean objectCreatorHasAccessPermission() {
    return model.objectCreatorHasAccessPermission;
  }

  public void enableObjectCreatorHasAccessPermission() {
    model.objectCreatorHasAccessPermission = true;
  }

  public void disableObjectCreatorHasAccessPermission() {
    model.objectCreatorHasAccessPermission = false;
  }

  public boolean objectCreatorHasGrantPermission() {
    return model.objectCreatorHasGrantPermission;
  }

  public void enableObjectCreatorHasGrantPermission() {
    model.objectCreatorHasGrantPermission = true;
  }

  public void disableObjectCreatorHasGrantPermission() {
    model.objectCreatorHasGrantPermission = false;
  }

  public boolean projectProtection() {
    return model.projectProtection.protectedFlag.equals("true");
  }

  public void enableProjectProtection() {
    model.projectProtection.protectedFlag = "true";
    model.projectProtection.exceptionPolicy = null;
  }

  public void enableProjectProtection(String exceptionPolicy) {
    model.projectProtection.protectedFlag = "true";
    model.projectProtection.exceptionPolicy = exceptionPolicy;
  }

  public void disableProjectProtection() {
    model.projectProtection.protectedFlag = "false";
    model.projectProtection.exceptionPolicy = null;
  }

  public String getProjectProtectionExceptionPolicy() throws OdpsException {
    if ((!projectProtection())
        || StringUtils.isNullOrEmpty(model.projectProtection.exceptionPolicy)) {
      throw new NoSuchObjectException("Exception Policy dose not exist!");
    }
    return model.projectProtection.exceptionPolicy;
  }

  public boolean checkPermissionUsingAclV2() {
    return model.checkPermissionUsingAclV2;
  }

  public boolean checkPermissionUsingPackageV2() {
    return model.checkPermissionUsingPackageV2;
  }

  public boolean supportAcl() {
    return model.supportAcl;
  }

  public boolean supportPolicy() {
    return model.supportPolicy;
  }

  public boolean supportPackage() {
    return model.supportPackage;
  }

  public boolean supportAclV2() {
    return model.supportAclV2;
  }

  public boolean supportPackageV2() {
    return model.supportPackageV2;
  }

  public boolean checkPermissionUsingPackage() {
    return model.checkPermissionUsingPackage;
  }

  public boolean createPackage() {
    return model.createPackage;
  }

  public boolean createPackageV2() {
    return model.createPackageV2;
  }

  public String getAuthorizationVersion() {
    return model.authorizationVersion;
  }

}
