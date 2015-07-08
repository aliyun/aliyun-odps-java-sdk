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

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.LazyLoad;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.RestClient;

public class SecurityConfiguration extends LazyLoad {

  @XmlRootElement(name = "SecurityConfiguration")
  static class SecurityConfigurationModel {

    @XmlElement(name = "CheckPermissionUsingAcl")
    boolean checkPermissionUsingAcl;

    @XmlElement(name = "CheckPermissionUsingPolicy")
    boolean checkPermissionUsingPolicy;

    @XmlElement(name = "LabelSecurity")
    boolean labelSecurity;

    @XmlElement(name = "ObjectCreatorHasAccessPermission")
    boolean objectCreatorHasAccessPermission;

    @XmlElement(name = "ObjectCreatorHasGrantPermission")
    boolean objectCreatorHasGrantPermission;

    @XmlRootElement(name = "ProjectProtection")
    static class ProjectProtection {

      @XmlAttribute(name = "Protected")
      String protectedFlag;

      @XmlElement(name = "Exceptions")
      String exceptionPolicy;
    }

    @XmlElement(name = "ProjectProtection")
    ProjectProtection projectProtection;
  }

  private SecurityConfigurationModel model;
  private String project;
  private RestClient client;

  SecurityConfiguration(String project, RestClient client) {
    this.model = null;
    this.project = project;
    this.client = client;
    lazyLoad();
  }

  @Override
  public void reload() throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project);
    Map<String, String> params = new HashMap<String, String>();
    params.put("security_configuration", null);
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
      String xmlSecurityConfiguration = JAXBUtils.marshal(model,
                                                          SecurityConfigurationModel.class);
      HashMap<String, String> headers = null;
      if (supervisionToken != null) {
        headers = new HashMap<String, String>();
        headers.put("odps-x-supervision-token", supervisionToken);
      }
      client.stringRequest(resource.toString(), "PUT", params, headers,
                           xmlSecurityConfiguration);
    } catch (OdpsException e) {
      throw e;
    } catch (JAXBException e) {
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
        || (model.projectProtection.exceptionPolicy == null)
        || (model.projectProtection.equals(""))) {
      throw new NoSuchObjectException("Exception Policy dose not exist!");
    }
    return model.projectProtection.exceptionPolicy;
  }
}
