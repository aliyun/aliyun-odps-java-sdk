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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.security.CheckPermissionConstants.*;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.security.Role.RoleModel;
import com.aliyun.odps.security.User.UserModel;

/**
 * ODPS安全管理类
 *
 * @author mengzhe.yangmz@aliyun-inc.com
 */
public class SecurityManager {

  private String project;
  private RestClient client;
  private SecurityConfiguration securityConfigration;

  @XmlRootElement(name = "Authorization")
  private static class AuthorizationQueryRequest {

    @XmlElement(name = "Query")
    private String query;

    @XmlElement(name = "ResponseInJsonFormat")
    private boolean responseInJsonFormat;

    @SuppressWarnings("unused")
    public AuthorizationQueryRequest() {
      this.query = "";
      this.responseInJsonFormat = true;
    }

    public AuthorizationQueryRequest(String query, boolean jsonFormat) {
      this.query = query;
      this.responseInJsonFormat = jsonFormat;
    }

  }

  @XmlRootElement(name = "Authorization")
  private static class AuthorizationQueryResponse {

    @XmlElement(name = "Result")
    private String result;

    public String getResult() {
      return result;
    }
  }

  @XmlRootElement(name = "Users")
  private static class ListUsersResponse {

    @XmlElement(name = "User")
    private List<UserModel> users = new ArrayList<UserModel>();
  }

  @XmlRootElement(name = "Roles")
  private static class ListRolesResponse {

    @XmlElement(name = "Role")
    private List<RoleModel> roles = new ArrayList<RoleModel>();
  }

  @XmlRootElement(name = "Auth")

  private static class CheckPermissionResponse {

    @XmlElement(name = "Result")
    private String result;

    public String getResult() {return result;}
  }

  public SecurityManager(String project, RestClient client) {
    this.project = project;
    this.client = client;
    this.securityConfigration = null;
  }

  public SecurityConfiguration getSecurityConfiguration() {
    if (this.securityConfigration == null) {
      this.securityConfigration = new SecurityConfiguration(project, client);
    }
    return this.securityConfigration;
  }

  public void setSecurityConfiguration(
      SecurityConfiguration securityConfigration) throws OdpsException {
    securityConfigration.update(null);
  }

  public void setSecurityConfiguration(
      SecurityConfiguration securityConfigration, String supervisionToken)
      throws OdpsException {
    securityConfigration.update(supervisionToken);
  }

  public String getProjectPolicy() throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    Response response = client.request(resource.toString(), "GET", params, null,
                                       null);
    try {
      return new String(response.getBody(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException("Can't parse response!", e);
    }
  }

  public void putProjectPolicy(String policy) throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    client.stringRequest(resource.toString(), "PUT", params, null, policy);
  }

  public String getRolePolicy(String roleName) throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project).append("/roles/")
        .append(roleName);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    Response response = client.request(resource.toString(), "GET", params, null,
                                       null);
    try {
      return new String(response.getBody(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException("Can't parse response!", e);
    }
  }

  public void putRolePolicy(String roleName, String policy)
      throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project).append("/roles/")
        .append(roleName);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    client.stringRequest(resource.toString(), "PUT", params, null, policy);
  }

  public List<User> listUsers() throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project).append("/users");
    ListUsersResponse resp = client.request(ListUsersResponse.class,
                                            resource.toString(), "GET");
    List<User> users = new ArrayList<User>();
    for (UserModel model : resp.users) {
      User t = new User(model, project, client);
      users.add(t);
    }
    return users;
  }

  public List<Role> listRoles() throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project).append("/roles");
    ListRolesResponse resp = client.request(ListRolesResponse.class,
                                            resource.toString(), "GET");
    List<Role> roles = new ArrayList<Role>();
    for (RoleModel model : resp.roles) {
      Role t = new Role(model, project, client);
      roles.add(t);
    }
    return roles;
  }

  public List<Role> listRolesForUser(String uid) throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project).append("/users/").append(uid);
    Map<String, String> params = new HashMap<String, String>();
    params.put("roles", null);
    ListRolesResponse resp = client.request(ListRolesResponse.class,
                                            resource.toString(), "GET", params, null, null);
    List<Role> roles = new ArrayList<Role>();
    for (RoleModel model : resp.roles) {
      Role t = new Role(model, project, client);
      roles.add(t);
    }
    return roles;
  }

  public List<User> listUsersForRole(String roleName) throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project).append("/roles/")
        .append(roleName);
    Map<String, String> params = new HashMap<String, String>();
    params.put("users", null);
    ListUsersResponse resp = client.request(ListUsersResponse.class,
                                            resource.toString(), "GET", params, null, null);
    List<User> users = new ArrayList<User>();
    for (UserModel model : resp.users) {
      User t = new User(model, project, client);
      users.add(t);
    }
    return users;
  }

  public CheckPermissionResult checkPermission(ObjectType type, String objectName,
                                               ActionType action) throws OdpsException {
    return checkPermission(type, objectName, action, project);
  }

  public CheckPermissionResult checkPermission(ObjectType type, String objectName,
                                               ActionType action, String projectName) throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(projectName).append("/auth/");

    Map<String, String> params = new HashMap<String, String>();
    params.put("type", type.toString());
    params.put("name", objectName);
    params.put("grantee", action.toString());
    CheckPermissionResponse response = client.request(CheckPermissionResponse.class,
                                                      resource.toString(), "GET", params, null, null);
    System.out.println(response.getResult());
    return response.getResult().toUpperCase().equals("ALLOW")?
           CheckPermissionResult.Allow : CheckPermissionResult.Deny;
  }

  public String runQuery(String query, Boolean jsonOutput) throws OdpsException {
    return runQuery(query, jsonOutput, null);
  }

  public String runQuery(String query, Boolean jsonOutput,
                         String supervisionToken) throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(project).append("/authorization");
    AuthorizationQueryRequest request = new AuthorizationQueryRequest(query,
                                                                      jsonOutput);
    String xmlRequest;
    try {
      xmlRequest = JAXBUtils.marshal(request, AuthorizationQueryRequest.class);
    } catch (JAXBException e) {
      throw new OdpsException(e.getMessage(), e);
    }
    HashMap<String, String> headers = new HashMap<String, String>();
    if (supervisionToken != null) {
      headers.put("odps-x-supervision-token", supervisionToken);
    }
    headers.put(Headers.CONTENT_TYPE, "application/xml");
    AuthorizationQueryResponse response = client.stringRequest(
        AuthorizationQueryResponse.class, resource.toString(), "POST", null,
        headers, xmlRequest);
    return response.getResult();
  }

  public String generateAuthorizationToken(String policy, String type)
      throws OdpsException {
    if (type.equalsIgnoreCase("Bearer")) {
      StringBuilder resource = new StringBuilder();
      resource.append("/projects/").append(project)
          .append("/authorization");
      HashMap<String, String> headers = new HashMap<String, String>();
      headers.put(Headers.CONTENT_TYPE, "application/json");
      Map<String, String> params = new HashMap<String, String>();
      params.put("sign_bearer_token", null);
      AuthorizationQueryResponse response = client.stringRequest(
          AuthorizationQueryResponse.class, resource.toString(), "POST", params,
          headers, policy);
      return response.getResult();
    } else {
      // 目前只支持bearer类型的token
      throw new OdpsException("Unsupport token type" + type);
    }

  }

}
