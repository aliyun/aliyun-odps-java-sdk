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

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.security.CheckPermissionConstants.ActionType;
import com.aliyun.odps.security.CheckPermissionConstants.CheckPermissionResult;
import com.aliyun.odps.security.CheckPermissionConstants.ObjectType;
import com.aliyun.odps.security.Role.RoleModel;
import com.aliyun.odps.security.User.UserModel;
import com.aliyun.odps.utils.StringUtils;

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

    public String getResult() {
      return result;
    }
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
    String resource = ResourceBuilder.buildProjectResource(project);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    Response response = client.request(resource, "GET", params, null,
                                       null);
    try {
      return new String(response.getBody(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException("Can't parse response!", e);
    }
  }

  public void putProjectPolicy(String policy) throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(project);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    client.stringRequest(resource, "PUT", params, null, policy);
  }

  public String getRolePolicy(String roleName) throws OdpsException {
    String resource = ResourceBuilder.buildRoleResource(project, roleName);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    Response response = client.request(resource, "GET", params, null,
                                       null);
    try {
      return new String(response.getBody(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException("Can't parse response!", e);
    }
  }

  public void putRolePolicy(String roleName, String policy)
      throws OdpsException {
    String resource = ResourceBuilder.buildRoleResource(project, roleName);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    client.stringRequest(resource, "PUT", params, null, policy);
  }

  public List<User> listUsers() throws OdpsException {
    String resource = ResourceBuilder.buildUsersResource(project);
    ListUsersResponse resp = client.request(ListUsersResponse.class,
                                            resource, "GET");
    List<User> users = new ArrayList<User>();
    for (UserModel model : resp.users) {
      User t = new User(model, project, client);
      users.add(t);
    }
    return users;
  }

  public List<Role> listRoles() throws OdpsException {
    String resource = ResourceBuilder.buildRolesResource(project);
    ListRolesResponse resp = client.request(ListRolesResponse.class,
                                            resource, "GET");
    List<Role> roles = new ArrayList<Role>();
    for (RoleModel model : resp.roles) {
      Role t = new Role(model, project, client);
      roles.add(t);
    }
    return roles;
  }

  @Deprecated
  public List<Role> listRolesForUser(String uid) throws OdpsException {
    return listRolesForUserID(uid);
  }

  /**
   * 获取指定用户 id 的角色列表
   * @param uid
   * @return
   * @throws OdpsException
   */
  public List<Role> listRolesForUserID(String uid) throws OdpsException {
    return listRolesForUserInternal(uid, null);
  }

  /**
   * 获取制定用户名的角色列表
   * @param userName
   * @return
   * @throws OdpsException
   */
  public List<Role> listRolesForUserName(String userName) throws OdpsException {
    return listRolesForUserInternal(userName, "displayname");
  }

  private List<Role> listRolesForUserInternal(String user, String type) throws OdpsException {
    String resource = ResourceBuilder.buildUserResource(project, user);
    Map<String, String> params = new HashMap<String, String>();
    params.put("roles", null);
    if (!StringUtils.isNullOrEmpty(type)) {
      params.put("type", type);
    }
    ListRolesResponse resp = client.request(ListRolesResponse.class,
                                            resource, "GET", params, null, null);
    List<Role> roles = new ArrayList<Role>();
    for (RoleModel model : resp.roles) {
      Role t = new Role(model, project, client);
      roles.add(t);
    }
    return roles;
  }

  public List<User> listUsersForRole(String roleName) throws OdpsException {
    String resource = ResourceBuilder.buildRoleResource(project, roleName);
    Map<String, String> params = new HashMap<String, String>();
    params.put("users", null);
    ListUsersResponse resp = client.request(ListUsersResponse.class,
                                            resource, "GET", params, null, null);
    List<User> users = new ArrayList<User>();
    for (UserModel model : resp.users) {
      User t = new User(model, project, client);
      users.add(t);
    }
    return users;
  }

  /**
   * 查看是否有操作权限
   *
   * @param type
   *        查看权限的对象类型
   * @param objectName
   *        查看权限的对象名称
   * @param action
   *        查看的权限类型
   * @param projectName
   *        查看对象所在项目名称
   * @param columns
   *        查看权限的列名，若查看列操作权限，type 值为 Table {@link ObjectType}
   *
   * @return CheckPermissionResult
   * @throws OdpsException
   */
  public CheckPermissionResult checkPermission(ObjectType type, String objectName,
                                               ActionType action, String projectName,
                                               List<String> columns) throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(ResourceBuilder.encodeObjectName(projectName))
        .append("/auth/");

    Map<String, String> params = new HashMap<String, String>();
    params.put("type", type.toString());
    params.put("name", objectName);
    params.put("grantee", action.toString());
    if ((columns != null) && !columns.isEmpty()) {
      params.put("columns", StringUtils.join(columns.toArray(), ","));
    }

    CheckPermissionResponse response = client.request(CheckPermissionResponse.class,
                                                      resource.toString(), "GET", params, null,
                                                      null);
    System.out.println(response.getResult());
    return response.getResult().toUpperCase().equals("ALLOW") ?
           CheckPermissionResult.Allow : CheckPermissionResult.Deny;

  }

  /**
   * 查看是否有操作权限
   *
   * @param type
   *        查看权限的对象类型
   * @param objectName
   *        查看权限的对象名称
   * @param action
   *        查看的权限类型
   * @param columns
   *        查看权限的列名，若查看列操作权限，type 值为 Table {@link ObjectType}
   *
   * @return CheckPermissionResult
   * @throws OdpsException
   */
  public CheckPermissionResult checkPermission(ObjectType type, String objectName,
                                               ActionType action, List<String> columns)
      throws OdpsException {
    return checkPermission(type, objectName, action, project, columns);
  }

  /**
   * 查看是否有操作权限
   *
   * @param type
   *        查看权限的对象类型
   * @param objectName
   *        查看权限的对象名称
   * @param action
   *        查看的权限类型
   *
   * @return CheckPermissionResult
   * @throws OdpsException
   */
  public CheckPermissionResult checkPermission(ObjectType type, String objectName,
                                               ActionType action) throws OdpsException {
    return checkPermission(type, objectName, action, project);
  }

  /**
   * 查看是否有操作权限
   *
   * @param type
   *        查看权限的对象类型
   * @param objectName
   *        查看权限的对象名称
   * @param action
   *        查看的权限类型
   * @param projectName
   *        查看对象所在项目名称
   *
   * @return CheckPermissionResult
   * @throws OdpsException
   */
  public CheckPermissionResult checkPermission(ObjectType type, String objectName,
                                               ActionType action, String projectName) throws OdpsException {
    return checkPermission(type, objectName, action, projectName, null);
  }

  public String runQuery(String query, Boolean jsonOutput) throws OdpsException {
    return runQuery(query, jsonOutput, null);
  }

  public String runQuery(String query, Boolean jsonOutput,
                         String supervisionToken) throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(ResourceBuilder.encodeObjectName(project)).append("/authorization");
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
      headers.put(Headers.ODPS_SUPERVISION_TOKEN, supervisionToken);
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
      resource.append("/projects/").append(ResourceBuilder.encodeObjectName(project))
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
