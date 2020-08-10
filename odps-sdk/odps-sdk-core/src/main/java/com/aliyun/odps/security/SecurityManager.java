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
import java.util.Objects;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.security.CheckPermissionConstants.ActionType;
import com.aliyun.odps.security.CheckPermissionConstants.CheckPermissionResult;
import com.aliyun.odps.security.CheckPermissionConstants.ObjectType;
import com.aliyun.odps.security.Role.RoleModel;
import com.aliyun.odps.security.User.UserModel;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.utils.GsonObjectBuilder;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * ODPS安全管理类
 *
 * @author mengzhe.yangmz@aliyun-inc.com
 */
public class SecurityManager {

  private String project;
  private RestClient client;
  private SecurityConfiguration securityConfigration;

  @Root(name = "Authorization", strict = false)
  private static class AuthorizationQueryRequest {

    @Element(name = "Query", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String query;

    @Element(name = "ResponseInJsonFormat", required = false)
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

  @Root(name = "Authorization", strict = false)
  private static class AuthorizationQueryResponse {

    @Element(name = "Result", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String result;

    public String getResult() {
      return result;
    }
  }

  public enum AuthorizationQueryStatus {
    TERMINATED,
    RUNNING,
    FAILED;
  }

  @Root(name = "AuthorizationQuery", strict = false)
  private static class AuthorizationQueryStatusModel {

    @Element(name = "Result", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String result;

    @Element(name = "Status", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String status;

    public String getResult() {
      return result;
    }

    public AuthorizationQueryStatus getStatus() throws OdpsException {
      if (StringUtils.isNullOrEmpty(status)) {
        throw new IllegalArgumentException("Cannot get authorization query status.");
      }

      try {
        return AuthorizationQueryStatus.valueOf(status.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new OdpsException("Unknown authorization query status: " + status);
      }
    }
  }

  public class AuthorizationQueryInstance {

    private String queryResult;
    private String instanceId;
    private String projectName;
    private SecurityManager securityManager;
    private boolean isSync;


    public AuthorizationQueryInstance(String result) {
      this.queryResult = result;
      this.isSync = true;
    }

    public AuthorizationQueryInstance(String projectName, String instanceId, SecurityManager sm) {
      if (StringUtils.isNullOrEmpty(projectName) || StringUtils.isNullOrEmpty(instanceId)) {
        throw new IllegalArgumentException(
            "Arguments: project and instance id cannot be null or empty.");
      }
      this.projectName = projectName;
      this.instanceId = instanceId;
      this.securityManager = sm;
      this.isSync = false;
    }

    public String getId() {
      if (isSync) {
        return null;
      }

      return instanceId;
    }

    public boolean isSync() {
      return isSync;
    }

    public String waitForSuccess() throws OdpsException {
      return waitForSuccess(1000);
    }

    public String waitForSuccess(long interval) throws OdpsException {
      if (isSync) {
        return queryResult;
      }

      while (!isTerminated()) {
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          throw new OdpsException(e);
        }
      }

      AuthorizationQueryStatusModel model = getModel();
      if (model.getStatus() == AuthorizationQueryStatus.TERMINATED) {
        return model.getResult();
      } else {
        throw new OdpsException("Authorization query failed: " + model.getResult());
      }
    }

    private AuthorizationQueryStatusModel getModel() throws OdpsException {
      StringBuilder resource = new StringBuilder();
      resource.append("/projects/")
          .append(ResourceBuilder.encodeObjectName(project))
          .append("/authorization/")
          .append(instanceId);
      return securityManager.client
          .request(AuthorizationQueryStatusModel.class, resource.toString(), "GET", null, null, null);

    }

    public boolean isTerminated() throws OdpsException {
      return (getStatus() != AuthorizationQueryStatus.RUNNING);
    }

    public AuthorizationQueryStatus getStatus() throws OdpsException {
      if (!isSync) {
        return getModel().getStatus();
      }

      return AuthorizationQueryStatus.TERMINATED;
    }

    public String getResult() throws OdpsException {
      if (!isSync) {
        queryResult = getModel().getResult();
      }
      return queryResult;
    }
  }

  @Root(name = "Users", strict = false)
  private static class ListUsersResponse {

    @ElementList(entry = "User", inline = true, required = false)
    private List<UserModel> users = new ArrayList<UserModel>();
  }

  @Root(name = "Roles", strict = false)
  private static class ListRolesResponse {

    @ElementList(entry = "Role", inline = true, required = false)
    private List<RoleModel> roles = new ArrayList<RoleModel>();
  }

  @Root(name = "Auth", strict = false)
  private static class CheckPermissionResponse {

    @Element(name = "Result", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String result;

    @Element(name = "Message", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String message;

    public String getResult() {
      return result;
    }

    public String getMessage() {
      return message;
    }
  }

  public SecurityManager(String project, RestClient client) {
    this.project = project;
    this.client = client;
    this.securityConfigration = null;
  }

  public SecurityConfiguration getSecurityConfiguration() {
    return getSecurityConfiguration(false);
  }

  public SecurityConfiguration getSecurityConfiguration(boolean strip) {
    SecurityConfiguration securityConfiguration;
    if (strip) {
      if (this.securityConfigration == null) {
        securityConfiguration = new SecurityConfiguration(project, client, true);
      } else {
        // If security configuration is cached, return it
        securityConfiguration = this.securityConfigration;
      }
    } else {
      if (this.securityConfigration == null) {
        // If strip is false and security configuration is not cached, cache it
        this.securityConfigration = new SecurityConfiguration(project, client, false);
      }
      securityConfiguration = this.securityConfigration;
    }

    return securityConfiguration;
  }

  public void setSecurityConfiguration(
      SecurityConfiguration securityConfigration
  ) throws OdpsException {
    securityConfigration.update(null);
  }

  public void setSecurityConfiguration(
      SecurityConfiguration securityConfigration, String supervisionToken
  )
      throws OdpsException {
    securityConfigration.update(supervisionToken);
  }

  public String getProjectPolicy() throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(project);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    Response response = client.request(resource, "GET", params, null,
                                       null
    );
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

  public String getSecurityPolicy() throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(project);
    Map<String, String> params = new HashMap<String, String>();
    params.put("security_policy", null);
    Response response = client.request(resource, "GET", params, null, null);
    try {
      return new String(response.getBody(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException("Can't parse response!", e);
    }
  }

  public void putSecurityPolicy(String securityPolicy) throws OdpsException {
    String resource = ResourceBuilder.buildProjectResource(project);
    Map<String, String> params = new HashMap<String, String>();
    params.put("security_policy", null);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/json");
    client.stringRequest(resource, "PUT", params, headers, securityPolicy);
  }

  public String getRolePolicy(String roleName) throws OdpsException {
    String resource = ResourceBuilder.buildRoleResource(project, roleName);
    Map<String, String> params = new HashMap<String, String>();
    params.put("policy", null);
    Response response = client.request(resource, "GET", params, null,
                                       null
    );
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
                                            resource, "GET"
    );
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
                                            resource, "GET"
    );
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
   *
   * @param uid
   * @return
   * @throws OdpsException
   */
  public List<Role> listRolesForUserID(String uid) throws OdpsException {
    return listRolesForUserInternal(uid, null);
  }

  /**
   * 获取制定用户名的角色列表
   *
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
                                            resource, "GET", params, null, null
    );
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
                                            resource, "GET", params, null, null
    );
    List<User> users = new ArrayList<User>();
    for (UserModel model : resp.users) {
      User t = new User(model, project, client);
      users.add(t);
    }
    return users;
  }

  public static class CheckPermissionResultInfo {
    public CheckPermissionResult result;
    public String message;

    public CheckPermissionResultInfo(CheckPermissionResult result, String message) {
      this.result = Objects.requireNonNull(result);
      this.message = message;
    }

    public CheckPermissionResult getResult() {
      return result;
    }

    public String getMessage() {
      return message;
    }
  }

  public static class PermissionDesc {
    private String projectName;
    private ObjectType objectType;
    private String objectName;
    private ActionType actionType;
    private Map<String, String> params;

    public PermissionDesc(String projectName,
                          ObjectType objectType,
                          String objectName,
                          ActionType actionType) {
      this.projectName = Objects.requireNonNull(projectName);
      this.objectType = Objects.requireNonNull(objectType);
      this.objectName = Objects.requireNonNull(objectName);
      this.actionType = Objects.requireNonNull(actionType);
      params = new HashMap<>();
    }

    public String getProjectName() {
      return projectName;
    }

    public ObjectType getObjectType() {
      return objectType;
    }

    public String getObjectName() {
      return objectName;
    }

    public ActionType getActionType() {
      return actionType;
    }

    public Map<String, String> getParams() {
      return new HashMap<>(params);
    }

    public void setPrincipal(String principal) {
      params.put("Principal", principal);
    }

    public void setColumns(List<String> columns) {
      params.put("odps:SelectColumns", GsonObjectBuilder.get().toJson(columns));
    }

    public void addParam(String key, String value) {
      params.put(key, value);
    }

    private String buildResourceString(ObjectType objectType, String objectName) {
      switch (objectType) {
        case Project:
          return ResourceBuilder.buildProjectResource(projectName);
        case Table:
          return ResourceBuilder.buildTableResource(projectName, objectName);
        case Function:
          return ResourceBuilder.buildFunctionResource(projectName, objectName);
        case Instance:
          return ResourceBuilder.buildInstanceResource(projectName, objectName);
        case Resource:
          return ResourceBuilder.buildResourceResource(projectName, objectName);
        default:
          throw new IllegalArgumentException("Unsupported object type");
      }
    }

    public String toJson() {
      JsonArray jsonArray = new JsonArray();
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("Action", actionType.name());
      jsonObject.addProperty("Resource", buildResourceString(objectType, objectName));

      for (Map.Entry<String, String> param : params.entrySet()) {
        jsonObject.addProperty(param.getKey(), param.getValue());
      }

      jsonArray.add(jsonObject);
      return GsonObjectBuilder.get().toJson(jsonArray);
    }
  }

  /**
   * Check permission
   *
   * @param desc Permission description. See {@link PermissionDesc}
   * @return {@link CheckPermissionResultInfo}
   * @throws OdpsException
   */
  public CheckPermissionResultInfo checkPermission(PermissionDesc desc)
      throws OdpsException {
    String resource = "/projects/" + ResourceBuilder.encodeObjectName(desc.projectName)
                      + "/auth/";
    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/json");
    CheckPermissionResponse response = client.stringRequest(CheckPermissionResponse.class,
                                                            resource,
                                                            "POST",
                                                            null,
                                                            headers,
                                                            desc.toJson());
    CheckPermissionResult result = CheckPermissionResult.valueOf(response.result);
    return new CheckPermissionResultInfo(result, response.message);
  }

  /**
   * 查看是否有操作权限
   *
   * @param type
   *     查看权限的对象类型
   * @param objectName
   *     查看权限的对象名称
   * @param action
   *     查看的权限类型
   * @param projectName
   *     查看对象所在项目名称
   * @param columns
   *     查看权限的列名，若查看列操作权限，type 值为 Table {@link ObjectType}
   * @return CheckPermissionResult
   * @throws OdpsException
   */
  @Deprecated
  public CheckPermissionResult checkPermission(
      ObjectType type, String objectName,
      ActionType action, String projectName,
      List<String> columns
  ) throws OdpsException {

    Map<String, String> params = new HashMap<String, String>();
    params.put("type", type.toString());
    params.put("name", objectName);
    params.put("grantee", action.toString());
    if ((columns != null) && !columns.isEmpty()) {
      params.put("columns", StringUtils.join(columns.toArray(), ","));
    }

    String resource = "/projects/" + ResourceBuilder.encodeObjectName(projectName)
                      + "/auth/";
    CheckPermissionResponse response = client.request(CheckPermissionResponse.class,
                                                      resource,
                                                      "GET",
                                                      params,
                                                      null,
                                                      null);
    return response.getResult().toUpperCase().equals("ALLOW") ?
           CheckPermissionResult.Allow : CheckPermissionResult.Deny;
  }

  /**
   * 查看是否有操作权限
   *
   * @param type
   *     查看权限的对象类型
   * @param objectName
   *     查看权限的对象名称
   * @param action
   *     查看的权限类型
   * @param columns
   *     查看权限的列名，若查看列操作权限，type 值为 Table {@link ObjectType}
   * @return CheckPermissionResult
   * @throws OdpsException
   */
  @Deprecated
  public CheckPermissionResult checkPermission(
      ObjectType type, String objectName,
      ActionType action, List<String> columns
  )
      throws OdpsException {
    return checkPermission(type, objectName, action, project, columns);
  }

  /**
   * 查看是否有操作权限
   *
   * @param type
   *     查看权限的对象类型
   * @param objectName
   *     查看权限的对象名称
   * @param action
   *     查看的权限类型
   * @return CheckPermissionResult
   * @throws OdpsException
   */
  @Deprecated
  public CheckPermissionResult checkPermission(
      ObjectType type, String objectName,
      ActionType action
  ) throws OdpsException {
    return checkPermission(type, objectName, action, project);
  }

  /**
   * 查看是否有操作权限
   *
   * @param type
   *     查看权限的对象类型
   * @param objectName
   *     查看权限的对象名称
   * @param action
   *     查看的权限类型
   * @param projectName
   *     查看对象所在项目名称
   * @return CheckPermissionResult
   * @throws OdpsException
   */
  @Deprecated
  public CheckPermissionResult checkPermission(
      ObjectType type, String objectName,
      ActionType action, String projectName
  ) throws OdpsException {
    return checkPermission(type, objectName, action, projectName, null);
  }

  /**
   * 执行安全命令, 返回 query 实例
   *
   * @param query
   * @param jsonOutput
   * @return query 实例
   * @throws OdpsException
   */
  public AuthorizationQueryInstance run(String query, Boolean jsonOutput) throws OdpsException {
    return run(query, jsonOutput, null);
  }

  /**
   * 执行安全命令, 返回 query 实例
   *
   * @param query
   * @param jsonOutput
   * @param supervisionToken
   * @return query 实例
   * @throws OdpsException
   */
  public AuthorizationQueryInstance run(String query, Boolean jsonOutput, String supervisionToken)
      throws OdpsException {
    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(ResourceBuilder.encodeObjectName(project))
        .append("/authorization");
    AuthorizationQueryRequest request = new AuthorizationQueryRequest(query, jsonOutput);

    String xmlRequest;
    try {
      xmlRequest = SimpleXmlUtils.marshal(request);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
    HashMap<String, String> headers = new HashMap<String, String>();
    if (supervisionToken != null) {
      headers.put(Headers.ODPS_SUPERVISION_TOKEN, supervisionToken);
    }
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    Response response =
        client.stringRequest(resource.toString(), "POST", null, headers, xmlRequest);
    try {
      AuthorizationQueryResponse queryResponse =
          SimpleXmlUtils.unmarshal(response, AuthorizationQueryResponse.class);

      if (response.getStatus() == 200) {
        return new AuthorizationQueryInstance(queryResponse.getResult());
      }

      return new AuthorizationQueryInstance(project, queryResponse.getResult(), this);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  /**
   * 执行安全命令, 返回 query 结果
   *
   * @param query
   * @param jsonOutput
   * @return query 结果
   * @throws OdpsException
   */
  public String runQuery(String query, Boolean jsonOutput) throws OdpsException {
    return runQuery(query, jsonOutput, null);
  }

  /**
   * 执行安全命令, 返回 query 结果
   *
   * @param query
   * @param jsonOutput
   * @param supervisionToken
   * @return query 结果
   * @throws OdpsException
   */
  public String runQuery(String query, Boolean jsonOutput, String supervisionToken)
      throws OdpsException {
    return run(query, jsonOutput, supervisionToken).waitForSuccess();
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
          headers, policy
      );
      return response.getResult();
    } else {
      // 目前只支持bearer类型的token
      throw new OdpsException("Unsupport token type" + type);
    }

  }

}
