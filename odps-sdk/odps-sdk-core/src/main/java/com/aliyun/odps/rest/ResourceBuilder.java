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

/**
 *
 */
package com.aliyun.odps.rest;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import com.aliyun.odps.utils.StringUtils;

/**
 * 用来构造RESTful API调用的资源标识符
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class ResourceBuilder {

  public static final String TENANTS = "/tenants";
  public static final String AUTHORIZATION = "/authorization";
  public static final String PROJECTS = "/projects";
  public static final String SCHEMAS = "/schemas";
  public static final String TABLES = "/tables";
  public static final String REGISTRATION = "/registration";
  public static final String FUNCTIONS = "/functions";
  public static final String EVENTS = "/events";
  public static final String RESOURCES = "/resources";
  public static final String INSTANCES = "/instances";
  public static final String CACHED_INSTANCES = "/cachedinstances";
  public static final String VOLUMES = "/volumes";
  private static final String STREAMS = "/streams";
  private static final String TOPOLOGIES = "/topologies";
  private static final String XFLOWS = "/xflows";
  private static final String STREAMJOBS = "/streamjobs";
  private static final String SERVERS = "/servers";
  private static final String MATRICES = "/matrices";
  private static final String CLASSIFICATIONS =  "/classifications";
  private static final String TAGS = "/tags";
  private static final String QUOTAS = "/quotas";

  private static final String OFFLINEMODELS = "/offlinemodels";
  private static final String USERS = "/users";
  private static final String ROLES = "/roles";
  private static final String SESSIONS = "/session";
  private static final String CLUSTERS = "/clusters";

  private static final String API = "/api";
  private static final String STORAGE = "/storage";
  private static final String TABLE_SESSIONS = "/sessions";
  private static final String DATA = "/data";
  private static final String COMMIT = "/commit";

  public static String buildProjectsResource() {
    return PROJECTS;
  }

  public static String buildProjectResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));

    return sb.toString();
  }

  public static String buildTablesResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(TABLES);

    return sb.toString();
  }

  public static String buildTableResource(String projectName, String tableName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(TABLES).append('/').append(encodeObjectName(tableName));

    return sb.toString();
  }

  public static String buildTableResource(String projectName, String schemaName, String tableName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS)
      .append('/')
      .append(encodeObjectName(projectName));
    if (!StringUtils.isNullOrEmpty(schemaName)) {
      sb.append(SCHEMAS)
        .append('/')
        .append(encodeObjectName(schemaName));
    }
    sb.append(TABLES)
      .append('/')
      .append(encodeObjectName(tableName));
    return sb.toString();
  }

  public static String buildFunctionsResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(REGISTRATION)
        .append(FUNCTIONS);

    return sb.toString();
  }

  public static String buildFunctionResource(String projectName, String functionName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(REGISTRATION).append(FUNCTIONS).append('/').append(encodeObjectName(functionName));

    return sb.toString();
  }

  public static String buildXFlowsResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(XFLOWS);

    return sb.toString();
  }

  public static String buildXFlowResource(String projectName, String xFlowName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(XFLOWS).append('/').append(encodeObjectName(xFlowName));

    return sb.toString();
  }

  public static String buildInstancesResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(INSTANCES);

    return sb.toString();
  }

  public static String buildCachedInstancesResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(CACHED_INSTANCES);

    return sb.toString();
  }


  public static String buildInstanceResource(String projectName, String instanceId) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(INSTANCES).append('/').append(encodeObjectName(instanceId));

    return sb.toString();
  }

  public static String buildResourcesResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(RESOURCES);

    return sb.toString();
  }

  public static String buildResourceResource(String projectName, String resourceName) {

    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(RESOURCES).append('/').append(encodeObjectName(resourceName));

    return sb.toString();
  }

  public static String buildVolumesResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(VOLUMES);

    return sb.toString();
  }

  public static String buildVolumeResource(String projectName, String volumeName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(VOLUMES).append('/').append(encodeObjectName(volumeName));

    return sb.toString();
  }

  public static String buildStreamsResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(STREAMS);

    return sb.toString();
  }

  public static String buildStreamResource(String projectName, String streamName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(STREAMS).append('/').append(encodeObjectName(streamName));

    return sb.toString();
  }

  public static String buildTopologiesResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(TOPOLOGIES);

    return sb.toString();
  }

  public static String buildTopologyResource(String projectName, String topologyName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(TOPOLOGIES).append('/').append(encodeObjectName(topologyName));

    return sb.toString();
  }

  public static String buildStreamJobsResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(STREAMJOBS);

    return sb.toString();
  }

  public static String buildStreamJobResource(String projectName, String streamJobName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(STREAMJOBS).append('/').append(encodeObjectName(streamJobName));

    return sb.toString();
  }

  public static String buildServersResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(SERVERS);

    return sb.toString();
  }

  public static String buildServerResource(String projectName, String serverName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(SERVERS).append('/').append(encodeObjectName(serverName));

    return sb.toString();
  }


  public static String buildVolumePartitionResource(String projectName, String volumeName,
                                                    String partitionName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(VOLUMES).append('/').append(encodeObjectName(volumeName));
    sb.append('/').append(partitionName);

    return sb.toString();
  }

  public static String encodeObjectName(String name) {
    if (name == null || name.trim().length() != name.length()) {
      throw new IllegalArgumentException("Invalid name: " + name);
    }

    return encode(name);
  }

  public static String encode(String str) {
    if (str == null || str.length() == 0) {
      return str;
    }

    String r = null;
    try {
      r = URLEncoder.encode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("Encode failed: " + str);
    }
    r = r.replaceAll("\\+", "%20");
    return r;
  }

  public static String buildEventsResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(REGISTRATION)
        .append(EVENTS);

    return sb.toString();
  }

  public static String buildEventResource(String projectName, String eventName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName));
    sb.append(REGISTRATION).append(EVENTS).append('/').append(encodeObjectName(eventName));

    return sb.toString();
  }

  public static String buildOfflineModelResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(OFFLINEMODELS);

    return sb.toString();
  }

  public static String buildOfflineModelResource(String projectName,
                                                 String modelName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(OFFLINEMODELS).append("/").append(modelName);

    return sb.toString();
  }

  public static String buildMatrixResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(MATRICES);

    return sb.toString();
  }

  public static String buildMatrixResource(String projectName, String maxtrixName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(MATRICES).append("/").append(maxtrixName);

    return sb.toString();
  }

  public static String buildUsersResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(USERS);

    return sb.toString();
  }

  public static String buildUserResource(String projectName, String user) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(USERS).append("/")
        .append(encodeObjectName(user));

    return sb.toString();
  }

  public static String buildRolesResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(ROLES);

    return sb.toString();
  }

  public static String buildRoleResource(String projectName, String roleName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(ROLES).append("/")
        .append(roleName);

    return sb.toString();
  }

  public static String buildSessionsResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName)).append(SESSIONS);

    return sb.toString();
  }

  public static String buildClassificationsResource(String projectName) {
    return PROJECTS + "/" + encodeObjectName(projectName) + CLASSIFICATIONS;
  }

  public static String buildClassificationResource(String projectName, String classificationName) {
    return PROJECTS + "/" + encodeObjectName(projectName) + CLASSIFICATIONS
        + "/" + encodeObjectName(classificationName);
  }

  public static String buildTagsResource(String projectName, String classificationName) {
    return PROJECTS + "/" + encodeObjectName(projectName) + CLASSIFICATIONS
        + "/" + encodeObjectName(classificationName) + TAGS;
  }

  public static String buildTagResource(
      String projectName,
      String classificationName,
      String tagName) {
    return PROJECTS + "/" + encodeObjectName(projectName) + CLASSIFICATIONS
        + "/" + encodeObjectName(classificationName) + TAGS + "/" + encodeObjectName(tagName);
  }

  public static String buildProjectSecurityManagerResource(String projectName) {
    return PROJECTS
        + "/" + encodeObjectName(projectName)
        + AUTHORIZATION;
  }

  public static String buildProjectAuthorizationInstanceResource(
      String projectName,
      String instanceId) {
    return PROJECTS
        + "/" + encodeObjectName(projectName)
        + AUTHORIZATION
        + "/" + instanceId;
  }

  public static String buildTenantSecurityManagerResource(String tenantId) {
    return TENANTS
        + "/" + encodeObjectName(tenantId)
        + AUTHORIZATION;
  }

  public static String buildTenantAuthorizationInstanceResource(
      String tenantId,
      String instanceId) {
    return TENANTS
        + "/" + encodeObjectName(tenantId)
        + AUTHORIZATION
        + "/" + instanceId;
  }


  public static String buildTenantRoleResource(String tenantId, String roleName) {
    return TENANTS
        + "/" + encodeObjectName(tenantId)
        + AUTHORIZATION
        + ROLES
        + "/" + encodeObjectName(roleName);
  }

  public static String buildTenantUsersResource(String tenantId) {
    return TENANTS
        + "/" + encodeObjectName(tenantId)
        + AUTHORIZATION
        + USERS;
  }

  public static String buildQuotaResource(String name) {
    return QUOTAS + "/" + encodeObjectName(name);
  }

  public static String buildClustersResource() {
    return CLUSTERS;
  }

  public static String buildStoragePrefix(String version) {
    StringBuilder sb = new StringBuilder();

    sb.append(API).append(STORAGE);
    if (!StringUtils.isNullOrEmpty(version)) {
      sb.append("/").append(version);
    }
    return sb.toString();
  }

  public static String buildTableSessionResource(String version,
                                                 String projectName,
                                                 String schemaName,
                                                 String tableName,
                                                 String sessionId) {
    StringBuilder sb = new StringBuilder();
    sb.append(buildStoragePrefix(version));
    sb.append(buildTableResource(projectName, schemaName, tableName));
    if (StringUtils.isNullOrEmpty(sessionId)) {
      sb.append(TABLE_SESSIONS);
    } else {
      sb.append(TABLE_SESSIONS)
              .append('/')
              .append(encodeObjectName(sessionId));
    }
    return sb.toString();
  }

  public static String buildTableDataResource(String version,
                                                String projectName,
                                                String schemaName,
                                                String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append(buildStoragePrefix(version));
    sb.append(buildTableResource(projectName, schemaName, tableName));
    sb.append(DATA);
    return sb.toString();
  }

  public static String buildTableSessionDataResource(String version,
                                                     String projectName,
                                                     String schemaName,
                                                     String tableName,
                                                     String sessionId) {
    StringBuilder sb = new StringBuilder();
    sb.append(buildStoragePrefix(version));
    sb.append(buildTableResource(projectName, schemaName, tableName));
    sb.append(TABLE_SESSIONS)
            .append('/')
            .append(encodeObjectName(sessionId));
    sb.append(DATA);
    return sb.toString();
  }

  public static String buildTableCommitResource(String version,
                                                String projectName,
                                                String schemaName,
                                                String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append(buildStoragePrefix(version));
    sb.append(buildTableResource(projectName, schemaName, tableName));
    sb.append(COMMIT);
    return sb.toString();
  }
}
