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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Calendar;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.task.GalaxyTask;
import com.aliyun.odps.commons.util.JacksonParser;

/**
 * StreamJobs表示ODPS中所有StreamJob的集合
 *
 * @author zhiyong.dai@alibaba-inc.com
 */
public class StreamJobs {

  RestClient client;
  Odps odps;

  public StreamJobs(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  public String list() throws OdpsException {
    GalaxyTask task = new GalaxyTask();
    String taskName = "LIST_STREAMJOB_TASK";
    task.setName(taskName);
    task.setOperationType("LIST_STREAMJOB");
    //TODO 处理异常情况
    return runInstance(getDefaultProjectName(), task);
  }

  /**
   * 创建StreamJob
   *
   * @param streamJobName
   *     所要创建的StreamJob名
   * @param sql
   *     StreamJob的sql描述
   * @param hints
   *     能够影响Streamjob执行的Set信息，例如：odps.streamjob.work.size等
   * @throws OdpsException
   */
  public String create(String streamJobName, String sql, Map<String, String> hints) throws OdpsException {
    return create(getDefaultProjectName(), streamJobName, sql, hints);
  }

  /**
   * 创建StreamJob
   *
   * @param projectName
   *     StreamJob所在Project名称
   * @param streamJobName
   *     所要创建的StreamJob名
   * @param sql 
   *     StreamJob的SQL描述
   * @param hints
   *     能够影响Streamjob执行的Set信息，例如：odps.streamjob.work.size等
   * @throws OdpsException
   */
  public String create(String projectName, String streamJobName, String sql, Map<String, String> hints)
      throws OdpsException {
    if (projectName == null || streamJobName == null) {
      throw new IllegalArgumentException();
    }
    if (sql.length() == 0) {
      throw new OdpsException("Sql can not be empty");
    }

    GalaxyTask task = new GalaxyTask();
    task.setName("CREATE_STREAMJOB_TASK");
    task.setStreamJobName(streamJobName);
    task.setOperationType("CREATE_STREAMJOB");
    task.setSql(sql);

    if (hints != null) {
      try {
        String json = JacksonParser.getObjectMapper().writeValueAsString(hints);
        task.setSettings(json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }
    }

    return runInstance(projectName, task);
  }

  /**
   * 删除StreamJob
   *
   * @param streamJobName
   *     StreamJob名
   * @throws OdpsException
   */
  public String delete(String streamJobName) throws OdpsException {
    return delete(getDefaultProjectName(), streamJobName);
  }

  /**
   * 删除StreamJob
   *
   * @param projectName
   *     StreamJob所在Project
   * @param streamJobName
   *     StreamJob名
   * @throws OdpsException
   */
  public String delete(String projectName, String streamJobName) throws OdpsException {
    if (projectName == null || streamJobName == null) {
      throw new IllegalArgumentException();
    }

    GalaxyTask task = new GalaxyTask();
    task.setName("DELETE_STREAMJOB_TASK");
    task.setStreamJobName(streamJobName);
    task.setOperationType("DELETE_STREAMJOB");

    return runInstance(projectName, task);
  }
  /**
   * 获取StreamJob的状态
   *
   * @param streamJobName
   *     StreamJob名
   * @throws OdpsException
   */
  public String getStatus(String streamJobName) throws OdpsException {
    return getStatus(getDefaultProjectName(), streamJobName);
  }

  /**
   * 获取StreamJob的状态
   *
   * @param projectName
   *     StreamJob所在Project
   * @param streamJobName
   *     StreamJob名
   * @throws OdpsException
   */
  public String getStatus(String projectName, String streamJobName) throws OdpsException {
    if (projectName == null || streamJobName == null) {
      throw new IllegalArgumentException();
    }

    GalaxyTask task = new GalaxyTask();
    task.setName("GETSTATUS_STREAMJOB_TASK");
    task.setStreamJobName(streamJobName);
    task.setOperationType("STATUS_STREAMJOB");

    return runInstance(projectName, task);
  }

  /**
   * 暂停StreamJob
   *
   * @param streamJobName
   *     StreamJob名
   * @throws OdpsException
   */
  public String pause(String streamJobName) throws OdpsException {
    return pause(getDefaultProjectName(), streamJobName);
  }

  /**
   * 暂停StreamJob
   *
   * @param projectName
   *     StreamJob所在Project
   * @param streamJobName
   *     StreamJob名
   * @throws OdpsException
   */
  public String pause(String projectName, String streamJobName) throws OdpsException {
    if (projectName == null || streamJobName == null) {
      throw new IllegalArgumentException();
    }

    GalaxyTask task = new GalaxyTask();
    task.setName("PAUSE_STREAMJOB_TASK");
    task.setStreamJobName(streamJobName);
    task.setOperationType("PAUSE_STREAMJOB");

    return runInstance(projectName, task);
  }

  /**
   * 恢复StreamJob
   *
   * @param streamJobName
   *     StreamJob名
   * @param hints
   *     能够影响Streamjob执行的Set信息，例如：odps.streamjob.work.size等
   * @throws OdpsException
   */
  public String resume(String streamJobName, Map<String, String> hints) throws OdpsException {
    return resume(getDefaultProjectName(), streamJobName, hints);
  }

  /**
   * 恢复StreamJob
   *
   * @param projectName
   *     StreamJob所在Project
   * @param streamJobName
   *     StreamJob名
   * @param hints
   *     能够影响Streamjob执行的Set信息，例如：odps.streamjob.work.size等
   * @throws OdpsException
   */
  public String resume(String projectName, String streamJobName, Map<String, String> hints)
      throws OdpsException {
    if (projectName == null || streamJobName == null) {
      throw new IllegalArgumentException();
    }

    GalaxyTask task = new GalaxyTask();
    task.setName("RESUME_STREAMJOB_TASK");
    task.setStreamJobName(streamJobName);
    task.setOperationType("RESUME_STREAMJOB");

    if (hints != null) {
      try {
        String json = JacksonParser.getObjectMapper().writeValueAsString(hints);
        task.setSettings(json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }
    }

    return runInstance(projectName, task);
  }

  /**
   * Rebalance StreamJob
   *
   * @param streamJobName
   *     StreamJob名
   * @param hints
   *     必须包含odps.streamjob.worker.num和odps.streamjob.worker.mem
   * @throws OdpsException
   */
  public String rebalance(String streamJobName, Map<String, String> hints) throws OdpsException {
    return rebalance(getDefaultProjectName(), streamJobName, hints);
  }

  /**
   * Rebalance StreamJob
   *
   * @param projectName
   *     StreamJob所在Project
   * @param streamJobName
   *     StreamJob名
   * @param hints
   *     必须包含odps.streamjob.worker.num和odps.streamjob.worker.mem
   * @throws OdpsException
   */
  public String rebalance(String projectName, String streamJobName, Map<String, String> hints)
      throws OdpsException {
    if (projectName == null || streamJobName == null) {
      throw new IllegalArgumentException();
    }

    GalaxyTask task = new GalaxyTask();
    task.setName("REBALANCE_STREAMJOB_TASK");
    task.setStreamJobName(streamJobName);
    task.setOperationType("REBALANCE_STREAMJOB");

    //TODO check hints must include odps.streamjob.worker.num & odps.streamjob.worker.mem
    if (hints != null) {
      try {
        String json = JacksonParser.getObjectMapper().writeValueAsString(hints);
        task.setSettings(json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }
    }

    return runInstance(projectName, task);
  }

  /* private */
  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }

  private String runInstance(String projectName, GalaxyTask task) throws OdpsException {
    Instance instance = odps.instances().create(projectName, task);
    instance.waitForSuccess();
    Map<String, String> resultMap = instance.getTaskResults();
    return resultMap.get(task.getName());
  }

}
