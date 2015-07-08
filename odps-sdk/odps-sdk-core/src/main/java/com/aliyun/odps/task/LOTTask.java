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

package com.aliyun.odps.task;

import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.commons.util.JacksonParser;

/**
 * LOTTask的定义
 *
 * @author garr.feng@alibaba-inc.com
 */
@XmlRootElement(name = "LOT")
public class LOTTask extends Task {

  @XmlType(name = "plan", propOrder = {"resourceName", "project"})
  public static class Plan {

    private String resourceName;

    private String project;

    public void setResourceName(String resourceName) {
      this.resourceName = resourceName;
    }

    @XmlElement(name = "ResourceName")
    public String getResourceName() {
      return this.resourceName;
    }

    public void setProject(String project) {
      this.project = project;
    }

    @XmlElement(name = "Project")
    public String getProject() {
      return this.project;
    }
  }

  private Plan plan;

  public Plan getPlan() {
    return plan;
  }

  private String query;

  public String getQuery() {
    return query;
  }

  /**
   * 设置SQL查询语句
   *
   * @param query
   */
  @XmlElement(name = "Query")
  public void setQuery(String query) {
    this.query = query;
  }

  /**
   * 设置查询Plan
   *
   * @param plan
   */
  @XmlElement(name = "Plan")
  public void setPlan(Plan plan) {
    this.plan = plan;
  }


  /**
   * 通过匿名Instance运行LOTTask
   *
   * @param odps
   * @param input
   * @return
   * @throws OdpsException
   */
  public static Instance run(Odps odps, String input, boolean runSQL) throws OdpsException {
    String project = odps.getDefaultProject();
    if (project == null) {
      throw new OdpsException("default project required.");
    }
    return run(odps, project, input, runSQL, "AnonymousLOTTask", null, null);
  }

  /**
   * 通过匿名Instance运行LOTTask
   *
   * @param odps
   * @param project
   * @param input
   * @param taskName
   * @param hints
   * @return
   * @throws OdpsException
   */
  public static Instance run(Odps odps, String project, String input,
                             boolean runSQL, Map<String, String> hints, Map<String, String> aliases)
      throws OdpsException {
    return run(odps, project, input, runSQL, "AnonymousLOTTask", hints, aliases);
  }

  /**
   * 运行LOTTask
   *
   * @param odps
   * @param project
   * @param input
   * @param hints
   * @return
   * @throws OdpsException
   */
  public static Instance run(Odps odps, String project, String input,
                             boolean runSQL, String taskName, Map<String, String> hints,
                             Map<String, String> aliases) throws OdpsException {
    LOTTask task = new LOTTask();
    if (runSQL) {
      task.setQuery(input);
    } else {
      Plan plan = new Plan();
      plan.setResourceName(input);
      plan.setProject(project);

      task.setPlan(plan);
    }

    task.setName(taskName);

    if (hints != null) {
      try {
        String json = JacksonParser.getObjectMapper().writeValueAsString(hints);
        task.setProperty("settings", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }

    }

    if (aliases != null) {
      try {
        String json = JacksonParser.getObjectMapper().writeValueAsString(aliases);
        task.setProperty("aliases", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }

    }

    return odps.instances().create(project, task);
  }
}
