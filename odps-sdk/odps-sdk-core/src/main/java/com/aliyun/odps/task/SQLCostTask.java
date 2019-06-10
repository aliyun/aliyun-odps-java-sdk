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

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * SQLCoastTask的定义
 *
 * @author garr.feng
 */

@XmlRootElement(name = "SQLCost")
public class SQLCostTask extends Task {

  private String query;

  public SQLCostTask() {
    setProperty("sqlcostmode", "sqlcostmode");
  }

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
   * 通过匿名Instance运行SQLCostTask
   *
   * @param odps
   * @param sql
   * @return
   * @throws OdpsException
   */
  public static Instance run(Odps odps, String sql) throws OdpsException {
    String project = odps.getDefaultProject();
    if (project == null) {
      throw new OdpsException("default project required.");
    }
    return run(odps, project, sql, "AnonymousSQLCostTask", null);
  }

  /**
   * 通过匿名Instance运行SQLCostTask
   *
   * @param odps
   * @param project
   * @param sql
   * @param taskName
   * @param hints
   * @return
   * @throws OdpsException
   */
  public static Instance run(Odps odps, String project, String sql, Map<String, String> hints)
      throws OdpsException {
    return run(odps, project, sql, "AnonymousSQLCostTask", hints);
  }

  /**
   * 运行SQLCostTask
   *
   * @param odps
   * @param project
   * @param sql
   * @param hints
   * @return
   * @throws OdpsException
   */
  public static Instance run(Odps odps, String project, String sql,
                             String taskName, Map<String, String> hints) throws OdpsException {
    SQLCostTask task = new SQLCostTask();
    task.setQuery(sql);
    task.setName(taskName);
    if (hints != null) {
      try {
        String json = new GsonBuilder().disableHtmlEscaping().create().toJson(hints);
        task.setProperty("settings", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }
    }
    return odps.instances().create(project, task);
  }
}
