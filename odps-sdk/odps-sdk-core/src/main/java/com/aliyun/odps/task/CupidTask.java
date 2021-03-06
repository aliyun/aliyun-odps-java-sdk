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

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.google.gson.GsonBuilder;

import java.util.Map;

@Root(name = "CUPID", strict = false)
public class CupidTask extends Task {

  @Element(name = "Plan", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  private String Plan;

  public String getPlan() {
    return Plan;
  }

  public void setPlan(String plan) {
    Plan = plan;
  }

  static CupidTask GetCupidTask(String plan, String taskName, Map<String, String> hints)
      throws OdpsException {
    CupidTask task = new CupidTask();
    task.setName(taskName);
    task.setPlan(plan);
    task.setProperty("type", "cupid");
    if (hints != null) {
      if (hints.containsKey("odps.cupid.task.subtype")) {
        task.setProperty("subtype", hints.get("odps.cupid.task.subtype"));
      }
      try {
        String json = new GsonBuilder().disableHtmlEscaping().create().toJson(hints);
        task.setProperty("settings", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }
    }
    return task;
  }

  public static Instance run(Odps odps, String project, String plan, Map<String, String> hints,
                             Integer priority, String runningCluster, String jobName) throws OdpsException {
    CupidTask task = GetCupidTask(plan, "cupid_task", hints);
    return odps.instances().create(project, task, priority, runningCluster, jobName);
  }

  public static Instance run(Odps odps, String project, String plan, Map<String, String> hints,
      int priority) throws OdpsException {
    CupidTask task = GetCupidTask(plan, "cupid_task", hints);
    return odps.instances().create(project, task, priority);
  }

  public static Instance run(Odps odps, String project, String plan, Map<String, String> hints)
      throws OdpsException {
    CupidTask task = GetCupidTask(plan, "cupid_task", hints);
    return odps.instances().create(project, task);
  }

}
