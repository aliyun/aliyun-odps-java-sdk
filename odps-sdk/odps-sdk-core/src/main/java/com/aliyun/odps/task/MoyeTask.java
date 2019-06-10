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

@XmlRootElement(name = "MOYE")
public class MoyeTask extends Task {

  private String Plan;

  public String getPlan() {
    return Plan;
  }

  @XmlElement(name = "Plan")
  public void setPlan(String plan) {
    Plan = plan;
  }

  static MoyeTask GetMoyeTask(String plan, String taskName, Map<String, String> hints)
      throws OdpsException {
    MoyeTask task = new MoyeTask();
    task.setName(taskName);
    task.setPlan(plan);
    task.setProperty("type", "moye");
    if (hints != null) {
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
      int priority) throws OdpsException {
    MoyeTask task = GetMoyeTask(plan, "no_use", hints);
    return odps.instances().create(project, task, priority);
  }

  public static Instance run(Odps odps, String project, String plan, Map<String, String> hints)
      throws OdpsException {
    MoyeTask task = GetMoyeTask(plan, "no_use", hints);
    return odps.instances().create(project, task);
  }

}
