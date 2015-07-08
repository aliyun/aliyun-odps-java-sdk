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
import com.aliyun.odps.commons.util.JacksonParser;

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

  static Instance run(Odps odps, String project, String plan, String taskName,
      Map<String, String> hints) throws OdpsException {
    MoyeTask task = new MoyeTask();
    task.setName(taskName);
    task.setPlan(plan);
    task.setProperty("type", "moye");
    if (hints != null) {
      try {
        String json = JacksonParser.getObjectMapper().writeValueAsString(hints);
        task.setProperty("settings", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }

    }
    return odps.instances().create(project, task);
  }

  public static Instance run(Odps odps, String project, String plan, Map<String, String> hints)
      throws OdpsException {
    return run(odps, project, plan, "no_use", hints);
  }

  public static Instance run(Odps odps, String project, String plan) throws OdpsException {
    return run(odps, project, plan, null);
  }
}
