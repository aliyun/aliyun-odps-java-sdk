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

package com.aliyun.odps.mapred;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.mapred.bridge.LOTGenerator;
import com.aliyun.odps.mapred.conf.JobHintTranslator;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.task.LOTTask;
import com.aliyun.odps.task.LOTTask.Plan;

public class LotBridgeJobRunner extends BridgeJobRunner {

  @Override
  protected Instance submitInternal() throws OdpsException {
    byte[] lotPlan;

    Pipeline pipeline = Pipeline.fromJobConf(job);
    lotPlan = new LOTGenerator(metaExplorer.getDefaultProject(), job, pipeline).generate();

    // XXX contain 'mr' to avoid conflict with other lottask's plan name
    String planTempResource = metaExplorer.addTempResourceWithRetry(new ByteArrayInputStream(
        lotPlan), "lotplan_mr_" + jobId, Resource.Type.FILE);

    // Construct LOTTask & assign a priority. The following code is to
    // workaround with the issue that LOTTask do not support priority setting.
    LOTTask task = new LOTTask();
    Plan plan = new Plan();
    plan.setResourceName(planTempResource);
    plan.setProject(metaExplorer.getDefaultProject());

    task.setPlan(plan);

    task.setName(taskName);

    try {
      String json = JSON.toJSONString(JobHintTranslator.apply(job));
      task.setProperty("settings", json);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

    try {
      // merge aliases from SessionState
      Map<String, String> aliases = new HashMap<String, String>();
      aliases.putAll(aliasToTempResource);
      aliases.putAll(SessionState.get().getAliases());
      String json = JSON.toJSONString(aliases);
      task.setProperty("aliases", json);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

    String commandText = SessionState.get().getCommandText();
    if (commandText.length() > 0) {
      task.setProperty("commandText", commandText);
    }

    if (SessionState.get().isCostMode()) {
      task.setProperty(SessionState.ODPS_JOB_COST_ESTIMATE, "true");
    }

    return metaExplorer.createInstance(task, job.getInstancePriority());

  }
}
