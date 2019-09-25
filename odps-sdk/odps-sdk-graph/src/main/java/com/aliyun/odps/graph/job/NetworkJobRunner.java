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

package com.aliyun.odps.graph.job;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task.Properties;
import com.aliyun.odps.Task.Property;
import com.aliyun.odps.conf.Configured;
import com.aliyun.odps.graph.GRAPH_CONF;
import com.aliyun.odps.graph.JobConf;
import com.aliyun.odps.graph.common.COMMON_GRAPH_CONF;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.task.GraphTask;
import com.aliyun.odps.utils.CommonUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 用于向ODPS服务端提交Graph作业
 */
public class NetworkJobRunner extends Configured implements JobRunner {

  @Override
  public RunningJob submit() throws OdpsException {

    String taskName = CommonUtils.generateGraphTaskName();

    GraphTask task = new GraphTask(taskName);

    Properties jobConf = new Properties();

    // console set
    JobConf confInConsole = new JobConf(SessionState.get().getDefaultJob());
    // job set
    JobConf confInJob = new JobConf(getConf());

    // add console set to settings
    Map<String, String> settings = new HashMap<String, String>();
    Iterator<Map.Entry<String, String>> settingsIter = confInConsole.iterator();
    while (settingsIter.hasNext()) {
      Map.Entry<String, String> entry = (Map.Entry<String, String>) settingsIter.next();
      settings.put(entry.getKey(), entry.getValue());
    }

    // add console set over job set
    Iterator<Map.Entry<String, String>> it = confInConsole.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
      // cache resources in console should not overwrite cache resource in job set
      // since -resources|-libjars set exists in both jobconf 
      if (entry.getKey().equals(GRAPH_CONF.CACHE_RESOURCES) ||
          entry.getKey().equals(GRAPH_CONF.CLASSPATH_RESOURCES)) {
        continue;
      }
      confInJob.set(entry.getKey(), entry.getValue());
    }

    // add resource to confInJob
    String[] resources = SessionState.get().getDefaultJob().getResources();
    if (resources != null) {
      for (String resource : resources) {
        confInJob.addCacheResourcesToClassPath(resource);
      }
    }

    // all job uses new sdk.
    jobConf.addProperty(new Property(COMMON_GRAPH_CONF.USE_NEW_SDK, "true"));

    // handle priority
    int priority = confInJob.getJobPriority();

    // copy confInJob to jobConf
    Iterator<Map.Entry<String, String>> itInSession = confInJob.iterator();
    while (itInSession.hasNext()) {
      Map.Entry<String, String> entry = (Map.Entry<String, String>) itInSession.next();
      Property pType = new Property(entry.getKey(), entry.getValue());
      jobConf.addProperty(pType);
    }

    // handle settings
    try {
      String json = new GsonBuilder().disableHtmlEscaping().create().toJson(settings);
      task.setProperty("settings", json);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

    // handle alias
    Map<String, String> aliases = SessionState.get().getAliases();
    if (aliases != null) {
      try {
        String json = new GsonBuilder().disableHtmlEscaping().create().toJson(aliases);
        task.setProperty("aliases", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }
    }

    task.setJobConf(jobConf);

    Odps odps = SessionState.get().getOdps();
    String project = odps.getDefaultProject();
    Instance instance = GraphTask.run(odps, project, task, priority, confInJob.get(GRAPH_CONF.RUNNING_CLUSTER, null));
    if (instance == null) {
      throw new RuntimeException("Client-side submit job failed.");
    }

    NetworkRunningJob rJob = new NetworkRunningJob(task, instance);
    if (SessionState.get().isCostMode()) {
      rJob.setIsCostMode(true);
    }

    System.err.println();
    System.err.println("ID = " + rJob.getInstanceID());

    printLogViewIfNecessary(instance);
    return rJob;
  }

  // print logview in old console, do not print logview in new console
  private void printLogViewIfNecessary(Instance instance) {
    try {
      String log = SessionState.get().getOdps().logview().generateLogView(instance, 7 * 24);
      System.out.println(log);
    } catch (Exception e) {
      // do nothing if not load logview class
    }
  }

}
