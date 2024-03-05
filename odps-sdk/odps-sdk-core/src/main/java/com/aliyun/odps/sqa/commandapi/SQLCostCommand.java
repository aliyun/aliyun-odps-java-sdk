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

package com.aliyun.odps.sqa.commandapi;

import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.task.SQLCostTask;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.gson.GsonBuilder;

class SQLCostCommand implements Command {

  private String query;
  private String instanceId = "";

  public SQLCostCommand(String query) {
    this.query = query;
  }

  @Override
  public boolean isSync() {
    return false;
  }

  @Override
  public List<String> getResultHeaders() {
    return Collections.singletonList("Info");
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return Collections.singletonList(TypeInfoFactory.STRING);
  }

  @Override
  public RecordIter<Record> run(Odps odps, CommandInfo commandInfo) throws OdpsException {

    String taskName = "sql_cost_command_task_" + Calendar.getInstance().getTimeInMillis();
    SQLCostTask task = new SQLCostTask();
    task.setName(taskName);
    task.setQuery(query + ";");

    Map<String, String> settings = commandInfo.getHint();
    if (settings != null) {
      try {
        String json = (new GsonBuilder()).disableHtmlEscaping().create().toJson(settings);
        task.setProperty("settings", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }
    }

    instanceId = CommandUtil.runJob(task, odps);
    Instance instance = odps.instances().get(instanceId);
    commandInfo.setTaskName(taskName);
    commandInfo.setInstance(instance, odps.logview().generateLogView(instance, 7 * 24), null);
    return null;
  }
}
