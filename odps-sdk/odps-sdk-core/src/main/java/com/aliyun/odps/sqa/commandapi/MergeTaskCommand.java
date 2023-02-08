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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

abstract class MergeTaskCommand implements Command {

  private static final String ARCHIVE_SETTINGS = "archiveSettings";
  private String tableName;
  private String partition;
  // TODO: support schema

  private String instanceId = "";

  public MergeTaskCommand(String tableName, String partition) {
    this.tableName = tableName;
    this.partition = partition;
  }

  void runJob(Task task, Odps odps) throws OdpsException {
    instanceId = CommandUtil.runJob(task, odps);
  }

  private void addSetting(Map<String, String> config, Map<String, String> setting) {
    String origSettings = null;
    String addedSettings = null;

    Entry<String, String> property = null;
    for (Entry<String, String> pr : config.entrySet()) {
      if ("settings".equals(pr.getKey())) {
        property = pr;
        origSettings = pr.getValue();
        break;
      }
    }
    if (property == null || origSettings == null) {
      try {
        addedSettings = new GsonBuilder().disableHtmlEscaping().create().toJson(setting);
      } catch (Exception e) {
        return;
      }

      if (addedSettings != null) {
        config.put("settings", addedSettings);
      }
    } else {
      try {
        JsonObject jsonObject = new JsonParser().parse(origSettings).getAsJsonObject();
        for (Entry<String, String> prop : setting.entrySet()) {
          jsonObject.addProperty(prop.getKey(), prop.getValue());
        }
        addedSettings = jsonObject.toString();
      } catch (Exception e) {
        return;
      }

      if (addedSettings != null) {
        property.setValue(addedSettings);
      }
    }
  }

  RecordIter<Record> mergeTaskRun(Odps odps, CommandInfo commandInfo, String taskNamePrefix,
                                  Map<String, String> settingMap) {
    int retryTime = odps.getRestClient().getRetryTimes();
    retryTime = retryTime > 0 ? retryTime : 1;
    String tableInfo = getTableName() + " " + getPartition();
    Map<String, String> settings = commandInfo.getHint();

    while (true) {
      try {
        String taskName =
            taskNamePrefix + Calendar.getInstance().getTimeInMillis();
        MergeTask task = new MergeTask(taskName, tableInfo);

        Map<String, String> map = new HashMap<>(settingMap);
        if (map.containsKey(ARCHIVE_SETTINGS)) {
          task.setProperty(ARCHIVE_SETTINGS, map.get(ARCHIVE_SETTINGS));
          map.remove(ARCHIVE_SETTINGS);
        }

        addSetting(settings, map);

        for (Entry<String, String> property : settings.entrySet()) {
          task.setProperty(property.getKey(), property.getValue());
        }

        runJob(task, odps);
        Instance instance = odps.instances().get(getInstanceId());
        commandInfo.setTaskName(taskName);
        commandInfo.setInstance(instance, odps.logview().generateLogView(instance, 7 * 24), null);

        break;
      } catch (Exception e) {
        if (--retryTime <= 0) {
          throw new RuntimeException(e.getMessage());
        }
      }
    }
    return null;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartition() {
    return partition;
  }

  public String getInstanceId() {
    return instanceId;
  }

  /**
   * 结果集从instance中获取,默认只有一列。
   *
   * @return
   */
  @Override
  public List<String> getResultHeaders() {
    return Collections.singletonList("Info");
  }


  /**
   * 结果集从instance中获取,默认只有一列。
   *
   * @return
   */
  @Override
  public List<TypeInfo> getResultTypes() {
    return Collections.singletonList(TypeInfoFactory.STRING);
  }
}
