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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

class DescribeInstanceCommand implements Command {

  private static final Map<String, TypeInfo> instanceMap = new LinkedHashMap<>();

  static {
    instanceMap.put("ID", TypeInfoFactory.STRING);
    instanceMap.put("Owner", TypeInfoFactory.STRING);
    instanceMap.put("StartTime", TypeInfoFactory.DATETIME);
    instanceMap.put("EndTime", TypeInfoFactory.DATETIME);
    instanceMap.put("InstanceStatus", TypeInfoFactory.STRING);
    instanceMap.put("TaskStatus",
                    TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.STRING));
    instanceMap.put("Query", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING));
  }

  private String instanceId;

  public DescribeInstanceCommand(String instanceId) {
    this.instanceId = instanceId;
  }

  @Override
  public boolean isSync() {
    return true;
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return new ArrayList<>(instanceMap.values());
  }

  @Override
  public List<String> getResultHeaders() {
    return new ArrayList<>(instanceMap.keySet());
  }

  @Override
  public RecordIter<Record> run(Odps odps, CommandInfo commandInfo) throws OdpsException {

    String projectName = odps.getDefaultProject();
    if (!(odps.instances().exists(projectName, instanceId))) {
      throw new RuntimeException("Instance not found : " + instanceId);
    }
    Instance i = odps.instances().get(projectName, instanceId);

    Map<String, Object> map = new HashMap<>();

    map.put("ID", i.getId());
    map.put("Owner", i.getOwner());
    map.put("StartTime", i.getStartTime());
    map.put("EndTime", i.getEndTime());
    map.put("InstanceStatus", i.getStatus().toString());

    Map<String, String> taskStatusMap = new LinkedHashMap<>();
    for (Map.Entry<String, Instance.TaskStatus> entry : i.getTaskStatus().entrySet()) {
      taskStatusMap.put(entry.getKey(), entry.getValue().toString());
    }
    map.put("TaskStatus", taskStatusMap);

    List<String> queryList = new ArrayList<>();
    for (Task task : i.getTasks()) {
      queryList.add(task.getCommandText());
    }
    map.put("Query", queryList);

    return new RecordIter<>(CommandUtil.toRecord(map, instanceMap).iterator(), getResultHeaders(),
                            getResultTypes());
  }

}
