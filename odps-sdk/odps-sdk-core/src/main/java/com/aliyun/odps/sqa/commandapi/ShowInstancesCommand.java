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
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.InstanceFilter;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

class ShowInstancesCommand implements Command {

  private static final Map<String, TypeInfo> instanceMap = new LinkedHashMap<>();

  static {
    instanceMap.put("StartTime", TypeInfoFactory.DATETIME);
    instanceMap.put("Duration", TypeInfoFactory.BIGINT);
    instanceMap.put("TaskStatus", TypeInfoFactory.STRING);
    instanceMap.put("InstanceId", TypeInfoFactory.STRING);
    instanceMap.put("InstanceStatus", TypeInfoFactory.STRING);
    instanceMap.put("Owner", TypeInfoFactory.STRING);
    instanceMap.put("Query", TypeInfoFactory.STRING);
  }

  private Date fromDate;
  private Date toDate;
  private Integer number;
  private String project = null;
  private Boolean onlyOwner = true;

  public ShowInstancesCommand(String fromDate, String toDate, String number) {
    this.fromDate = CommandUtil.toDate(fromDate);
    this.toDate = CommandUtil.toDate(toDate);
    this.number = number == null ? null : CommandUtil.getNumberToken(number);
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
    if (fromDate != null && toDate != null) {
      if (fromDate.equals(toDate)) {
        throw new IllegalArgumentException(
            "the date range is act as [from,to), so from and to couldn't be the same day");
      }
    }
    project = project == null ? odps.getDefaultProject() : project;
    number = number == null ? 50 : number;

    fromDate = CommandUtil.getTime(fromDate, 0);
    toDate = CommandUtil.getTime(toDate, 0);

    if (fromDate == null && toDate == null) {
      fromDate = CommandUtil.getTime(new Date(), 0);
    }

    List<Map<String, Object>> results = new ArrayList<>();

    InstanceFilter filter = new InstanceFilter();
    filter.setFromTime(fromDate);
    filter.setEndTime(toDate);
    filter.setOnlyOwner(onlyOwner);
    Iterator<Instance> insListing = odps.instances().iterator(project, filter);

    int count = 0;
    while (insListing.hasNext() && count < number) {
      Instance instance = insListing.next();
      results.add(getInstanceInfo(instance));
      count++;
    }

    return new RecordIter<>(CommandUtil.toRecord(results, instanceMap).iterator(),
                            getResultHeaders(), getResultTypes());
  }

  private Map<String, Object> getInstanceInfo(Instance instance) {
    Map<String, Object> map = new LinkedHashMap<>();

    map.put("StartTime", instance.getStartTime());

    long runTime = -1L;
    if (instance.getEndTime() != null && instance.getStartTime() != null) {
      runTime = (instance.getEndTime().getTime() - instance.getStartTime().getTime()) / 1000;
    } else if (instance.getEndTime() == null && instance.getStartTime() != null) {
      // 当instance为running时，可能为空
      runTime = (new Date().getTime() - instance.getStartTime().getTime()) / 1000;
    }
    map.put("Duration", runTime);
    map.put("TaskStatus", null);
    map.put("InstanceId", instance.getId());
    map.put("InstanceStatus", instance.getStatus().toString());
    map.put("Owner", instance.getOwner());

    StringBuilder sb = new StringBuilder();
    try {
      List<Task> tasks = instance.getTasks();
      for (Task task : tasks) {
        sb.append(task.getCommandText());
        TaskStatus status = instance.getTaskStatus().get(task.getName());
        map.put("TaskStatus", status == null ? null : status.getStatus().toString());
      }
    } catch (OdpsException ignored) {
    }

    map.put("Query", sb.toString());

    return map;
  }

}
