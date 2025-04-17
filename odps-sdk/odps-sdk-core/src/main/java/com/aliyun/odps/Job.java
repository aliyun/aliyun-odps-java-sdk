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

package com.aliyun.odps;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.ElementListUnion;
import com.aliyun.odps.simpleframework.xml.Path;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.task.AlgoTask;
import com.aliyun.odps.task.CupidTask;
import com.aliyun.odps.task.GalaxyTask;
import com.aliyun.odps.task.GraphTask;
import com.aliyun.odps.task.LOTTask;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.task.MoyeTask;
import com.aliyun.odps.task.SQLCostTask;
import com.aliyun.odps.task.SQLRTTask;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.task.SqlPlanTask;
import com.aliyun.odps.task.XLibTask;

/**
 * ODPS Job定义。ODPS内部使用。
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class Job {

  @Root(name = "Job", strict = false)
  static class JobModel {
    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date creationTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModified;

    @Element(name = "Priority", required = false)
    int priority = 9; // Default value 9.

    @Element(name = "RunningCluster", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String runningCluster;

    @Element(name = "Guid", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String uniqueIdentifyID;

    @Path(value = "Tasks")
    @ElementListUnion({
        @ElementList(entry = "Graph", inline = true, type = GraphTask.class),
        @ElementList(entry = "LOT", inline = true, type = LOTTask.class),
        @ElementList(entry = "SQLPlan", inline = true, type = SqlPlanTask.class),
        @ElementList(entry = "SQL", inline = true, type = SQLTask.class),
        @ElementList(entry = "SQLRT", inline = true, type = SQLRTTask.class),
        @ElementList(entry = "XLib", inline = true, type = XLibTask.class),
        @ElementList(entry = "SQLCost", inline = true, type = SQLCostTask.class),
        @ElementList(entry = "MOYE", inline = true, type = MoyeTask.class),
        @ElementList(entry = "Galaxy", inline = true, type = GalaxyTask.class),
        @ElementList(entry = "CUPID", inline = true, type = CupidTask.class),
        @ElementList(entry = "Merge", inline = true, type = MergeTask.class),
        @ElementList(entry = "AlgoTask", inline = true, type = AlgoTask.class),
    })
    List<Task> tasks = new LinkedList<Task>();
  }

  JobModel model;

  /* Un-document */
  public Job() {
    model = new JobModel();
  }

  public Job(JobModel model) {
    this.model = model;
  }

  /* Un-document */
  public String getName() {
    return model.name;
  }

  /* Un-document */
  public void setName(String name) {
    model.name = name;
  }

  /* Un-document */
  public String getComment() {
    return model.comment;
  }

  /* Un-document */
  public void setComment(String comment) {
    model.comment = comment;
  }

  /* Un-document */
  public String getOwner() {
    return model.owner;
  }

  /* Un-document */
  public int getPriority() {
    return model.priority;
  }

  /**
   * 设置作业优先级。优先级的取值去见为[0, 9]的整型值，数字越大，优先级越低。
   *
   * @param priority
   *     优先级 (注：公共云环境此参数无效)
   */
  public void setPriority(int priority) {
    model.priority = priority;
  }

  public void setUniqueIdentifyID(String uniqueIdentifyID) {
    model.uniqueIdentifyID = uniqueIdentifyID;
  }

  /* Un-document */
  public Date getCreatedTime() {
    return model.creationTime;
  }

  /* Un-document */
  public Date getLastModifiedTime() {
    return model.lastModified;
  }

  /* Un-document */
  public List<Task> getTasks() {
    return model.tasks;
  }

  /* Un-document */
  public void addTask(Task task) {
    model.tasks.add(task);
  }

  public String getRunningCluster() {
    return model.runningCluster;
  }

  public void setRunningCluster(String runningCluster) {
    model.runningCluster = runningCluster;
  }

  /* Un-document */
  public String toXmlString() throws OdpsException {
    try {
      return SimpleXmlUtils.marshal(model);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }
}
