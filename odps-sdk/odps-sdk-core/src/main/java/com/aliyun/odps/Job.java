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

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.rest.JAXBUtils;

/**
 * ODPS Job定义。ODPS内部使用。
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class Job {

  @XmlRootElement(name = "Job")
  static class JobModel {

    @XmlElement(name = "Name")
    String name;

    @XmlElement(name = "Comment")
    String comment;

    @XmlElement(name = "Owner")
    String owner;

    @XmlElement(name = "CreationTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date creationTime;

    @XmlElement(name = "LastModifiedTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date lastModified;

    @XmlElement(name = "Priority")
    int priority = 9; // Default value 9.

    @XmlElement(name = "RunningCluster")
    String runningCluster;

    @XmlElementWrapper(name = "Tasks")
    @XmlElementRef
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

  /* Un-document */
  public void setPriority(int priority) {
    model.priority = priority;
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
      return JAXBUtils.marshal(model, JobModel.class);
    } catch (JAXBException e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }
}
