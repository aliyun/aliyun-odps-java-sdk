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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.Instance.InstanceResultModel;
import com.aliyun.odps.Instance.InstanceResultModel.TaskResult;
import com.aliyun.odps.Instance.TaskStatusModel;
import com.aliyun.odps.Job.JobModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * Instances表示ODPS中所有{@link Instance}的集合
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class Instances implements Iterable<Instance> {

  /* for instance listing */
  @XmlRootElement(name = "Instances")
  private static class ListInstanceResponse {

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;

    @XmlElement(name = "Instance")
    private List<TaskStatusModel> instances = new ArrayList<TaskStatusModel>();

  }

  private Odps odps;
  private RestClient client;

  /**
   * 获取默认 runningCluster
   * 如果为 null,使用project 默认 cluster
   *
   * @return
   */
  public String getDefaultRunningCluster() {
    return defaultRunningCluster;
  }

  /**
   * 设置默认 runningCluster
   * 如果为 null,使用project 默认 cluster
   *
   * @param defaultRunningCluster
   */
  public void setDefaultRunningCluster(String defaultRunningCluster) {
    this.defaultRunningCluster = defaultRunningCluster;
  }

  private String defaultRunningCluster;

  Instances(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * 为给定的{@link Job}定义在默认{@link Project}中创建{@link Instance}
   *
   * @param job
   *     {@link Job}定义
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance create(Job job) throws OdpsException {
    return create(getDefaultProjectName(), job);
  }

  /**
   * 为给定的{@link Task}创建{@link Instance}
   *
   * @param task
   *     {@link Task}对象
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance create(Task task) throws OdpsException {
    return create(getDefaultProjectName(), task);
  }

  /**
   * 为给定的{@link Task}创建Instance
   *
   * @param task
   *     {@link Task}
   * @param priority
   *     指定优先级
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance create(Task task, int priority) throws OdpsException {
    return create(getDefaultProjectName(), task, priority);
  }

  /**
   * 为给定的{@link Task}创建Instance
   *
   * @param task
   *     {@link Task}
   * @param priority
   *     指定优先级
   * @param runningCluster
   *     指定的计算集群
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance create(Task task, int priority, String runningCluster) throws OdpsException {
    return create(getDefaultProjectName(), task, priority, runningCluster);
  }

  /**
   * 获取指定Instance
   *
   * @param id
   *     Instance ID
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance get(String id) {
    return get(getDefaultProjectName(), id);
  }

  /**
   * 获取指定Instance
   *
   * @param projectName
   *     {@link Instance}所在的{@link Project}名称
   * @param id
   *     Instance ID
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance get(String projectName, String id) {
    TaskStatusModel model = new TaskStatusModel();
    model.name = id;
    return new Instance(projectName, model, null, odps);
  }


  /**
   * 判断指定 Instance 是否存在
   *
   * @param id
   *     Instance ID
   * @return 存在返回true, 否则返回false
   * @throws OdpsException
   */
  public boolean exists(String id) throws OdpsException {
    return exists(getDefaultProjectName(), id);
  }

  /**
   * 判断指定 Instance 是否存在
   *
   * @param projectName
   *     所在{@link Project}名称
   * @param id
   *     Instance ID
   * @return 存在返回true, 否则返回flase
   * @throws OdpsException
   */
  public boolean exists(String projectName, String id) throws OdpsException {
    try {
      Instance i = get(projectName, id);
      i.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  /**
   * 为给定的{@link Task}创建Instance
   *
   * @param projectName
   *     指定Instance所在的Project名称
   * @param task
   *     {@link Task}对象
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance create(String projectName, Task task) throws OdpsException {
    return create(projectName, task, null, null);
  }

  /**
   * 为给定的{@link Task}创建Instance
   *
   * @param projectName
   *     Instance运行的Project名称
   * @param task
   *     {@link Task}对象
   * @param priority
   *     指定的优先级
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance create(String projectName, Task task, int priority)
      throws OdpsException {
    return create(projectName, task, priority, defaultRunningCluster);
  }

  /**
   * 为给定的{@link Task}创建Instance
   *
   * @param projectName
   *     Instance运行的Project名称
   * @param task
   *     {@link Task}对象
   * @param priority
   *     指定的优先级
   * @param runningCluster
   *     指定的计算集群
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance create(String projectName, Task task, int priority, String runningCluster) throws OdpsException {
    return create(projectName, task, new Integer(priority), runningCluster);
  }

  public Instance create(String projectName, Task task, Integer priority, String runningCluster) throws OdpsException {
    Job job = new Job();
    job.addTask(task);
    if (priority != null) {
      // check priority not negative
      if (priority < 0) {
        throw new OdpsException("Priority must more than or equal to zero.");
      }
      job.setPriority(priority);
    }
    job.setRunningCluster(runningCluster);

    return create(projectName, job);
  }

  /*
   * 表示匿名Instance的请求内容
   */
  @XmlRootElement(name = "Instance")
  private static class AnonymousInstance {

    @XmlElement(name = "Job")
    JobModel job;
  }

  /**
   * 使用给定的{@link Job}定义在给定的project内创建Instance
   *
   * @param project
   *     Instance运行的project
   * @param job
   *     Job定义
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  Instance create(String project, Job job) throws OdpsException {
    if (project == null) {
      throw new IllegalArgumentException("project required.");
    }

    if (job == null) {
      throw new IllegalArgumentException("Job required.");
    }

    if (job.getTasks().size() == 0) {
      throw new IllegalArgumentException("Tasks required.");
    }

    String guid = UUID.randomUUID().toString();

    for (Task t : job.getTasks()) {
      t.setProperty("uuid", guid);
      if (t.getName() == null) {
        throw new OdpsException("Task name required.");
      }
    }

    OdpsHooks hooks = null;

    if (OdpsHooks.isEnabled()) {
      hooks = new OdpsHooks();
      hooks.before(job, odps);
    }

    AnonymousInstance i = new AnonymousInstance();
    i.job = job.model;
    String xml = null;
    try {
      xml = JAXBUtils.marshal(i, AnonymousInstance.class);
    } catch (JAXBException e) {
      throw new OdpsException(e.getMessage(), e);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    String resource = ResourceBuilder.buildInstancesResource(project);
    Response resp = client.stringRequest(resource, "POST", null, headers, xml);

    String location = resp.getHeaders().get(Headers.LOCATION);
    if (location == null || location.trim().length() == 0) {
      throw new OdpsException("Invalid response, Location header required.");
    }

    String instanceId = location.substring(location.lastIndexOf("/") + 1);

    Map<String, Instance.Result> results = new HashMap<String, Instance.Result>();

    TaskStatusModel model = new TaskStatusModel();
    model.name = instanceId;

    if (resp.getStatus() == 200 && resp.getBody() != null
        && resp.getBody().length > 0) {
      try {
        InstanceResultModel result = JAXBUtils.unmarshal(resp,
                                                         InstanceResultModel.class);
        for (TaskResult taskResult : result.taskResults) {
          model.tasks.add(createInstanceTaskModel(taskResult));
          results.put(taskResult.name, taskResult.result);
        }
      } catch (JAXBException e) {
        throw new OdpsException("Invalid create instance response.", e);
      }
    }

    Instance instance = new Instance(project, model, results, odps);

    instance.setOdpsHooks(hooks);

    if (OdpsHooks.isEnabled()) {
      if (hooks == null) {
        hooks = new OdpsHooks();
      }
      hooks.onInstanceCreated(instance, odps);
    }

    return instance;
  }

  private TaskStatusModel.InstanceTaskModel createInstanceTaskModel(
      TaskResult taskResult) {
    TaskStatusModel.InstanceTaskModel taskModel =  new TaskStatusModel.InstanceTaskModel();
    taskModel.name = taskResult.name;
    taskModel.status = taskResult.status;
    taskModel.type = taskResult.type;

    return taskModel;
  }

  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }

  /**
   * 获得Instance的迭代器
   *
   * @return {@link Instance}的迭代器
   */
  public Iterator<Instance> iterator() {
    return iterator(getDefaultProjectName());
  }

  /**
   * 获得指定Project下，Instance迭代器
   *
   * @param project
   *     Project名称
   * @return {@link Instance}的迭代器
   */
  public Iterator<Instance> iterator(final String project) {
    return iterator(project, null);
  }

  /**
   * 获得{@link Instance}迭代器
   *
   * @param filter
   *     {@link InstanceFilter}过滤器
   * @return {@link Instance}的迭代器
   */
  public Iterator<Instance> iterator(final InstanceFilter filter) {
    return iterator(getDefaultProjectName(), filter);
  }

  /**
   * 获得指定{@link Project}下，{@link Instance}迭代器
   *
   * @param project
   *     {@link Project}名称
   * @param filter
   *     {@link InstanceFilter}过滤器
   * @return {@link Instance}迭代器
   */
  public Iterator<Instance> iterator(final String project,
                                     final InstanceFilter filter) {
    return new InstanceListIterator(project, filter);
  }



  /**
   * 获得Instance的 iterable 迭代器
   *
   * @return {@link Instance}的 iterable 迭代器
   */
  public Iterable<Instance> iterable() {
    return iterable(getDefaultProjectName());
  }

  /**
   * 获得指定Project下，Instance iterable迭代器
   *
   * @param project
   *     Project名称
   * @return {@link Instance}的 iterable 迭代器
   */
  public Iterable<Instance> iterable(final String project) {
    return iterable(project, null);
  }

  /**
   * 获得{@link Instance} iterable 迭代器
   *
   * @param filter
   *     {@link InstanceFilter}过滤器
   * @return {@link Instance}的 iterable 迭代器
   */
  public Iterable<Instance> iterable(final InstanceFilter filter) {
    return iterable(getDefaultProjectName(), filter);
  }

  /**
   * 获得指定{@link Project}下，{@link Instance} iterable 迭代器
   *
   * @param project
   *     {@link Project}名称
   * @param filter
   *     {@link InstanceFilter}过滤器
   * @return {@link Instance} iterable 迭代器
   */
  public Iterable<Instance> iterable(final String project,
                                     final InstanceFilter filter) {
    return new Iterable<Instance>() {
      @Override
      public Iterator<Instance> iterator() {
        return new InstanceListIterator(project, filter);
      }
    };
  }

  private class InstanceListIterator extends ListIterator<Instance> {
    Map<String, String> params = new HashMap<String, String>();

    InstanceFilter filter;
    String project;

    InstanceListIterator(String projectName, InstanceFilter filter) {
      this.filter = filter;
      this.project = projectName;
    }

    @Override
    protected List<Instance> list() {
      ArrayList<Instance> instances = new ArrayList<Instance>();

      String lastMarker = params.get("marker");
      if (params.containsKey("marker") && lastMarker.length() == 0) {
        return null;
      }

      if (filter != null) {
        if (filter.getStatus() != null) {
          params.put("status", filter.getStatus().toString());
        }

        StringBuilder range = new StringBuilder();
        Date from = filter.getFromTime();
        Date end = filter.getEndTime();

        if (from != null && end != null) {
          if (from.getTime() / 1000 >= end.getTime() / 1000) {
            throw new IllegalArgumentException(
                "invalid query range, end value must be greater than begin value and they could not be in the same second!");
          }
        }

        if (from != null) {
          range.append(from.getTime() / 1000);
        }

        if (from != null || end != null) {
          range.append(':');
        }

        if (end != null) {
          range.append(end.getTime() / 1000);
        }

        if (range.length() > 0) {
          params.put("daterange", range.toString());
        }

        if (filter.getOnlyOwner() != null) {
          params.put("onlyowner", filter.getOnlyOwner() ? "yes" : "no");
        }
      }

      String resource = ResourceBuilder.buildInstancesResource(project);
      try {

        ListInstanceResponse resp = client.request(
            ListInstanceResponse.class, resource, "GET", params);

        for (TaskStatusModel model : resp.instances) {
          Instance t = new Instance(project, model, null, odps);
          instances.add(t);
        }

        params.put("marker", resp.marker);
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      return instances;

    }
  }
}
