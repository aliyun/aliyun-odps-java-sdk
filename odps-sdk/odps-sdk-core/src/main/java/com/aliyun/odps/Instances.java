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

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.aliyun.odps.Instance.InstanceResultModel;
import com.aliyun.odps.Instance.InstanceResultModel.TaskResult;
import com.aliyun.odps.Instance.TaskStatusModel;
import com.aliyun.odps.Job.JobModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.options.CreateInstanceOption;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Instances表示ODPS中所有{@link Instance}的集合
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class Instances implements Iterable<Instance> {

  /* for instance listing */
  @Root(name = "Instances", strict = false)
  private static class ListInstanceResponse {

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;

    @ElementList(entry = "Instance", inline = true, required = false)
    private List<TaskStatusModel> instances = new ArrayList<TaskStatusModel>();
  }

  @Root(name = "Instances", strict = false)
  private static class ListInstanceQueueResponse {

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;

    @Element(name = "Content", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String queue;
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
    return create(job, false);
  }

  public Instance create(Job job, boolean tryWait) throws OdpsException {
    return create(getDefaultProjectName(), job, tryWait);
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
    Instance instance = new Instance(projectName, model, null, odps);
    if (id.endsWith("_mcqa")) {
      instance.setMcqaV2(true);
    }
    return instance;
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
  public Instance create(String projectName, Task task, Integer priority, String runningCluster) throws OdpsException {
    return create(projectName, task, priority, runningCluster, null);
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
   * @param jobName
   *     指定的作业名称
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance create(String projectName, Task task, Integer priority, String runningCluster,
                         String jobName) throws OdpsException {
    return create(projectName, task, priority, runningCluster, jobName, null);
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
   * @param jobName
   *     指定的作业名称
   * @param mcqaConnHeader
   *     指定 MCQA 2.0 {@link Quota#getMcqaConnHeader()}
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  public Instance create(String projectName, Task task, Integer priority, String runningCluster,
                         String jobName, String mcqaConnHeader) throws OdpsException {
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
    if (jobName != null) {
      job.setName(jobName);
    }
    return create(projectName, job, false, mcqaConnHeader);
  }

  /*
   * 表示匿名Instance的请求内容
   */
  @Root(name = "Instance", strict = false)
  private static class AnonymousInstance {

    @Element(name = "Job", required = false)
    JobModel job;
  }

  Instance create(String project, Job job) throws OdpsException {
    return create(project, job, false);
  }

  Instance create(String project, Job job, boolean tryWait) throws OdpsException {
    return create(project, job, tryWait, null);
  }

  /**
   * 使用给定的{@link Job}定义在给定的project内创建Instance
   *
   * @param project
   *     Instance运行的project
   * @param job
   *     Job定义
   * @param tryWait
   *     是否尝试等待Instance执行完成
   * @param mcqaConnHeader
   *     是否使用 MCQA 2.0, 指定 MCQA 2.0 {@link Quota#getMcqaConnHeader()}
   * @return {@link Instance}对象
   * @throws OdpsException
   */
  Instance create(String project, Job job, boolean tryWait, String mcqaConnHeader) throws OdpsException {
    if (StringUtils.isNullOrEmpty(project)) {
      throw new IllegalArgumentException("project required.");
    }
    if (job == null) {
      throw new IllegalArgumentException("Job required.");
    }
    CreateInstanceOption createInstanceOption = new CreateInstanceOption.Builder()
        .setProjectName(project)
        .setPriority(job.getPriority())
        .setRunningCluster(job.getRunningCluster())
        .setJobName(job.getName())
        .setTryWait(tryWait)
        .setMcqaConnHeader(mcqaConnHeader)
        .build();
    return create(job.getTasks(), createInstanceOption);
  }

  public Instance create(Task task, CreateInstanceOption option) throws OdpsException {
    return create(Collections.singletonList(task), option);
  }

  public Instance create(List<Task> tasks, CreateInstanceOption option) throws OdpsException {
    String project = option.getProjectName();
    if (StringUtils.isNullOrEmpty(project)) {
      project = getDefaultProjectName();
    }
    if (tasks == null || tasks.size() == 0) {
      throw new IllegalArgumentException("Tasks required.");
    }
    Job job = new Job();
    for (Task t : tasks) {
      job.addTask(t);
    }
    if (option.getPriority() != null) {
      // check priority not negative
      if (option.getPriority() < 0) {
        throw new OdpsException("Priority must more than or equal to zero.");
      }
      job.setPriority(option.getPriority());
    }
    job.setRunningCluster(option.getRunningCluster());
    if (option.getJobName() != null) {
      job.setName(option.getJobName());
    }
    if (option.getUniqueIdentifyID() != null) {
      job.setUniqueIdentifyID(option.getUniqueIdentifyID());
    }

    String guid = UUID.randomUUID().toString();

    for (Task t : job.getTasks()) {
      t.setProperty("uuid", guid);
      t.loadSystemSettings();
      t.loadGlobalSettings();
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
      xml = SimpleXmlUtils.marshal(i);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");

    HashMap<String, String> params = new HashMap<String, String>();

    String resource = ResourceBuilder.buildInstancesResource(project);
    if (StringUtils.isNotBlank(option.getMcqaConnHeader())) {
      resource = "/mcqa" + resource;
      headers.put(Headers.ODPS_MCQA_CONN, option.getMcqaConnHeader());
    }
    if (option.isTryWait()) {
      params.put("tryWait", null);
    }

    long startTime = System.currentTimeMillis();
    Response resp = null;
    // at least wait 180s when get 409
    while (System.currentTimeMillis() - startTime < TimeUnit.SECONDS.toMillis(180)) {
      try {
        resp = client.stringRequest(resource, "POST", params, headers, xml);
        break;
      } catch (OdpsException e) {
        if (e.getStatus() != null && e.getStatus() == 409 && e.existRetryAfter()) {
          try {
            Long retryAfter = e.getRetryAfter();
            if (retryAfter > 0) {
              Thread.sleep(e.getRetryAfter() * 1000);
            }
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            throw e;
          }
        } else {
          throw e;
        }
      }
    }
    // If we exit the loop without a successful response, make one last attempt
    if (resp == null) {
      resp = client.stringRequest(resource, "POST", params, headers, xml);
    }
    String location = resp.getHeaders().get(Headers.LOCATION);
    if (location == null || location.trim().length() == 0) {
      throw new OdpsException("Invalid response, Location header required.");
    }

    String instanceId = location.substring(location.lastIndexOf("/") + 1);

    Map<String, TaskResult> results = new HashMap<>();

    TaskStatusModel model = new TaskStatusModel();
    model.name = instanceId;

    if (resp.getStatus() == 200 && resp.getBody() != null
        && resp.getBody().length > 0) {
      try {
        InstanceResultModel result = SimpleXmlUtils.unmarshal(resp,
                                                         InstanceResultModel.class);
        for (TaskResult taskResult : result.taskResults) {
          model.tasks.add(createInstanceTaskModel(taskResult));
          results.put(taskResult.name, taskResult);
        }
      } catch (Exception e) {
        throw new OdpsException("Invalid create instance response.", e);
      }
    }

    Instance instance = new Instance(project, model, results, odps);

    // set mcqa 2.0 header
    if (resp.getStatus() == 201 && (resp.getHeader(Headers.ODPS_MCQA_QUERY_COOKIE) != null)) {
      instance.addUserDefinedHeaders(ImmutableMap.of(Headers.ODPS_MCQA_QUERY_COOKIE,
                                                     resp.getHeader(
                                                         Headers.ODPS_MCQA_QUERY_COOKIE)));
    }
    if (StringUtils.isNotBlank(option.getMcqaConnHeader())) {
      instance.addUserDefinedHeaders(ImmutableMap.of(Headers.ODPS_MCQA_CONN, option.getMcqaConnHeader()));
      instance.setMcqaV2(true);
    }

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


  private class InstanceQueueListIterator extends ListIterator<Instance.InstanceQueueingInfo> {
    Map<String, String> params = new HashMap<String, String>();

    InstanceFilter filter;
    String project;
    Integer maxItemsPerRequest;

    InstanceQueueListIterator(String projectName, InstanceFilter filter, Integer maxItemsPerRequest) {
      this.filter = filter;
      this.project = projectName;
      this.maxItemsPerRequest = maxItemsPerRequest;
    }

    @Override
    protected List<Instance.InstanceQueueingInfo> list() {
      if ((params.containsKey("marker")) && (params.get("marker").length() == 0)){
        return null;
      }

      if (maxItemsPerRequest != null) {
        params.put("maxitems", maxItemsPerRequest.toString());
      }

      if (filter != null) {
        if (filter.getStatus() != null) {
          params.put("status", filter.getStatus().toString());
        }

        if (filter.getOnlyOwner() != null) {
          params.put("onlyowner", filter.getOnlyOwner() ? "yes" : "no");
        }

        if (filter.getQuotaIndex() != null) {
          params.put("quotaindex", filter.getQuotaIndex());
        }
      }

      String resource = ResourceBuilder.buildCachedInstancesResource(project);
      try {
        ListInstanceQueueResponse resp = client.request(
            ListInstanceQueueResponse.class, resource, "GET", params);

        params.put("marker", resp.marker);

        List<Instance.InstanceQueueingInfo> result = new ArrayList<Instance.InstanceQueueingInfo>();
        if (resp.queue != null) {
          for (JsonElement item : new JsonParser().parse(resp.queue).getAsJsonArray()) {
            result.add(new Instance.InstanceQueueingInfo(item.getAsJsonObject()));
          }
        }
        return result;
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  /**
   * 获取运行队列中的所有 instance 的排队信息
   *
   *
   * @return 迭代器
   */
  public Iterator<Instance.InstanceQueueingInfo> iteratorQueueing() {
    return iteratorQueueing(getDefaultProjectName());
  }


  /**
   * 获取运行队列中的所有 instance 的排队信息
   *
   * @param project
   *          项目名称
   *
   * @return 迭代器
   */
  public Iterator<Instance.InstanceQueueingInfo> iteratorQueueing(String project) {
    return iteratorQueueing(project, null);
  }

  /**
   * 获取运行队列中的所有 instance 的排队信息
   *
   * @param filter
   *          过滤器
   *
   * @return 迭代器
   */
  public Iterator<Instance.InstanceQueueingInfo> iteratorQueueing(InstanceFilter filter) {
    return iteratorQueueing(getDefaultProjectName(), filter);
  }

  /**
   * 获取运行队列中的所有 instance 的排队信息
   *
   * @param project
   *          项目名称
   * @param filter
   *          过滤器
   *
   * @return 迭代器
   */
  public Iterator<Instance.InstanceQueueingInfo> iteratorQueueing(String project, InstanceFilter filter) {
    return iteratorQueueing(project, filter, null);
  }

  /**
   * 获取运行队列中的所有 instance 的排队信息
   *
   * @param project
   *          项目名称
   * @param filter
   *          过滤器
   * @param maxItemsPerRequest
   *          每次request取到的Instance的数量上限, 默认为1000
   *
   * @return 迭代器
   */
  public Iterator<Instance.InstanceQueueingInfo> iteratorQueueing(
      String project, InstanceFilter filter, Integer maxItemsPerRequest) {
    return new InstanceQueueListIterator(project, filter, maxItemsPerRequest);
  }
}
