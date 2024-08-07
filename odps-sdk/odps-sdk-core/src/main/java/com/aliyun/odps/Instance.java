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
import com.aliyun.odps.simpleframework.xml.Attribute;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.Text;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.aliyun.odps.utils.GsonObjectBuilder;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;

import com.aliyun.odps.Instance.InstanceResultModel.TaskResult;
import com.aliyun.odps.Instance.TaskStatusModel.InstanceTaskModel;
import com.aliyun.odps.Job.JobModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;

/**
 * Instance表示ODPS中计算任务的一个运行实例
 */
public class Instance extends com.aliyun.odps.LazyLoad {

  /**
   * Instance的运行状态。
   *
   * @author xiaoming.yin
   */
  public static enum Status {

    /**
     * 运行
     */
    RUNNING("Running"),

    /**
     * 挂起
     */
    SUSPENDED("Suspended"),

    /**
     * 终止
     */
    TERMINATED("Terminated");

    private static final Map<String, Status> strToEnum = new HashMap<String, Status>();

    static { // Initialize the map
      for (Status t : values()) {
        strToEnum.put(t.toString(), t);
      }
    }

    private String strVal;

    private Status(String strVal) {
      this.strVal = strVal;
    }

    // package-private only.
    static Status fromString(String value) {
      if (value == null) {
        throw new NullPointerException();
      }
      return strToEnum.get(value);
    }

    /**
     * RUNNING 被转换成 "Running"，SUSPENDED 被转换成 "Suspended"，TERMINATED 被转换成
     * "Terminated"。
     */
    @Override
    public String toString() {
      return this.strVal;
    }
  }

  public class SetInformationResult {
    public String result;
    public String status;
  }

  private String project;
  private Map<String, Result> results;
  private boolean isSync = false;

  private TaskStatusModel model;

  private Status status;

  private RestClient client;

  private Odps odps;

  private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

  Instance(String project, TaskStatusModel model, Map<String, Result> results, Odps odps) {
    this.project = project;
    this.model = model;
    this.results = results;

    if (model.status != null) {
      this.status = Status.valueOf(model.status.toUpperCase());
    }

    if (results != null && results.size() > 0) {
      isSync = true;
      status = Status.TERMINATED;
    }

    this.odps = odps;
    this.client = odps.getRestClient();
  }

  @Root(name = "Instance", strict = false)
  private static class InstanceStatusModel {

    @Element(name = "Status", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String status;
  }

  @Root(strict = false)
  private static class InstanceDebugModel {

    @Element(name = "LogId", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String logId;

    @Element(name = "DebugId", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String debugId;
  }

  /**
   * 表示Instance运行结果的API响应内容
   *
   * @author shenggong.wang@alibaba-inc.com
   */
  @Root(name = "Instance", strict = false)
  public static class InstanceResultModel {

    @Root(strict = false)
    public static class TaskResult {

      @Attribute(name = "Type", required = false)
      String type;

      @Element(name = "Name", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      String name;

      @Element(name = "Result", required = false)
      Result result;

      @Element(name = "Status", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      String status;

      public String getType() {
        return type;
      }

      public String getName() {
        return name;
      }

      public Result getResult() {
        return result;
      }

      public String getStatus() {
        return status;
      }
    }

    @ElementList(name = "Tasks", entry = "Task", required = false)
    List<TaskResult> taskResults = new ArrayList<TaskResult>();
  }

  @Root(strict = false)
  public static class Result {

    @Attribute(name = "Transform", required = false)
    String transform;

    @Attribute(name = "Format", required = false)
    String format;

    @Text(required = false)
    String text = "";

    /**
     * {@link Instance}执行后，无论成功与否，返回给客户端的结果或失败信息。
     */
    public String getString() {
      if (transform != null && "Base64".equals(transform)) {
        try {
          String decodedString = new String(Base64.decodeBase64(text), "UTF-8");
          return decodedString;
        } catch (Exception e) {
          // return original text
          return text;
        }
      } else {
        return text;
      }
    }

    /**
     * 指明返回结果的格式，包括：text, csv
     *
     * @return 格式信息包括："text"，"csv"
     */
    public String getFormat() {
      return format;
    }
  }

  /**
   * setTaskInfo指定的KV内容
   *
   * @author dejun.xiedj@alibaba-inc.com
   */
  @Root(name = "Instance", strict = false)
  static class InstanceTaskInfoModel {

    @Element(name = "Key", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String key;

    @Element(name = "Value", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String value;
  }

  @Override
  public void reload() throws OdpsException {
    reload(false);
  }

  private void reload(boolean isBlock) throws OdpsException {
    Map<String, String> params = null;

    if (isBlock) {
      params = new HashMap<String, String>();
      params.put("instancestatus", null);
    }

    Response resp = client.request(getResource(), "GET", params, null, null);
    model.owner = resp.getHeaders().get(Headers.ODPS_OWNER);
    String startTimeStr = resp.getHeaders().get(Headers.ODPS_START_TIME);
    String endTimeStr = resp.getHeaders().get(Headers.ODPS_END_TIME);
    try {
      model.startTime = DateUtils.parseRfc822Date(startTimeStr);
    } catch (ParseException e) {
      throw new OdpsException("Invalid response, x-odps-start-time:" + startTimeStr);
    }

    try {
      model.endTime = DateUtils.parseRfc822Date(endTimeStr);
    } catch (ParseException e) {
      // nothing
    }

    try {
      InstanceStatusModel sm = SimpleXmlUtils.unmarshal(resp, InstanceStatusModel.class);
      status = Status.valueOf(sm.status.toUpperCase());
    } catch (Exception e) {
      throw new OdpsException("Invalid instance status response.", e);
    }

    // do not set load flat to true
  }

  /**
   * 停止正在执行的Instance
   * 注意：停止动作为异步执行，如果需要确保 Instance 停止，可以在调用本方法后轮询 Instance.isTerminated() 直至返回 true
   *
   * @throws OdpsException
   */
  public void stop() throws OdpsException {
    InstanceStatusModel sm = new InstanceStatusModel();
    sm.status = "Terminated";
    try {
      String ret = SimpleXmlUtils.marshal(sm);
      HashMap<String, String> headers = new HashMap<String, String>();
      headers.put(Headers.CONTENT_TYPE, "application/xml");
      client.stringRequest(getResource(), "PUT", null, headers, ret);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  /**
   * 获得Instance中{@link Task}的运行结果
   *
   * @return {@link Task}的运行结果, key为{@link Task}的名称，value为{@link Result}
   * @throws OdpsException
   */
  public Map<String, Result> getTaskResultsWithFormat() throws OdpsException {
    if (isSync) {
      return results;
    }
    results = new HashMap<>();
    for (TaskResult r : getRawTaskResults()) {
      results.put(r.name, r.result);
    }
    return results;
  }

  /**
   * 获得Instance中Task的运行结果
   *
   * @return {@link Task}的运行结果, key为{@link Task}的名称，value为{@link Result}
   * .getString()的结果。
   * @throws OdpsException
   */
  public Map<String, String> getTaskResults() throws OdpsException {
    Map<String, Result> results = getTaskResultsWithFormat();
    Map<String, String> result = new HashMap<String, String>();
    for (Entry<String, Result> entry : results.entrySet()) {
      result.put(entry.getKey(), entry.getValue().getString());
    }
    return result;
  }

  /**
   * 获取Instance中Task的原始运行结果
   *
   * @return {@link TaskResult}列表
   * @throws OdpsException
   */
  public List<TaskResult> getRawTaskResults() throws OdpsException {
    Map<String, String> params = new HashMap<>();
    params.put("result", null);
    InstanceResultModel instanceResult = client
        .request(InstanceResultModel.class, getResource(), "GET", params);

    return instanceResult.taskResults;
  }

  /**
   * 获取SQLRT Instance中Subquery的原始运行结果
   *
   * @return {@link TaskResult}列表
   * @throws OdpsException
   */
  public String getRawSubqueryResults(String subqueryId, String taskName) throws OdpsException {
    Map<String, String> params = new HashMap<>();
    params.put("result", null);
    params.put("subquery_id", subqueryId);
    InstanceResultModel instanceResult = client
        .request(InstanceResultModel.class, getResource(), "GET", params);

    results = new HashMap<>();
    for (TaskResult r : instanceResult.taskResults) {
      results.put(r.name, r.result);
    }
    taskName = taskName + "_" + subqueryId;
    String ret;
    try {
      ret = results.get(taskName).getString();
    } catch (NullPointerException e)
    {
      throw new OdpsException("Task result not found, please run query again.");
    }
    return ret;
  }

  public static class TaskCost {

    public Integer getCPUCost() {
      return cpuCost;
    }

    public void setCPUCost(Integer cpuCost) {
      this.cpuCost = cpuCost;
    }

    public Integer getMemoryCost() {
      return memoryCost;
    }

    public void setMemoryCost(Integer memoryCost) {
      this.memoryCost = memoryCost;
    }

    public Integer getInputSize() {
      return inputSize;
    }

    public void setInputSize(Integer inputSize) {
      this.inputSize = inputSize;
    }

    private Integer cpuCost = 0;  // 100core*second
    private Integer memoryCost = 0; // MB*second
    private Integer inputSize = 0; // bytes
  }

  /**
   * TaskSummary包含{@link Task}运行结束后的汇总信息
   *
   * <p>
   * Summary信息以key-value对的形式组织
   * </p>
   *
   * @author shenggong.wang@alibaba-inc.com
   */
  @SuppressWarnings({"serial", "rawtypes"})
  public static class TaskSummary extends HashMap {

    private String text;

    private String jsonSummary = "{}"; // An emtpy JSON string by default

    /**
     * 返回文本化汇总信息，类似于：
     *
     * <pre>
     * Summary:
     * inputs:
     *         my_project.wc_in: 1 (269 bytes)
     * outputs:
     *         my_project.wc_out: 2 (253 bytes)
     * M1_Stg1:
     *         worker count: 1
     *         input records:
     *                 input: 1  (min: 1, max: 1, avg: 1)
     *         output records:
     *                 R2_1_Stg1: 2  (min: 2, max: 2, avg: 2)
     * R2_1_Stg1:
     *         worker count: 1
     *         input records:
     *                 input: 2  (min: 2, max: 2, avg: 2)
     *         output records:
     *                 R2_1_Stg1FS_6415: 2  (min: 2, max: 2, avg: 2)
     * </pre>
     *
     * @return 汇总信息
     */
    public String getSummaryText() {
      return text;
    }

    void setSummaryText(String text) {
      this.text = text;
    }

    public String getJsonSummary() {
      return this.jsonSummary;
    }

    void setJsonSummary(String jsonSummary) {
      this.jsonSummary = jsonSummary;
    }
  }

  /**
   * 获得 Instance 中 Task 的运行资源消耗信息
   * 当且仅当 Task 产生计量信息时，返回资源消耗信息，否则返回 null
   *
   * @param taskName
   *     指定的TaskName
   * @return 资源消耗信息 {@link com.aliyun.odps.Instance.TaskCost}
   * @throws OdpsException
   */
  public TaskCost getTaskCost(String taskName) throws OdpsException {
    TaskSummary summary = getTaskSummary(taskName);
    if (summary == null) {
      return null;
    }

    try {
      if (summary.get("Cost") != null) {
        Map<String, Long> taskCostMap = (Map) summary.get("Cost");

        TaskCost cost = new TaskCost();

        if (taskCostMap.get("CPU") != null) {
          cost.setCPUCost(taskCostMap.get("CPU").intValue());
        }

        if (taskCostMap.get("Memory") != null) {
          cost.setMemoryCost(taskCostMap.get("Memory").intValue());
        }

        if (taskCostMap.get("Input") != null) {
          cost.setInputSize(taskCostMap.get("Input").intValue());
        }

        return cost;
      }
    } catch (Exception e) {
      // ignore
    }

    return null;
  }

  /**
   * 获得 Instance 中 Task 运行时的指定相关信息
   *
   * @param taskName
   *     指定的TaskName
   * @param infoKey
   *     指定相关信息的标志符
   * @return 相关信息
   * @throws OdpsException
   */
  public String getTaskInfo(String taskName, String infoKey) throws OdpsException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("info", null);
    params.put("taskname", taskName);
    params.put("key", infoKey);

    Response result = client.request(getResource(), "GET", params, null, null);
    try {
      return new String(result.getBody(), "utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException(e);
    }
  }

  /**
   * 设置 Instance 中 Task 运行时的指定相关信息
   *
   * @param taskName
   *     指定的TaskName
   * @param infoKey
   *     指定相关信息的标志符
   * @param infoValue
   *     指定相关信息的内容
   * @return 相关信息
   * @throws OdpsException
   */
  @Deprecated
  public String setTaskInfo(String taskName, String infoKey, String infoValue) throws OdpsException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("info", null);
    params.put("taskname", taskName);

    InstanceTaskInfoModel sm = new InstanceTaskInfoModel();
    sm.key = infoKey;
    sm.value = infoValue;
    try {
      String kv = SimpleXmlUtils.marshal(sm);
      HashMap<String, String> headers = new HashMap<String, String>();
      headers.put(Headers.CONTENT_TYPE, "application/xml");
      Response result = client.stringRequest(getResource(), "PUT", params, headers, kv);
      return new String(result.getBody(), "utf-8");
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  /**
   * 设置 Instance 中 Task 运行时的指定相关信息,返回结构化response
   *
   * @param taskName
   *     指定的TaskName
   * @param infoKey
   *     指定相关信息的标志符
   * @param infoValue
   *     指定相关信息的内容
   * @return SetInformationResult 相关信息及其状态
   * @throws OdpsException
   */
  public SetInformationResult setInformation(String taskName, String infoKey, String infoValue) throws OdpsException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("info", null);
    params.put("taskname", taskName);

    InstanceTaskInfoModel sm = new InstanceTaskInfoModel();
    sm.key = infoKey;
    sm.value = infoValue;
    Response result = null;
    try {
      String kv = SimpleXmlUtils.marshal(sm);
      HashMap<String, String> headers = new HashMap<String, String>();
      headers.put(Headers.CONTENT_TYPE, "application/xml");
      result = client.stringRequest(getResource(), "PUT", params, headers, kv);
      Type type = new TypeToken<SetInformationResult>(){}.getType();
      SetInformationResult setInformationResult = gson.fromJson(new String(result.getBody(), "utf-8"), type);
      if (setInformationResult == null) {
        // invalid response
        throw new OdpsException("Parse response json failed:"
            + new String(result.getBody()));
      }
      return setInformationResult;
    } catch (JsonParseException e) {
      throw new OdpsException("Parse response json failed, body:"
          + new String(result.getBody()) + " Error:" + e.getMessage(), e);
    } catch(Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }
  /**
   * 获得Instance中Task的运行汇总信息 当 Server 端返回信息，但是信息格式错误时，返回 null
   *
   * @param taskName
   *     指定的TaskName
   * @return 汇总信息 {@link TaskSummary}
   * @throws OdpsException
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public TaskSummary getTaskSummary(String taskName) throws OdpsException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("instancesummary", null);
    params.put("taskname", taskName);
    Response result = client.request(getResource(), "GET", params, null, null);

    TaskSummary summary = null;
    try {
      Gson gson = GsonObjectBuilder.get();
      Map<Object, Object> map = gson
              .fromJson(new String(result.getBody()),
              new TypeToken<Map<Object, Object>>() {}.getType());
      if (map.get("Instance") != null) {
        Map mapReduce = (Map) map.get("Instance");
        String jsonSummary = (String) mapReduce.get("JsonSummary");
        if (jsonSummary != null) {
          summary = gson.fromJson(jsonSummary, TaskSummary.class);
        }
        if (summary != null) {
          summary.setSummaryText((String) mapReduce.get("Summary"));
          summary.setJsonSummary(jsonSummary);
        }
      }
    } catch (Exception e) {
      return null;
    }
    return summary;
  }

  /* Response of get instance task status8 */
  @Root(name = "Instance", strict = false)
  static class TaskStatusModel {
    @Root(strict = false)
    static class InstanceTaskModel {

      @Element(name = "Name", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      String name;

      @Attribute(name = "Type", required = false)
      String type;

      @Element(name = "StartTime", required = false)
      @Convert(SimpleXmlUtils.DateConverter.class)
      Date startTime;

      @Element(name = "EndTime", required = false)
      @Convert(SimpleXmlUtils.DateConverter.class)
      Date endTime;

      @Element(name = "Status", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      String status;

      @ElementList(name = "Histories", entry = "History", required = false)
      List<InstanceTaskModel> histories = new ArrayList<InstanceTaskModel>();
    }

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "StartTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date startTime;

    @Element(name = "EndTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date endTime;

    @Element(name = "Status", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String status;

    @ElementList(name = "Tasks", entry = "Task", required = false)
    List<InstanceTaskModel> tasks = new ArrayList<InstanceTaskModel>();
  }

  /**
   * Task执行状态
   */
  public static class TaskStatus {

    /**
     * Instance中Task的状态
     *
     * @author shenggong.wang@alibaba-inc.com
     */
    public static enum Status {
      /**
       * 等待
       */
      WAITING,

      /**
       * 运行中
       */
      RUNNING,

      /**
       * 执行成功
       */
      SUCCESS,

      /**
       * 执行失败
       */
      FAILED,

      /**
       * 被挂起
       */
      SUSPENDED,

      /**
       * 被取消
       */
      CANCELLED
    }

    private InstanceTaskModel model;

    TaskStatus(InstanceTaskModel model) {
      this.model = model;
    }

    /**
     * 返回状态名称
     *
     * @return 状态名称
     */
    public String getName() {
      return model.name;
    }

    /**
     * 返回{@link Task}类型
     *
     * @return 类型，包括：SQL，GRAPH，XLIB
     */
    public String getType() {
      return model.type;
    }

    /**
     * 返回状态对象
     *
     * @return 状态对象 {@link Status}
     */
    public Status getStatus() {
      return TaskStatus.Status.valueOf(model.status.toUpperCase());
    }
  }

  /**
   * 查询Instance中所有{@link Task}的执行状态
   *
   * @return Map, key为Task名称, value为表示Task执行状态的{@link TaskStatus}对象
   * @throws OdpsException
   */
  public Map<String, TaskStatus> getTaskStatus() throws OdpsException {

    HashMap<String, TaskStatus> taskStatus = new HashMap<String, Instance.TaskStatus>();

    // the model has enough values for task status in sync instance
    TaskStatusModel taskStatusModel = model;

    if (!isSync || !hasTaskStatus(taskStatusModel)) {
      // async instance get task status directly
      // task status in sync instance model may be null, get task status to keep compatibility
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("taskstatus", null);

      // cannot assign to model, the resp below do not has such field such as model.name and so on
      taskStatusModel = client.request(TaskStatusModel.class, getResource(), "GET", params);
    }

    for (InstanceTaskModel taskModel : taskStatusModel.tasks) {
      TaskStatus status = new TaskStatus(taskModel);
      taskStatus.put(status.getName(), status);
    }

    return taskStatus;
  }

  private boolean hasTaskStatus(TaskStatusModel model) {
    for (InstanceTaskModel taskModel : model.tasks) {
      TaskStatus status = new TaskStatus(taskModel);
      if (status.model.status == null) {
        return false;
      }
    }

    return true;
  }

  /**
   * 获得Instance所包含的所有{@link Task}的名称
   *
   * @return {@link Task}的名称集合
   * @throws OdpsException
   */
  public Set<String> getTaskNames() throws OdpsException {
    return getTaskStatus().keySet();
  }

  /**
   * 检查{@link Instance}是否执行成功
   *
   * <p>
   * 成功的含义为Instance执行结束,并且所有Task的状态为成功
   * </p>
   *
   * @return true如果成功
   * @throws OdpsException
   */
  public boolean isSuccessful() throws OdpsException {
    boolean r = true;

    Map<String, TaskStatus> status = getTaskStatus();
    for (TaskStatus s : status.values()) {
      if (s.getStatus() != TaskStatus.Status.SUCCESS) {
        r = false;
        break;
      }
    }

    return r;
  }

  /**
   * 阻塞当前线程, 直到Instance结束
   *
   * @throws OdpsException
   *     Instance失败
   */
  public void waitForSuccess() throws OdpsException {
    waitForSuccess(1000);
  }

  /**
   * 阻塞当前线程, 直到Instance结束
   *
   * @param interval
   *     内部轮询间隔
   * @throws OdpsException
   *     Instance失败
   */
  public void waitForSuccess(long interval) throws OdpsException {
    waitForTerminated(interval, false);

    if (!isSuccessful()) {
      for (Entry<String, TaskStatus> e : getTaskStatus().entrySet()) {
        if (e.getValue().getStatus() == TaskStatus.Status.FAILED) {
          throw new OdpsException(getTaskResults().get(e.getKey()));
        } else if (e.getValue().getStatus() != TaskStatus.Status.SUCCESS) {
          throw new OdpsException(e.getKey() + ", Status=" + e.getValue().getStatus());
        }
      }
    }
  }

  /**
   * 阻塞当前线程, 直到Instance结束。不检查 Instance 状态。
   *
   * @param interval
   *     内部轮询间隔
   * @param isBlock
   *     是否阻塞,
   *     不开启 block，将在客户端长轮询，直到作业结束
   *     开启 block 请求将会在服务端等待一段时间（每次请求等待5s，直到作业结束。long-polling），
   *     block 模式能够跳过 instance post running 阶段（key-path-end optimize）
   */
  public void waitForTerminated(long interval, boolean isBlock) {
    while (getStatus(isBlock) != Instance.Status.TERMINATED) {
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  /**
   * 直接获取作业结果，无需调用 waitForSuccess 方法
   * 该方法仅适用于离线作业（1个Instance对应1个Task）
   */
  public String waitForTerminatedAndGetResult() throws OdpsException {
    waitForTerminated(1000, true);

    TaskResult taskResult = getRawTaskResults().get(0);
    String resultStr = taskResult.getResult().getString();

    TaskStatus.Status taskStatus = TaskStatus.Status.SUCCESS;
    if (!StringUtils.isNullOrEmpty(taskResult.getStatus())) {
      taskStatus = TaskStatus.Status.valueOf(taskResult.getStatus().toUpperCase());
    } else {
      Map<String, TaskStatus> taskStatuses = getTaskStatus();
      for (Entry<String, TaskStatus> e : taskStatuses.entrySet()) {
        taskStatus = e.getValue().getStatus();
      }
    }
    if (taskStatus == TaskStatus.Status.FAILED) {
      throw new OdpsException(resultStr);
    } else if (taskStatus != TaskStatus.Status.SUCCESS) {
      throw new OdpsException(taskResult.getName() + ", Status=" + taskStatus + ", Result=" + resultStr);
    }
    return resultStr;
  }

  // SysTask

  /**
   * StageProgress表示{@link Task}执行过程中各阶段的进度统计
   */
  @Root(strict = false)
  public static class StageProgress {


    public static enum Status {
      READY,
      WAITING,
      RUNNING,
      SUSPENDED,
      FAILED,
      TERMINATED,
      CANCELLED,
      CANCELLING,
    }

    @Attribute(name = "ID", required = false)
    String name;

    @Element(name = "Status", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String status;

    @Element(name = "BackupWorkers", required = false)
    int backupSysInstances;

    @Element(name = "TerminatedWorkers", required = false)
    int terminatedSysInstances;

    @Element(name = "RunningWorkers", required = false)
    int runningSysInstances;

    @Element(name = "TotalWorkers", required = false)
    int totalSysInstances;

    @Element(name = "InputRecords", required = false)
    long inputRecords;

    @Element(name = "OutputRecords", required = false)
    long outputRecords;

    @Element(name = "FinishedPercentage", required = false)
    int finishedPercentage;

    public Status getStatus() {
      if (status == null) {
        return null;
      }
      return Status.valueOf(status.toUpperCase());
    }

    /**
     * 获得backup的Worker数，对应于original worker，backup会在长尾时被启动，同时运行。
     *
     * @return Worker数
     */
    public int getBackupWorkers() {
      return backupSysInstances;
    }

    /**
     * 获得正在运行的Worker数。多个Worker构成一个Stage，Worker可以简单的被看做一个进程。
     *
     * @return Worker数
     */
    public int getRunningWorkers() {
      return runningSysInstances;
    }

    /**
     * 获得Stage名称。任意{@link Task}
     * 可以由任意多个Stage构成。Stage由任意多个Worker构成。Stage内的Worker有相同的数据处理逻辑。
     *
     * @return Stage名称
     */
    public String getName() {
      return name;
    }

    /**
     * 获得结束运行的Worker数
     *
     * @return 结束运行的Worker数
     */
    public int getTerminatedWorkers() {
      return terminatedSysInstances;
    }

    /**
     * 获得所有Worker数
     *
     * @return 所有Worker数
     */
    public int getTotalWorkers() {
      return totalSysInstances;
    }

    /**
     * 获得输入的{@link Record}数
     *
     * @return Record数
     */
    public long getInputRecords() {
      return inputRecords;
    }

    /**
     * 获得输出的{@link Record}数
     *
     * @return Record数
     */
    public long getOutputRecords() {
      return outputRecords;
    }

    /**
     * 获得已处理数据量的百分比
     *
     * @return finishedPercentage
     */
    public int getFinishedPercentage() {
      return finishedPercentage;
    }
  }

  public static String getStageProgressFormattedString(List<StageProgress> stages) {
    StringBuilder result = new StringBuilder();

    SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String dateString = sim.format(new Date());
    result.append(dateString + ' ');

    for (StageProgress stage : stages) {
      result.append(String.format("%s:%s/%s/%s%s%s", stage.getName(), stage.getRunningWorkers(),
                                  stage.getTerminatedWorkers(), stage.getTotalWorkers(),
                                  stage.getBackupWorkers() > 0 ? "(+" + stage.getBackupWorkers()
                                                                 + " backups)" : "",
                                  "[" + stage.getFinishedPercentage() + "%]\t"
      ));
    }
    return result.toString();
  }

  @Root(name = "Progress", strict = false)
  private static class TaskProgress {

    @ElementList(entry = "Stage", inline = true, required = false)
    List<StageProgress> stages = new LinkedList<StageProgress>();

    /**
     * 获得Instance的Stage统计
     *
     * @return {@link com.aliyun.odps.Instance.StageProgress}列表
     */
    List<StageProgress> getStages() {
      return stages;
    }

  }

  /**
   * 查询指定Task的Stage进度
   *
   * @param taskName
   *     Task名称
   * @return {@link com.aliyun.odps.Instance.StageProgress}列表
   * @throws OdpsException
   */
  public List<StageProgress> getTaskProgress(String taskName) throws OdpsException {
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("instanceprogress", taskName);
    params.put("taskname", taskName);

    TaskProgress r = client.request(TaskProgress.class, getResource(), "GET", params);

    return r.getStages();
  }

  /**
   * 获得Instance的 ODPS 对象
   *
   * @return Odps 对象
   */
  public Odps getOdps() {
    return odps;
  }

  /**
   * 获得Instance的ID
   *
   * @return Instance ID
   */
  public String getId() {
    return model.name;
  }


  /**
   * 获得Instance状态
   *
   *
   * <p>
   * Instance状态如下:<br />
   * <ul>
   * <li>RUNNING: 正在执行</li>
   * <li>SUSPENDED: 被挂起</li>
   * <li>TERMINATED: 执行结束, 包括成功、失败、取消等</li>
   * </p>
   *
   * @param isBlock
   *     是否使用 block 模式
   *     若启用 block 模式， 请求会被 block 住一定时间 （一般是 5s），随后再返回 instance 的状态。
   *     若不启用，请求将会立即返回。与接口 {@link #getStatus()} 的行为一致。
   * @return 返回{@link Status.TERMINATED}
   */
  public Status getStatus(boolean isBlock) {
    if (status != Status.TERMINATED) {
      try {
        reload(isBlock); // always reload status until terminated
      } catch (OdpsException e) {
        throw new ReloadException(e.getMessage(), e);
      }
    }

    try {
      if (hooks != null) {
        if (status == Status.TERMINATED) {
          if (!hookInvoked) {
            hookInvoked = true;
            hooks.after(this, odps);
          }
        }
      }
    } catch (OdpsException e) {
      throw new ReloadException(e.getMessage(), e);
    }
    return status;
  }

  /**
   * 获得Instance状态
   *
   * <p>
   * Instance状态如下:<br />
   * <ul>
   * <li>RUNNING: 正在执行</li>
   * <li>SUSPENDED: 被挂起</li>
   * <li>TERMINATED: 执行结束, 包括成功、失败、取消等</li>
   * </p>
   *
   * @return 返回{@link Status.TERMINATED}
   */
  public Status getStatus() {
    return getStatus(false);
  }

  /**
   * 检查是否执行完成
   *
   * @return TERMINATED状态返回true, 否则返回false
   */
  public boolean isTerminated() {
    return getStatus() == Status.TERMINATED;
  }

  /**
   * 获得Instance所属{@link Project}名称
   *
   * @return {@link Project}名称
   */
  public String getProject() {
    return project;
  }

  /**
   * 获得Instance是否为同步执行
   *
   * @return true如果同步执行
   */
  public boolean isSync() {
    return isSync;
  }

  /**
   * 获得Instance所属用户
   *
   * @return 用户名
   */
  public String getOwner() {
    if (model.owner == null) {
      lazyLoad();
    }
    return model.owner;
  }

  /**
   * 获得Instance开始执行时间
   *
   * @return 开始执行时间
   */
  public Date getStartTime() {
    if (model.startTime == null) {
      lazyLoad();
    }
    return model.startTime;
  }

  /**
   * 获得Instance结束执行时间
   *
   * @return 结束执行时间
   */
  public Date getEndTime() {
    if (model.endTime == null) {
      lazyLoad();
    }
    return model.endTime;
  }

  /**
   * 返回与作业运行实例相关的作业{@link Task}实例。
   *
   * @return 与作业运行实例相关的作业{@link Task}实例。
   */
  public List<Task> getTasks() throws OdpsException {
    String resource = getResource();

    return getJob(resource).getTasks();
  }

  /**
   * 获取 Instance 的优先级
   *
   * @return
   * @throws OdpsException
   */
  public int getPriority() throws OdpsException {
    String resource = getResource();

    return getJob(resource).getPriority();
  }

  private Job getJob(String resource) throws OdpsException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("source", null);
    JobModel model = client.request(JobModel.class, resource, "GET", params, null, null);
    return new Job(model);
  }

  /**
   * 获取 Instance 的 Job name
   *
   * @return job name
   * @throws OdpsException
   */
  public String getJobName() throws OdpsException {
    String resource = getResource();
    return getJob(resource).getName();
  }

  private String getResource() {
    return ResourceBuilder.buildInstanceResource(project, model.name);
  }

  /**
   * 获取Worker运行的LOG内容
   *
   * @param workerId
   *     Worker ID，{@link Task} 内Worker的唯一标示。
   * @param logType
   *     {@link LogType} 对象
   * @param size
   *     展示Log内容的长度
   * @return Log内容
   * @throws OdpsException
   * @throws IOException
   */
  public String getLog(String workerId, LogType logType, int size) throws OdpsException {

    if (workerId == null) {
      throw new OdpsException("Missing argument: workerId");
    }

    Map<String, String> params = new HashMap<String, String>();
    params.put("log", null);
    params.put("id", workerId);
    if (logType != null) {
      params.put("logtype", logType.toString());
    }
    if (size > 0) {
      params.put("size", String.valueOf(size));
    }

    Response resp = client.request(getResource(), "GET", params, null, null);

    return new String(resp.getBody());
  }

  /* Un-document */
  public String getTaskDetailJson(String taskName) throws OdpsException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("instancedetail", null);
    params.put("taskname", taskName);
    Response result = client.request(getResource(), "GET", params, null, null);
    return new String(result.getBody());
  }

  public OdpsHooks getOdpsHooks() {
    return hooks;
  }

  public void setOdpsHooks(OdpsHooks hooks) {
    this.hooks = hooks;
  }

  public boolean isHookInvoked() {
    return hookInvoked;
  }

  public void setHookInvoked(boolean hookInvoked) {
    this.hookInvoked = hookInvoked;
  }

  private boolean hookInvoked = false;
  private OdpsHooks hooks;

  /* Un-document */
  public String getTaskDetailJson2(String taskName) throws OdpsException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("detail", null);
    params.put("taskname", taskName);
    Response result = client.request(getResource(), "GET", params, null, null);
    return new String(result.getBody());
  }

  /**
   * 获取 task 运行 quota group 信息以及 quota group 中作业排队情况
   *
   * 注意，此接口的调用频率受限，两次调用间隔不能少于 30s, 否则返回字符串为 {}。
   *
   * @param taskName
   * @return task quota 相关信息
   *
   * 返回的字符串为 json 格式，下面给出一个例子：
   * {"quotaInfos": {   // 格式为 [ odps task 中 fuxi job 名称, fuxi job quota infos]
   *      "job0":       // fuxi job 名称
   *          "{
   *             "AmWaitingResInfo":   // 本 fuxi job 中 fuxi task 的资源使用情况 [taskId, info]
   *             [[0,                  // fuxi task id
   *                {
   *                  "RunningInstanceNum": 1, // fuxi task  中正在运行的 instance 数量
   *                  "SlotDesc": {},          // 一个最小资源单元的描述
   *                  "SuId": 0,               // fuxi task id
   *                  "TotalInstanceNum": 1,   // fuxi task 中运行的 fuxi instance 数量
   *                  "totalAheadResourceDesire": {}, // 等待队列中，排在本 fuxi task 之前的 task 所需资源总和，资源包括 CPU 和 memory
   *                  "totalAheadTaskNum": 0,         // 等待队列中，排在本 fuxi task 之前的 task 数量
   *                  "waitingQueueItemInfo": {}   // 排在本 fuxi task 之前的等待的 job 队列所需资源信息 [jobname, info(cpu,mem)]
   *              }]],
   *              "GroupAlias": "ttt",      // odps task 运行所在 quota 组名称
   *              "GroupId": 1,                // odps task 运行所在 quota 组 id
   *              "GroupMaxQuota": {           // quota 组可用资源上限
   *                  "CPU": 67200,
   *                  "Memory": 2437512},
   *              "GroupRequest": {            // 当前 quota 组中 job 申请资源的总和
   *                  "CPU": 100,
   *                  "Memory": 2068},
   *              "RuntimeQuota": {            // quota 组当前可被使用的资源总值
   *                  "CPU": 100,
   *                  "Memory": 2068},
   *              "UsedQuota": {               // quota 组当前已经被使用的资源总值
   *                  "CPU": 100,
   *                  "Memory": 2068}}"        // 空闲 quota = RuntimeQuota - UsedQuota
   *       }
   * }
   *
   * @throws OdpsException
   */
  public String getTaskQuotaJson(String taskName) throws OdpsException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("instancequota", null);
    params.put("taskname", taskName);
    Response result = client.request(getResource(), "GET", params, null, null);
    return new String(result.getBody());
  }

  /**
   * 以 debug 模式，启动 instance
   *
   * 本接口是提供给内部 debug 工具使用的，不建议用户直接使用
   *
   * @param workerId
   *     Worker ID，{@link Task} 内Worker的唯一标示。
   * @param debugId
   *     Debug ID, 标志此次 debug
   * @return 启动的结果
   * @throws OdpsException
   */
  public String startDebug(String workerId, String debugId) throws OdpsException {

    if (workerId == null) {
      throw new OdpsException("Missing argument: workerId");
    }

    if (debugId == null) {
      throw new OdpsException("Missing argument: debugId");
    }

    InstanceDebugModel model = new InstanceDebugModel();
    model.debugId = debugId;
    model.logId = workerId;

    try {
      String body = SimpleXmlUtils.marshal(model);
      HashMap<String, String> headers = new HashMap<String, String>();
      headers.put(Headers.CONTENT_TYPE, "application/xml");

      Response resp = client.stringRequest(getResource() + "/debug", "POST", null, headers, body);
      return new String(resp.getBody());
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  public static class InstanceQueueingInfo {

    InstanceQueueingInfo(JsonObject props) {
      properties = props;
    }

    public enum Status {
      /**
       * 运行
       */
      RUNNING,

      /**
       * 挂起
       */
      SUSPENDED,

      /**
       * 终止
       */
      TERMINATED,

      /**
       * 无法识别
       */
      UNKNOWN;
    }

    JsonObject properties;

    /**
     * 根据关键字来获取属性值
     *
     * @param key
     *        字段关键字
     * @param clz
     *        对应值的类型
     *
     * @return 相关字段的值
     */
    public <T> T getProperty(String key, Class<T> clz) {
      if (properties.get(key) == null) {
        return null;
      }

      return cast(properties.get(key), clz);
    }

    /**
     *
     * @param object
     * @param clz
     * @param <T>
     * @return
     */
    private <T> T cast(Object object, Class<T> clz) {
      if (object instanceof JsonPrimitive) {
        if (clz.equals(Integer.class)) {
          return clz.cast(((JsonPrimitive) object).getAsInt());
        } else if (clz.equals(Short.class)) {
          return clz.cast(((JsonPrimitive) object).getAsShort());
        } else if (clz.equals(Boolean.class)) {
          return clz.cast(((JsonPrimitive) object).getAsBoolean());
        } else if (clz.equals(Double.class)) {
          return clz.cast(((JsonPrimitive) object).getAsDouble());
        } else if (clz.equals(Long.class)) {
          return clz.cast(((JsonPrimitive) object).getAsLong());
        } else if (clz.equals(Float.class)) {
          return clz.cast(((JsonPrimitive) object).getAsFloat());
        } else if (clz.equals(String.class)) {
          return clz.cast(((JsonPrimitive) object).getAsString());
        } else if (clz.equals(Byte.class)) {
          return clz.cast(((JsonPrimitive) object).getAsByte());
        }
      } else {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.fromJson(gson.toJson(object), clz);
      }
      return null;
    }

    /**
     * 获取 instance id
     *
     * @return instance id
     */
    public String getId() {
      return properties.has("instanceId") ? properties.get("instanceId").getAsString() : null;
   }

    /**
     * 获取 instance 优先级
     *
     * @return instance 优先级
     */
    public Integer getPriority() {
      return properties.has("instancePriority") ? properties.get("instancePriority").getAsInt() : null;
    }

    /**
     * 获取 instance 进度
     *
     * @return progress
     */
    public Double getProgress() {
      return properties.has("instanceProgress") ? properties.get("instanceProgress").getAsDouble() : null;
    }

    /**
     * 获取 Job  名称
     *
     * @return job name
     */
    public String getJobName() {
      return properties.has("jobName") ? properties.get("jobName").getAsString() : null;
    }

    /**
     * 获取 project 名称
     *
     * @return project name
     */
    public String getProject() {
      return properties.has("projectName") ? properties.get("projectName").getAsString() : null;
    }

    /**
     * 获取 instance owner 的 skynetId
     *
     * @return skynetId
     */
    public String getSkyNetId() {
      return properties.has("skynetId") ? properties.get("skynetId").getAsString() : null;
    }

    /**
     * 获取 instance 开始
     *
     * @return instance 开始时间
     */
    public Date getStartTime() {
      String startTimeStr = properties.get("startTime").getAsString();
      DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
      Date startTime = null;
      try {
        startTime = format.parse(startTimeStr);
      } catch (ParseException e1) {
        try {
          startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSZ").parse(startTimeStr);
        } catch (ParseException e2) {

        }
      }
      return startTime;
    }

    /**
     * 当前运行的 odps task 类型
     *
     * @return task type
     */
    public String getTaskType() {
      return properties.get("taskType").getAsString();
    }

    /**
     * 当前运行的 odps task 名称
     *
     * @return task name
     */
    public String getTaskName() {
      return properties.get("taskName").getAsString();
    }

    /**
     * 获取 instance owner 的 云账号信息
     *
     * @return userAcount
     */
    public String getUserAccount() {
      return properties.get("userAccount").getAsString();
    }

    /**
     * 获取 instance 状态
     *
     * @return 状态
     */
    public Status getStatus() {
      if (!properties.has("status")) {
        return null;
      }
      String status = properties.get("status").getAsString();

      try {
        return Status.valueOf(status.toUpperCase());
      } catch (Exception e) {
        return Status.UNKNOWN;
      }
    }

  }

  /**
   * 获取运行中 instance 的排队信息
   *
   * @return {@link InstanceQueueingInfo}的迭代器
   * @throws OdpsException
   */
  public InstanceQueueingInfo getQueueingInfo() throws OdpsException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("cached", null);

    Response resp = client.request(getResource(), "GET", params, null, null);
    Gson gson = GsonObjectBuilder.get();
    JsonObject object = gson.fromJson(new String(resp.getBody()), JsonObject.class);
    return new InstanceQueueingInfo(object);
  }
}
