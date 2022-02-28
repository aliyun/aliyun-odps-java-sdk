package com.aliyun.odps;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SessionQueryResult;
import com.aliyun.odps.task.SQLRTTask;
import com.aliyun.odps.utils.CSVRecordParser;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.lang.reflect.Type;

public class Session {

  public class SubQueryInfo {
    public static final String kNotFoundCode = "NotFound";
    public static final String kFailedCode = "Failed";
    public static final String kOKCode = "ok";

    public int queryId = -1;
    public String status = kOKCode;
    public String result;

    public SubQueryInfo(String status, String result) {
      this.status = status;
      this.result = result;
    }
  }

  public class SubQueryResult {
    TableSchema schema = null;
    List<String> warnings = new ArrayList<>();
    List<Record> records = null;

    public void setSchema(TableSchema schema) {
      this.schema = schema;
    }

    public void addWarning(String warning) {
      this.warnings.add(warning);
    }

    public void setRecords(List<Record> records) {
      this.records = records;
    }

    public TableSchema getSchema() {
      return schema;
    }

    public List<Record> getRecords() {
      return records;
    }

    public List<String> getWarnings() {
      return warnings;
    }
  }

  public class SessionItem {
    public String owner;
    public String sessionId;
    public String aliasName;
    public String version;
  }

  private static final String DEFAULT_TASK_NAME = "console_sqlrt_task";
  private static final long POLL_INTERVAL = TimeUnit.MILLISECONDS.toMillis(1000);

  private String taskName = DEFAULT_TASK_NAME;

  public Session(Odps odps, Instance instance) {
    this(odps, instance, null, DEFAULT_TASK_NAME);
  }

  public Session(Odps odps, Instance instance, String sessionName, String taskName) {
    this.sessionName = sessionName;
    this.instance = instance;
    this.startSessionMessage = "";
    this.taskName = taskName;
    this.odps = odps;
  }

  private Odps odps;
  private String sessionName;
  private Instance instance;
  private String logView;
  private boolean isStarted = false;
  private SessionProgress progress = null;
  private String startSessionMessage;

  private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  public static int OBJECT_STATUS_RUNNING = 2;
  public static int OBJECT_STATUS_FAILED = 4;
  public static int OBJECT_STATUS_TERMINATED = 5;
  public static int OBJECT_STATUS_CANCELLED = 6;

  public String getLogView() throws OdpsException{
    if (logView == null && odps != null) {
      logView = new LogView(odps).generateLogView(instance, 7 * 24 /* by default one week. can be set by config */);
    }
    return logView;
  }

  public void setLogView(String logView) {
    this.logView = logView;
  }

  public String getStartSessionMessage() {
    return startSessionMessage;
  }

  public class SubQueryResponse {
    public Integer status;
    public String result;
    public String warnings;
    public Integer subQueryId;
  }

  public class SessionProgress {
    public Integer totalWorkerCount;
    public Integer launchedWorkerCount;
    public Integer launchedPercentage;
  }

  /**
   * 返回 odps instance 对象
   *
   * @return instance
   */
  public Instance getInstance() {
    return instance;
  }

  /**
   * attach 指定名字的 session
   * 此调用会立即返回，不会等待 session 启动完成。 可以手动调用 {@link #waitForStart(long)} 来等待启动。
   *
   * @param odps
   *     odps 对象
   * @param sessionName
   *     指定 session 的名字
   * @return session 对象
   * @throws OdpsException
   */
  public static Session attach(Odps odps, String sessionName) throws OdpsException {
    return attach(odps, sessionName, null);
  }

  /**
   * attach 指定名字的 session
   * 此调用会立即返回，不会等待 session 启动完成。 可以手动调用 {@link #waitForStart(long)} 来等待启动。
   *
   * @param odps
   *     odps 对象
   * @param sessionName
   *     指定的 session 名字
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @return
   * @throws OdpsException
   */
  public static Session attach(Odps odps, String sessionName, Map<String, String> hints)
      throws OdpsException {
    return attach(odps, sessionName, hints, null);
  }

  /**
   * attach 指定名字的 session
   *
   * @param odps
   *     odps 对象
   * @param sessionName
   *     指定的 session 名字
   * @param hints
   *     能够影响 SQL 执行的Set 参数数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @return
   * @throws OdpsException
   */
  public static Session attach(Odps odps, String sessionName, Map<String, String> hints,
                               Long timeout) throws OdpsException {
    return attach(odps, sessionName, hints, timeout, DEFAULT_TASK_NAME);
  }

  /**
   * attach 指定名字的 session
   *
   * @param odps
   *     odps 对象
   * @param sessionName
   *     指定的 session 名字
   * @param hints
   *     能够影响 SQL 执行的Set 参数数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @param taskName
   *     SqlRtTask的taskName
   * @return
   * @throws OdpsException
   */
  public static Session attach(Odps odps, String sessionName, Map<String, String> hints,
                               Long timeout, String taskName) throws OdpsException {
    return attach(odps, sessionName, hints, timeout, null, taskName);
  }

  /**
   * attach 指定名字的 session
   *
   * @param odps
   *     odps 对象
   * @param sessionName
   *     指定的 session 名字
   * @param hints
   *     能够影响 SQL 执行的Set 参数数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @param runningCluster
   *      运行集群
   * @param taskName
   *     SqlRtTask的taskName
   * @return
   * @throws OdpsException
   */
  public static Session attach(Odps odps, String sessionName, Map<String, String> hints,
                               Long timeout, String runningCluster, String taskName) throws OdpsException {
    if (StringUtils.isNullOrEmpty(sessionName)) {
      throw new IllegalArgumentException("Session name can not be empty.");
    }

    if (hints == null) {
      hints = new HashMap<>();
    }
    hints.put("odps.sql.session.share.id", sessionName);

    try {
      return createInternal(odps, null, sessionName, null, null, null, hints, timeout, null, runningCluster, taskName);
    } finally {
      hints.remove("odps.sql.session.share.id");
    }
  }
  /**
   * 创建 session
   * 此调用会立即返回，不会等待 session 启动完成。 可以手动调用 {@link #waitForStart(long)} 来等待启动。
   *
   * @param odps
   *     odps 对象
   * @param workerCount
   *     session work 数量（单位：个）
   * @param workerMemory
   *     session work 内存 （单位： MB）
   * @return session 对象
   * @throws OdpsException
   */
  public static Session create(Odps odps, int workerCount, int workerMemory)
      throws OdpsException {
    return create(odps, workerCount, workerMemory, null, null, null, null, null);
  }

  /**
   * 创建 session
   * 此调用会立即返回，不会等待 session 启动完成。 可以手动调用 {@link #waitForStart(long)} 来等待启动。
   *
   * @param odps
   *     odps 对象
   * @param workerCount
   *     session work 数量（单位：个）
   * @param workerMemory
   *     session work 内存 （单位： MB）
   * @param sessionName
   *     指定 session 名字
   * @return session 对象
   * @throws OdpsException
   */
  public static Session create(Odps odps, int workerCount, int workerMemory, String sessionName)
      throws OdpsException {
    return create(odps, workerCount, workerMemory, sessionName, null, null, null, null);
  }

  /**
   * 创建 session
   *
   * @param odps
   *     odps 对象
   * @param workerCount
   *     session work 数量（单位：个）
   * @param workerMemory
   *     session work 内存 （单位： MB）
   * @param sessionName
   *     指定 session 名字
   * @param workerSpareSpan
   *     session 对应 cg service 的服务休息时间, 格式是 startHour-endHour
   *     例如 0-12 表示0点到12点 worker 数会降为 0。
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @return session 对象
   * @throws OdpsException
   */
  public static Session create(Odps odps, int workerCount, int workerMemory, String sessionName,
                               String projectName, String workerSpareSpan,
                               Map<String, String> hints, Long timeout) throws OdpsException {

    return create(odps, workerCount, workerMemory, sessionName, projectName,
                          workerSpareSpan, hints,
                          timeout, null);
  }

  /**
   * 创建 session
   *
   * @param odps
   *     odps 对象
   * @param workerCount
   *     session work 数量（单位：个）
   * @param workerMemory
   *     session work 内存 （单位： MB）
   * @param sessionName
   *     指定 session 名字
   * @param workerSpareSpan
   *     session 对应 cg service 的服务休息时间, 格式是 startHour-endHour
   *     例如 0-12 表示0点到12点 worker 数会降为 0。
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @param priority
   *     session 优先级
   * @return session 对象
   * @throws OdpsException
   */
  public static Session create(Odps odps, int workerCount, int workerMemory, String sessionName,
                               String projectName, String workerSpareSpan,
                               Map<String, String> hints, Long timeout, Integer priority) throws OdpsException {

    return create(odps, workerCount, workerMemory, sessionName, projectName, workerSpareSpan, hints, timeout, priority, null);
  }


  /**
   * 创建 session
   *
   * @param odps
   *     odps 对象
   * @param workerCount
   *     session work 数量（单位：个）
   * @param workerMemory
   *     session work 内存 （单位： MB）
   * @param sessionName
   *     指定 session 名字
   * @param workerSpareSpan
   *     session 对应 cg service 的服务休息时间, 格式是 startHour-endHour
   *     例如 0-12 表示0点到12点 worker 数会降为 0。
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @param priority
   *     session 优先级
   * @param runningCluster
   *      运行集群
   * @return session 对象
   * @throws OdpsException
   */
  public static Session create(Odps odps, int workerCount, int workerMemory, String sessionName,
                               String projectName, String workerSpareSpan,
                               Map<String, String> hints, Long timeout, Integer priority, String runningCluster) throws OdpsException {

    return createInternal(odps, projectName, sessionName, workerCount, workerMemory,
                          workerSpareSpan, hints,
                          timeout, priority, runningCluster);
  }

  /**
   * 创建 session
   *
   * @param odps
   *     odps 对象
   * @param workerCount
   *     session work 数量（单位：个）
   * @param workerMemory
   *     session work 内存 （单位： MB）
   * @param sessionName
   *     指定 session 名字
   * @param workerSpareSpan
   *     session 对应 cg service 的服务休息时间, 格式是 startHour-endHour
   *     例如 0-12 表示0点到12点 worker 数会降为 0。
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @param priority
   *     session 优先级
   * @param runningCluster
   *      运行集群
   * @param taskName
   *     SqlRtTask的taskName
   * @return session 对象
   * @throws OdpsException
   */
  public static Session create(Odps odps, int workerCount, int workerMemory, String sessionName,
                               String projectName, String workerSpareSpan,
                               Map<String, String> hints, Long timeout, Integer priority, String runningCluster,
                               String taskName) throws OdpsException {

    return createInternal(odps, projectName, sessionName, workerCount, workerMemory,
        workerSpareSpan, hints,
        timeout, priority, runningCluster, taskName);
  }
  /**
   * 创建 session
   *
   * @param odps
   *     odps 对象
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @return session 对象
   * @throws OdpsException
   */
  public static Session create(Odps odps, String sessionName, String projectName, Map<String, String> hints, Long timeout) throws OdpsException {
    return create(odps, sessionName, projectName, hints, timeout, null);
  }



  /**
   * 创建 session
   *
   * @param odps
   *     odps 对象
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @param priority
   *     session 优先级
   * @return session 对象
   * @throws OdpsException
   */
  public static Session create(Odps odps, String sessionName, String projectName, Map<String, String> hints, Long timeout, Integer priority) throws OdpsException {
    return create(odps, sessionName, projectName, hints, timeout, priority, null);
  }

  /**
   * 创建 session
   *
   * @param odps
   *     odps 对象
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @param timeout
   *     等待 session 启动的超时时间，单位: 秒
   *     其中: null 表示从不等待； 0 表示阻塞等待
   * @param priority
   *     session 优先级
   * @param runningCluster
   *     运行集群
   * @return session 对象
   * @throws OdpsException
   */
  public static Session create(Odps odps, String sessionName, String projectName, Map<String, String> hints, Long timeout, Integer priority, String runningCluster) throws OdpsException {
    return createInternal(odps, projectName, sessionName, null, null,
        null, hints, timeout, priority, runningCluster);
  }

  /**
   * 提交查询
   *
   * @param sql
   *     sql 语句
   * @return 查询结果
   * @throws OdpsException
   */
  @Deprecated
  public SessionQueryResult run(String sql) throws OdpsException {
    return run(sql, null);
  }


  /**
   * 提交查询
   *
   * @param sql
   *     sql 语句
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @return 查询结果
   * @throws OdpsException
   */
  @Deprecated
  public SessionQueryResult run(String sql, Map<String, String> hints) throws OdpsException {

    JsonObject request = new JsonObject();
    request.add("query", new JsonPrimitive(sql));

    if (hints == null) {
      hints = new HashMap<String, String>();
    }

    JsonObject settings = new JsonObject();

    for (Map.Entry<String, String> property : hints.entrySet()) {
      settings.addProperty(property.getKey(), property.getValue());
    }

    request.add("settings", settings);

    Instance.SetInformationResult setInformationResult = instance.setInformation(taskName, "query", gson.toJson(request));
    SubQueryInfo subQueryInfo = null;
    if (!setInformationResult.status.equals(SubQueryInfo.kOKCode)) {
      // instance not found or failed
      subQueryInfo = new SubQueryInfo(setInformationResult.status, setInformationResult.result);
    } else if (!StringUtils.isNullOrEmpty(setInformationResult.result)) {
      Type type = new TypeToken<SubQueryInfo>() {
      }.getType();
      try {
        subQueryInfo = gson.fromJson(setInformationResult.result, type);
      } catch (Exception e) {
        throw new OdpsException(setInformationResult.result);
      }
      subQueryInfo.status = setInformationResult.status;
    } else {
      // there will be no 'result' in response from old sqlrt task
    }
    return new SessionQueryResult(subQueryInfo, new ListIterator<SubQueryResponse>() {
      boolean queryTerminated = false;

      @Override
      protected List<SubQueryResponse> list() {
        try {
          if (queryTerminated) {
            return null;
          }

          while (true) {
            SubQueryResponse response = getResponse(instance.getTaskInfo(taskName, "result"));

            if (response == null || response.status == null) {
              checkTaskStatus();
            } else {
              if (response.status != OBJECT_STATUS_RUNNING) {
                queryTerminated = true;
              }

              return Arrays.asList(response);
            }
          }
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }
      }
    });
  }

  /**
   * 提交查询
   *
   * @param sql
   *     sql 语句
   * @param hints
   *     能够影响 SQL 执行的Set 参数
   * @return SubQueryInfo query标识
   * @throws OdpsException
   */
  public SubQueryInfo runSubQuery(String sql, Map<String, String> hints) throws OdpsException {

    JsonObject request = new JsonObject();
    request.add("query", new JsonPrimitive(sql));

    if (hints == null) {
      hints = new HashMap<String, String>();
    }

    JsonObject settings = new JsonObject();

    for (Map.Entry<String, String> property : hints.entrySet()) {
      settings.addProperty(property.getKey(), property.getValue());
    }

    request.add("settings", settings);

    Instance.SetInformationResult setInformationResult = instance.setInformation(taskName, "query", gson.toJson(request));
    SubQueryInfo subQueryInfo = null;
    if (!setInformationResult.status.equals(SubQueryInfo.kOKCode)) {
      // instance not found or failed
      subQueryInfo = new SubQueryInfo(setInformationResult.status, setInformationResult.result);
    } else if (!StringUtils.isNullOrEmpty(setInformationResult.result)) {
      Type type = new TypeToken<SubQueryInfo>() {}.getType();
      try {
        subQueryInfo = gson.fromJson(setInformationResult.result, type);
      } catch (Exception e) {
        throw new OdpsException(setInformationResult.result);
      }
      subQueryInfo.status = setInformationResult.status;
    } else {
      throw new OdpsException("Invalid setInformation response.");
    }
    return subQueryInfo;
  }

  /**
   * 等待SubQuery执行结束获取查询结果
   *
   * @param queryId
   *
   * @return 查询结果
   * @throws OdpsException
   */
  public SubQueryResponse waitForSubqueryTerminated(int queryId) throws OdpsException {
    boolean terminated = false;
    SubQueryResponse response = new SubQueryResponse();
    while (!terminated) {
      response = getResponse(instance.getTaskInfo(taskName, "get_finished_status_" + queryId));
      if (response == null || response.status == null) {
        checkTaskStatus();
      } else if (response.status == OBJECT_STATUS_FAILED && response.result.contains("SubQuery not found")) {
        throw new OdpsException(response.result);
      } else if (response.status != OBJECT_STATUS_RUNNING) {
        terminated = true;
      }
    }
    return response;
  }

  /**
   * getInformation查询SubQuery结果
   *
   * @param queryId
   *     sql 语句
   * @return 查询结果
   * @throws OdpsException
   */
  public SubQueryResult getSubQueryResult(int queryId) throws OdpsException {
    String resultString = getSubQueryResultInternal(queryId);
    SubQueryResult result = new SubQueryResult();
    CSVRecordParser.ParseResult parseResult = CSVRecordParser.parse(resultString);
    result.setSchema(parseResult.getSchema());
    result.setRecords(parseResult.getRecords());
    return result;
  }

  /**
   * 从odps_worker查询SubQuery结果
   *
   * @param queryId
   *     sql 语句
   * @return 查询结果
   * @throws OdpsException
   */
  public SubQueryResult getSubQueryResultFromWorker(int queryId) throws OdpsException {
    SubQueryResponse response = waitForSubqueryTerminated(queryId);
    String subqueryId = "session_query_" + queryId;
    String resultString = instance.getRawSubqueryResults(subqueryId, taskName);

    if (response.status == OBJECT_STATUS_FAILED) {
      throw new OdpsException(resultString);
    } else {
      if (!StringUtils.isNullOrEmpty(response.result)) {
        resultString += response.result;
      }
      SubQueryResult result = new SubQueryResult();
      if (!StringUtils.isNullOrEmpty(response.warnings)) {
        result.addWarning(response.warnings);
      }
      CSVRecordParser.ParseResult parseResult = CSVRecordParser.parse(resultString);
      result.setSchema(parseResult.getSchema());
      result.setRecords(parseResult.getRecords());
      return result;
    }
  }

  /**
   * 查询当前CacheOn的变量列表
   *
   * @param
   * @return 查询结果
   * @throws OdpsException
   */
  public List<String> showVariables(Map<String, String> hints) throws OdpsException {
    SubQueryInfo subQueryInfo = runSubQuery("show variables;", hints);
    String resultString = getSubQueryResultInternal(subQueryInfo.queryId);
    List<String> vars = new ArrayList<>();
    if (!StringUtils.isNullOrEmpty(resultString)) {
      vars = Arrays.asList(resultString.split("\n"));
    }
    return vars;
  }

  private String getSubQueryResultInternal(int queryId) throws OdpsException {
    SubQueryResult result = new SubQueryResult();
    String resultString = "";
    boolean terminated = false;
    while(!terminated) {
      SubQueryResponse response = getResponse(instance.getTaskInfo(taskName, "result_" + queryId));
      if (response == null || response.status == null) {
        checkTaskStatus();
      } else {
        if (!StringUtils.isNullOrEmpty(response.result)) {
          resultString += response.result;
        }
        if (!StringUtils.isNullOrEmpty(response.warnings)) {
          result.addWarning(response.warnings);
        }
        if (response.status == OBJECT_STATUS_FAILED) {
          throw new OdpsException(resultString);
        } else if (response.status != OBJECT_STATUS_RUNNING) {
          terminated = true;
        }
      }
    }
    return resultString;
  }

  /**
   * 停止 session
   *
   * @throws OdpsException
   */
  public void stop() throws OdpsException {
    instance.stop();
  }

  /**
   * 阻塞等待 session 启动
   *
   * @throws OdpsException
   */
  public void waitForStart() throws OdpsException {
    waitForStart(0L);
  }

  /**
   * 判断 session 是否已经成功启动
   *
   * @return 是否已经启动
   * @throws OdpsException 启动异常
   */

  public boolean isStarted() throws OdpsException {

    if (!isStarted) {
      SubQueryResponse response = getResponse(instance.getTaskInfo(taskName, "status"));

      if (response == null || response.status == null) {
        checkTaskStatus();
      } else if (response.status == OBJECT_STATUS_RUNNING) {
        isStarted = true;
        if (response.result != null && response.result.length() > 0) {
          startSessionMessage += response.result;
        }
      } else if (response.status == OBJECT_STATUS_FAILED) {
        throw new OdpsException(
            String.format("Start session[%s] failed: %s ", instance.getId(), response.result));
      } else if(!StringUtils.isNullOrEmpty(response.result)) {
        try {
          progress = gson.fromJson(response.result, SessionProgress.class);
        } catch (Exception e) {
          // ignore
        }
      }

    }

    return isStarted;
  }

  public void waitAttachSuccess() throws OdpsException {
    SubQueryResponse response = getResponse(instance.getTaskInfo(taskName, "wait_attach_success"));

    if (response == null || response.status == null) {
      checkTaskStatus();
    } else if (response.status == OBJECT_STATUS_RUNNING) {
      if (response.result != null && response.result.length() > 0) {
        startSessionMessage += response.result;
      }
    } else if (response.status == OBJECT_STATUS_FAILED) {
      throw new OdpsException(
          String.format("Start session[%s] failed: %s ", instance.getId(), response.result));
    } else if(!StringUtils.isNullOrEmpty(response.result)) {
      try {
        progress = gson.fromJson(response.result, SessionProgress.class);
      } catch (Exception e) {
        // ignore
      }
    }
  }

  /**
   * get sqlstats of subqyery
   *
   * @return stats
   * @throws OdpsException 启动异常
   */

  public String getQueryStats() throws OdpsException {
    return getInformation("sqlstats");
  }

  /**
   * get sqlstats of subqyery
   * @param queryId
   *     queryId
   * @return stats
   * @throws OdpsException 启动异常
   */

  public String getQueryStats(int queryId) throws OdpsException {
    return getInformation("sqlstats_" + String.valueOf(queryId));
  }
  /**
   * getInformation
   *
   * @return stats
   * @throws OdpsException 获取information异常
   */

  public String getInformation(String key) throws OdpsException {
    SubQueryResponse response = getResponse(instance.getTaskInfo(taskName, key));

    if (response != null && response.result != null) {
      return response.result;
    }
    return null;
  }

  /**
   * setInformation
   *
   * @return stats
   * @throws OdpsException 设置information异常
   */

  public String setInformation(String key, String value) throws OdpsException {
    String result = instance.setTaskInfo(taskName, key, value);
    return result;
  }

  /**
   * 获取 session 启动的进度
   * @return session 启动的当前进度，包含 worker 启动数和启动总数，启动进度百分比
   *
   * @throws OdpsException
   */
  public SessionProgress getStartProgress() throws OdpsException {
    if (progress == null) {
      isStarted();
    }
    return progress;
  }

  /**
   * 等待 session 启动
   *
   * @param timeout
   *     等待的超时时间(单位: 秒)
   *     0 表示阻塞等待
   * @throws OdpsException
   */
  public void waitForStart(long timeout) throws OdpsException {
    long startTime = System.currentTimeMillis();
    long endTime = 0;

    if (timeout > 0) {
      endTime += startTime + TimeUnit.SECONDS.toMillis(timeout);
    }

    while (0 == endTime || System.currentTimeMillis() < endTime) {
      if (isStarted()) {
        return;
      }
      sleep();
    }
    throw new OdpsException("Start session timeout.");
  }

  private SubQueryResponse getResponse(String result) throws OdpsException {
    if (StringUtils.isNullOrEmpty(result)) {
      return null;
    }
    try {
      return gson.fromJson(result, SubQueryResponse.class);
    } catch (Exception e) {
      throw new OdpsException("Invalid response:" + result);
    }
  }

  private void checkTaskStatus() throws OdpsException {
    Instance.TaskStatus status = instance.getTaskStatus().get(taskName);

    if (status != null && status.getStatus() != Instance.TaskStatus.Status.RUNNING) {
      throw new OdpsException(String.format("Session[%s] is %s: %s", instance.getId(),
                                            status.getStatus().toString(),
                                            instance.getTaskResults().get(taskName)));
    }
  }

  private static Session createInternal(Odps odps, String projectName, String sessionName,
                                        Integer workerCount, Integer workerMemory,
                                        String workerSpareSpan, Map<String, String> hints,
                                        Long timeout, Integer priority, String runningCluster) throws OdpsException {
    return createInternal(odps, projectName, sessionName, workerCount, workerMemory,
        workerSpareSpan, hints, timeout, priority, runningCluster, DEFAULT_TASK_NAME);
  }

  private static Session createInternal(Odps odps, String projectName, String sessionName,
                                        Integer workerCount, Integer workerMemory,
                                        String workerSpareSpan, Map<String, String> hints,
                                        Long timeout, Integer priority, String runningCluster,
                                        String taskName) throws OdpsException {
    if (projectName != null && projectName.trim().isEmpty()) {
      throw new IllegalArgumentException("Project name can not be empty.");
    }

    if (null == hints) {
      hints = new HashMap<String, String>();
    }

    projectName = (projectName == null ? odps.getDefaultProject() : projectName);

    if (!StringUtils.isNullOrEmpty(workerSpareSpan)) {
      hints.put("odps.sql.session.worker.sparespan", workerSpareSpan);
    }

    if (!StringUtils.isNullOrEmpty(sessionName)) {
      hints.put("odps.sql.session.name", sessionName.trim());
    }

    if (null != workerCount) {
      hints.put("odps.sql.session.worker.count", workerCount.toString());
    }

    if (null != workerMemory) {
      hints.put("odps.sql.session.worker.memory", workerMemory.toString());
    }

    String userSubmitMode = hints.get("odps.sql.submit.mode");
    hints.put("odps.sql.submit.mode", "script");
    SQLRTTask task = new SQLRTTask();
    task.setName(taskName);

    try {
      String json = gson.toJson(hints);
      task.setProperty("settings", json);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

    if (userSubmitMode == null || userSubmitMode.isEmpty()) {
      hints.remove("odps.sql.submit.mode");
    } else {
      hints.put("odps.sql.submit.mode", userSubmitMode);
    }

    Instance instance = odps.instances().create(projectName, task, priority, runningCluster);

    Session session = new Session(odps, instance, sessionName, taskName);

    // attach mode, long polling
    if (hints.containsKey("odps.sql.session.share.id")) {
      session.waitAttachSuccess();
    } else {
      if (timeout != null) {
        session.waitForStart(timeout);
      }
    }

    return session;
  }

  public void printLogView() throws OdpsException{
    System.out.println("");
    System.err.println("ID = " + instance.getId());

    System.err.println("Log view:");
    System.err.println(getLogView());
  }

  public void cancelQuery(int queryId) throws OdpsException {
    Instance.SetInformationResult setInformationResult
        = instance.setInformation(taskName, "cancel", String.valueOf(queryId));
    if (!setInformationResult.status.equals(SubQueryInfo.kOKCode)) {
      throw new OdpsException(setInformationResult.result);
    }
  }

  private void sleep() throws OdpsException {
    try {
      Thread.sleep(POLL_INTERVAL);
    } catch (InterruptedException e) {
      throw new OdpsException("Interrupted while sleep.", e);
    }
  }

  public String getSessionName() {
    return sessionName;
  }

  public String getTaskName() { return taskName; }
}
