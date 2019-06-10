package com.aliyun.odps;

import com.aliyun.odps.data.SessionQueryResult;
import com.aliyun.odps.task.SQLRTTask;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Session {

  private static String DEFAULT_TASK_NAME = "console_sqlrt_task";
  private static final long POLL_INTERVAL = TimeUnit.SECONDS.toMillis(3);

  public Session(Odps odps, Instance instance) throws OdpsException {
    this(odps, instance, null);
  }

  Session(Odps odps, Instance instance, String sessionName) throws OdpsException {
    this.sessionName = sessionName;
    this.instance = instance;
    this.logView = new LogView(odps).generateLogView(instance, 7 * 24 /* by default one week. can be set by config */);
  }

  private String sessionName;
  private Instance instance;
  private String logView;

  private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private static int OBJECT_STATUS_RUNNING = 2;
  private static int OBJECT_STATUS_FAILED = 4;
  private static int OBJECT_STATUS_TERMINATED = 5;
  private static int OBJECT_STATUS_CANCELLED = 6;

  public String getLogView() {
    return logView;
  }

  public void setLogView(String logView) {
    this.logView = logView;
  }

  public class SubQueryResponse {
    public Integer status;
    public String result;
    public String warnings;
    public Integer subQueryId;
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
    if (StringUtils.isNullOrEmpty(sessionName)) {
      throw new IllegalArgumentException("Session name can not be empty.");
    }

    if (hints == null) {
      hints = new HashMap<String, String>();
    }
    hints.put("odps.sql.session.share.id", sessionName);

    try {
      return createInternal(odps, null, null, null, null, null, hints, timeout);
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

    return createInternal(odps, projectName, sessionName, workerCount, workerMemory,
                          workerSpareSpan, hints,
                          timeout);
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

    return createInternal(odps, projectName, sessionName, null, null,
        null, hints, timeout);
  }

  /**
   * 提交查询
   *
   * @param sql
   *     sql 语句
   * @return 查询结果
   * @throws OdpsException
   */
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

    instance.setTaskInfo(DEFAULT_TASK_NAME, "query", gson.toJson(request));

    return new SessionQueryResult(new ListIterator<SubQueryResponse>() {
      boolean queryTerminated = false;

      @Override
      protected List<SubQueryResponse> list() {
        try {
          if (queryTerminated) {
            return null;
          }

          while (true) {
            SubQueryResponse response = getResponse(instance.getTaskInfo(DEFAULT_TASK_NAME, "result"));

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
      SubQueryResponse response = getResponse(instance.getTaskInfo(DEFAULT_TASK_NAME, "status"));

      if (response == null || response.status == null) {
        checkTaskStatus();
      } else if (response.status == OBJECT_STATUS_RUNNING) {
        return;
      } else if (response.status == OBJECT_STATUS_FAILED) {
        throw new OdpsException(
            String.format("Start session[%s] failed: %s ", sessionName, response.result));
      }

      sleep();
    }
  }

  private SubQueryResponse getResponse(String result) {
    if (StringUtils.isNullOrEmpty(result)) {
      return null;
    }

    return gson.fromJson(result, SubQueryResponse.class);
  }


  private void checkTaskStatus() throws OdpsException {
    Instance.TaskStatus status = instance.getTaskStatus().get(DEFAULT_TASK_NAME);

    if (status != null && status.getStatus() != Instance.TaskStatus.Status.RUNNING) {
      throw new OdpsException(String.format("Session[%s] is %s: %s", instance.getId(),
                                            status.getStatus().toString(),
                                            instance.getTaskResults().get(DEFAULT_TASK_NAME)));
    }
  }

  private static Session createInternal(Odps odps, String projectName, String sessionName,
                                        Integer workerCount, Integer workerMemory,
                                        String workerSpareSpan, Map<String, String> hints,
                                        Long timeout) throws OdpsException {
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
    task.setName(DEFAULT_TASK_NAME);

    try {
      String json = gson.toJson(hints);
      task.setProperty("settings", json);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }

    Instance instance = odps.instances().create(projectName, task);
    if (userSubmitMode == null || userSubmitMode.isEmpty()) {
      hints.remove("odps.sql.submit.mode");
    } else {
      hints.put("odps.sql.submit.mode", userSubmitMode);
    }

    Session session = new Session(odps, instance, sessionName);
    session.printLogView();

    if (timeout != null) {
      session.waitForStart(timeout);
    }

    return session;
  }

  public void printLogView() {
    System.out.println("");
    System.err.println("ID = " + instance.getId());

    System.err.println("Log view:");
    System.err.println(logView);
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

}
