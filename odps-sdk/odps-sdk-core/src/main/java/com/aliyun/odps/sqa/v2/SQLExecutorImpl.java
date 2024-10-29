package com.aliyun.odps.sqa.v2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Quota;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.QueryInfo;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.sqa.SQLExecutorConstants;
import com.aliyun.odps.sqa.SQLExecutorPool;
import com.aliyun.odps.sqa.commandapi.Command;
import com.aliyun.odps.sqa.commandapi.CommandInfo;
import com.aliyun.odps.sqa.commandapi.RecordIter;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.sqa.commandapi.utils.SqlParserUtil;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.utils.CSVRecordParser;
import com.aliyun.odps.utils.StringUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SQLExecutorImpl implements SQLExecutor {

  private static final String DEFAULT_TASK_NAME = "AnonymousMCQATask";
  private static final String MCQA_PREFIX = "/mcqa";

  private final Odps odps;
  private final Odps mcqaOdps;
  private InstanceTunnel instanceTunnel;
  private final List<String> log;
  private final boolean useInstanceTunnel;
  private final String id;

  // current query info
  QueryInfo queryInfo = null;
  private final boolean useCommandApi;
  private boolean parseSuccess = false;
  private final boolean odpsNamespaceSchema;
  private final String taskName;
  private final SQLExecutorPool pool;

  public SQLExecutorImpl(SQLExecutorBuilder builder)
      throws OdpsException {
    String quotaNickName = Preconditions.checkNotNull(builder.getQuotaName(), "quotaName");

    this.odps = builder.getOdps().clone();
    this.odps.setTunnelEndpoint(builder.getTunnelEndpoint());
    this.mcqaOdps = this.odps.clone();
    this.useInstanceTunnel = builder.isUseInstanceTunnel();
    if (useInstanceTunnel) {
      this.instanceTunnel = new InstanceTunnel(odps);
      if (builder.getTunnelSocketTimeout() >= 0) {
        instanceTunnel.getConfig().setSocketConnectTimeout(builder.getTunnelSocketTimeout());
      }
      if (builder.getTunnelReadTimeout() >= 0) {
        instanceTunnel.getConfig().setSocketTimeout(builder.getTunnelReadTimeout());
      }
    }
    this.log = new ArrayList<>();
    // each executor has a uuid
    this.id = UUID.randomUUID().toString();
    mcqaOdps.getRestClient().setPrefix(MCQA_PREFIX);
    Quota quota = builder.getQuota();
    if (quota == null) {
      quota = odps.quotas()
          .getWlmQuota(odps.getDefaultProject(), quotaNickName, builder.getRegionId());
    }
    String mcqaConnectionHeader = quota.getMcqaConnHeader();

    if (!quota.isInteractiveQuota()) {
      throw new OdpsException("Quota name: " + quotaNickName + " , is not interactive quota.");
    }
    mcqaOdps.getRestClient().addUserDefinedHeader(Headers.ODPS_MCQA_CONN, mcqaConnectionHeader);
    log.add("Init MCQA 2.0 successfully");

    this.odpsNamespaceSchema = builder.isOdpsNamespaceSchema();
    this.useCommandApi = builder.isUseCommandApi();
    this.taskName = StringUtils.isNullOrEmpty(builder.getTaskName()) ? DEFAULT_TASK_NAME : builder.getTaskName();
    this.pool = builder.getPool();

    // For the job resumed from the specified instance job, we assume that it is a select job,
    // which will cause an error to be reported during getResult for non-select jobs.
    // However, for MCQA2.0 jobs, it is not necessary to call this interface, so this error reporting behavior may be acceptable.
    if (builder.getRecoverInstance() != null) {
      queryInfo = new QueryInfo("unknown", null, ExecuteMode.INTERACTIVE);
      queryInfo.setInstance(builder.getRecoverInstance(), ExecuteMode.INTERACTIVE, null, null);
      queryInfo.setSelect(true);
    }
  }

  @Override
  public void run(String sql, Map<String, String> hint) throws OdpsException {
    if (hint == null) {
      hint = new HashMap<>();
    } else {
      hint = new HashMap<>(hint);
    }
    queryInfo = new QueryInfo(sql, hint, ExecuteMode.INTERACTIVE);
    queryInfo.setCommandInfo(new CommandInfo(sql, hint));

    if (useCommandApi) {
      Command command = CommandUtil.parseCommand(sql);
      if (command != null) {
        queryInfo.getCommandInfo().setCommand(command);
        queryInfo.getCommandInfo().setOdpsNamespaceSchema(odpsNamespaceSchema);
        if (!command.isSync()) {
          command.run(odps, queryInfo.getCommandInfo());
        }
        parseSuccess = true;
        return;
      }
    }
    parseSuccess = false;
    Instance currentInstance =
        SQLTask.run(mcqaOdps, mcqaOdps.getDefaultProject(), sql, taskName, hint,
                    null);
    currentInstance.setMcqaV2(true);
    queryInfo.setInstance(currentInstance, ExecuteMode.INTERACTIVE, null, null);
    queryInfo.setSelect(isSelect(sql));
    log.add("Successfully submitted MCQA 2.0 Job, ID: " + currentInstance.getId());
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getTaskName() {
    return taskName;
  }

  @Override
  public String getQueryId() {
    if (queryInfo != null) {
      return queryInfo.getInstance().getId();
    }
    return null;
  }

  @Override
  public int getSubqueryId() {
    return -1;
  }

  @Override
  public String getLogView() {
    try {
      if (queryInfo != null && queryInfo.getInstance() != null) {
        return new LogView(odps).generateLogView(queryInfo.getInstance(), 7 * 24);
      }
    } catch (OdpsException e) {
      return null;
    }
    return null;
  }

  @Override
  public boolean isActive() {
    // in mcqa 1.0, this method will check session status,
    // however, in mcqa 2.0, no session is created, and this method will always return false.
    return false;
  }

  @Override
  public void cancel() throws OdpsException {
    if (queryInfo != null) {
      Instance instance = queryInfo.getInstance();
      try {
        instance.stop();
      } catch (OdpsException e) {
        // stop will throw exception when instance is not running, so we check here.
        if (!"InvalidStateSetting".equals(e.getErrorCode())) {
          throw e;
        }
      }
    }
  }

  @Override
  public Instance getInstance() {
    if (queryInfo != null) {
      return queryInfo.getInstance();
    } else {
      return null;
    }
  }

  @Override
  public List<Instance.StageProgress> getProgress() throws OdpsException {
    if (queryInfo != null) {
      return queryInfo.getInstance().getTaskProgress(taskName);
    } else {
      return null;
    }
  }

  @Override
  public List<String> getExecutionLog() {
    List<String> executionLog = new ArrayList<>(log);
    log.clear();
    return executionLog;
  }

  @Override
  public String getSummary() throws OdpsException {
    if (queryInfo == null || queryInfo.getInstance() == null) {
      return null;
    }
    try {
      return getTaskSummaryV1(odps, queryInfo.getInstance(), taskName);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  private static String getTaskSummaryV1(Odps odps, Instance i, String taskName)
      throws Exception {
    RestClient client = odps.getRestClient();
    Map<String, String> params = new HashMap<>();
    params.put("summary", null);
    params.put("taskname", taskName);
    String queryString = "/projects/" + i.getProject() + "/instances/" + i.getId();
    Response result = client.request(queryString, "GET", params, null, null);

    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode rootNode = objectMapper.readTree(new String(result.getBody(), StandardCharsets.UTF_8));
    return rootNode.path("mapReduce").path("summary").asText();
  }

  @Override
  public List<Record> getResult() throws OdpsException, IOException {
    return getResult(null);
  }

  @Override
  public List<Record> getResult(Long countLimit) throws OdpsException, IOException {
    return getResult(countLimit, null);
  }

  @Override
  public List<Record> getResult(Long countLimit, Long sizeLimit)
      throws OdpsException, IOException {
    return getResult(null, countLimit, sizeLimit);
  }

  @Override
  public List<Record> getResult(Long offset, Long countLimit, Long sizeLimit)
      throws OdpsException, IOException {
    return getResult(offset, countLimit, sizeLimit, false);
  }

  @Override
  public List<Record> getResult(Long offset, Long countLimit, Long sizeLimit,
                                boolean limitEnabled) throws OdpsException, IOException {

    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    if (parseSuccess) {
      return getCommandResult(offset, countLimit, sizeLimit, limitEnabled);
    }
    if (!useInstanceTunnel && offset != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    if (!useInstanceTunnel && countLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    if (!useInstanceTunnel && sizeLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    if (useInstanceTunnel) {
      return getResultByInstanceTunnel(offset, countLimit, sizeLimit, limitEnabled);
    } else {
      return getResultDirectly();
    }
  }

  /**
   * 获取command的结果, 同步的command在此处才真正的run并获得结果, 异步的command在此处检查instance成功之后返回结果
   *
   * @param offset       返回结果开始的行数,从第几行开始。在打开command api的情况下，异步command不受该参数限制，
   *                     见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param countLimit   返回结果的最大数目。在打开command api的情况下，异步command不受该参数限制，
   *                     见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param sizeLimit    在打开command api的情况下，同步和异步command均不受该参数限制。
   * @param limitEnabled 在打开command api的情况下，同步和异步command均不受该参数限制。
   * @return Record的集合
   * @throws OdpsException
   */
  private List<Record> getCommandResult(Long offset, Long countLimit, Long sizeLimit,
                                        boolean limitEnabled) throws OdpsException {
    if (offset != null && offset < 0L) {
      throw new IllegalArgumentException("illegal argument. offset = " + offset);
    }
    if (countLimit != null && countLimit < 0L) {
      throw new IllegalArgumentException("illegal argument. countLimit = " + countLimit);
    }
    Command command = queryInfo.getCommandInfo().getCommand();
    if (command.isSync()) {
      RecordIter recordIterator = command.run(odps, queryInfo.getCommandInfo());
      if (recordIterator == null) {
        return Collections.emptyList();
      }
      recordIterator.setCountLimit(countLimit == null ? -1L : countLimit);
      recordIterator.setOffset(offset == null ? 0L : offset);
      List<Record> records = new ArrayList<>();
      while (recordIterator.hasNext()) {
        Record record = recordIterator.next();
        records.add(record);
      }
      return records;
    }
    Instance instance = queryInfo.getCommandInfo().getInstance();
    instance.waitForSuccess();
    instance = queryInfo.getCommandInfo().getInstance();
    String res = instance.getTaskResults().get(queryInfo.getCommandInfo().getTaskName());

    return CommandUtil.toRecord(res, command.getResultHeaders().get(0));
  }

  private List<Record> getResultByInstanceTunnel(Long offset, Long countLimit, Long sizeLimit,
                                                 boolean limitEnabled)
      throws OdpsException, IOException {
    if (queryInfo.isSelect()) {
      queryInfo.getInstance().waitForTerminated(100, true);
      InstanceTunnel.DownloadSession downloadSession = null;
      try {
        downloadSession =
            instanceTunnel.createDownloadSession(odps.getDefaultProject(), queryInfo.getInstance().getId(),
                                         limitEnabled);
      } catch (OdpsException e) {
        if (e.getErrorCode().equals("TaskFailed")) {
          // wait for success will check task status and throw exception
          queryInfo.getInstance().waitForSuccess();
        } else {
          throw e;
        }
      }
      List<Record> records = new ArrayList<>();
      TunnelRecordReader
          reader =
          downloadSession
              .openRecordReader(offset == null ? 0 : offset,
                                countLimit == null ? downloadSession.getRecordCount() : countLimit,
                                sizeLimit == null ? Long.MAX_VALUE : sizeLimit);
      while (true) {
        Record record = reader.read();
        if (sizeLimit != null && sizeLimit > 0 && reader.getTotalBytes() > sizeLimit) {
          throw new RuntimeException(
              "InvalidArgument: sizeLimit, fetched data is larger than limit size");
        }
        if (record == null) {
          break;
        } else {
          records.add(record);
        }
      }
      return records;
    } else {
      queryInfo.getInstance().waitForSuccess();
      Map<String, String> results = queryInfo.getInstance().getTaskResults();
      String selectResult = results.get(taskName);
      if (StringUtils.isNullOrEmpty(selectResult)) {
        return new ArrayList<>();
      }
      return CommandUtil.toRecord(selectResult, "Info");
    }
  }

  private List<Record> getResultDirectly() throws OdpsException {
    String resultStr = getResultString();
    if (resultStr != null) {
      if (queryInfo.isSelect()) {
        try {
          return SQLTask.parseCsvRecord(resultStr);
        } catch (Exception e) {
          throw new OdpsException(resultStr, e);
        }
      } else {
        // non-select command but with result
        return CommandUtil.toRecord(resultStr, "Info");
      }
    }
    return new ArrayList<>();
  }

  private String getResultString() throws OdpsException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    Instance currentInstance = queryInfo.getInstance();
    if (currentInstance.isSync()) {
      return getSyncResultStr();
    } else {
      return currentInstance.waitForTerminatedAndGetResult();
    }
  }

  private String getSyncResultStr() throws OdpsException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    Instance currentInstance = queryInfo.getInstance();
    Instance.InstanceResultModel.TaskResult
        taskResult =
        currentInstance.getRawTaskResults().get(0);
    Instance.TaskStatus.Status
        taskStatus =
        Instance.TaskStatus.Status.valueOf(taskResult.getStatus().toUpperCase());
    if (taskStatus == Instance.TaskStatus.Status.FAILED) {
      throw new OdpsException(taskResult.getResult().getString());
    } else if (taskStatus != Instance.TaskStatus.Status.SUCCESS) {
      throw new OdpsException("Status=" + taskResult.getStatus() + ", Result="
                              + taskResult.getResult().getString());
    }
    return taskResult.getResult().getString();
  }

  @Override
  public ResultSet getResultSet() throws OdpsException, IOException {
    return getResultSet(null);
  }

  @Override
  public ResultSet getResultSet(Long countLimit) throws OdpsException, IOException {
    return getResultSet(countLimit, null);
  }

  @Override
  public ResultSet getResultSet(Long countLimit, Long sizeLimit)
      throws OdpsException, IOException {
    return getResultSet(null, countLimit, sizeLimit);
  }

  @Override
  public ResultSet getResultSet(Long offset, Long countLimit, Long sizeLimit)
      throws OdpsException, IOException {
    return getResultSet(offset, countLimit, sizeLimit, false);
  }

  @Override
  public ResultSet getResultSet(Long offset, Long countLimit, Long sizeLimit,
                                boolean limitEnabled) throws OdpsException, IOException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    if (parseSuccess) {
      return getCommandResultSet(offset, countLimit, sizeLimit, limitEnabled);
    }
    if (!useInstanceTunnel && offset != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    if (!useInstanceTunnel && countLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    if (!useInstanceTunnel && sizeLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    if (useInstanceTunnel) {
      return getResultSetByInstanceTunnel(offset, countLimit, sizeLimit, limitEnabled);
    } else {
      return getResultSetDirectly();
    }
  }

  /**
   * 获取command的结果, 同步的command在此处才真正的run并获得结果, 异步的command在此处检查instance成功之后返回结果。
   *
   * @param offset       返回结果开始的行数,从第几行开始。在打开command api的情况下，异步command不受该参数限制，
   *                     见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param countLimit   返回结果的最大数目。在打开command api的情况下，异步command不受该参数限制，
   *                     见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param sizeLimit    同步和异步command均不受该参数限制。
   * @param limitEnabled 同步和异步command均不受该参数限制。
   * @return 结果封装为ResultSet形式。其中，同步command运行结果的recordCount不可知，规定为-1。
   * @throws OdpsException
   */
  private ResultSet getCommandResultSet(Long offset, Long countLimit, Long sizeLimit,
                                        boolean limitEnabled)
      throws OdpsException {
    if (offset != null && offset < 0L) {
      throw new IllegalArgumentException("illegal argument. offset = " + offset);
    }
    if (countLimit != null && countLimit < 0L) {
      throw new IllegalArgumentException("illegal argument. countLimit = " + countLimit);
    }

    Command command = queryInfo.getCommandInfo().getCommand();
    // 同步直接阻塞返回结果
    if (command.isSync()) {
      RecordIter recordIterator = command.run(odps, queryInfo.getCommandInfo());
      if (recordIterator == null) {
        // empty result set
        return new ResultSet(
            new InMemoryRecordIterator(new ArrayList<>()),
            new TableSchema(),
            0
        );
      }
      recordIterator.setCountLimit(countLimit == null ? -1L : countLimit);
      recordIterator.setOffset(offset == null ? 0L : offset);
      TableSchema schema = new TableSchema();
      schema.setColumns(Arrays.asList(recordIterator.getColumns()));
      return new ResultSet(recordIterator, schema, -1);
    }
    Instance instance = queryInfo.getCommandInfo().getInstance();
    instance.waitForSuccess();
    instance = queryInfo.getCommandInfo().getInstance();
    String res = instance.getTaskResults().get(queryInfo.getCommandInfo().getTaskName());
    List<Record> records = CommandUtil.toRecord(res, command.getResultHeaders().get(0));
    TableSchema schema = new TableSchema();
    schema.setColumns(Arrays.asList(records.get(0).getColumns()));
    return new ResultSet(records.iterator(), schema, records.size());
  }


  private ResultSet getResultSetDirectly() throws OdpsException {
    String resultStr = getResultString();
    if (!StringUtils.isNullOrEmpty(resultStr)) {
      if (queryInfo.isSelect()) {
        CSVRecordParser.ParseResult parseResult;
        try {
          parseResult = CSVRecordParser.parse(resultStr);
        } catch (Exception e) {
          throw new OdpsException(resultStr);
        }
        List<Record> records = parseResult.getRecords();
        return new ResultSet(
            new InMemoryRecordIterator(records),
            parseResult.getSchema(),
            records.size());
      } else {
        List<Record> records = CommandUtil.toRecord(resultStr, "Info");
        TableSchema schema = new TableSchema();
        schema.setColumns(Arrays.asList(records.get(0).getColumns()));
        return new ResultSet(
            new InMemoryRecordIterator(records),
            schema,
            records.size());
      }
    } else {
      // empty result set
      return new ResultSet(
          new InMemoryRecordIterator(new ArrayList<>()),
          new TableSchema(),
          0
      );
    }
  }

  private ResultSet getResultSetByInstanceTunnel(Long offset, Long countLimit, Long sizeLimit,
                                                 boolean limitEnabled)
      throws OdpsException, IOException {
    if (queryInfo.isSelect()) {
      queryInfo.getInstance().waitForTerminated(100, true);
      InstanceTunnel.DownloadSession downloadSession = null;
      try {
        downloadSession =
            instanceTunnel.createDownloadSession(odps.getDefaultProject(), queryInfo.getInstance().getId(),
                                         limitEnabled);
      } catch (TunnelException e) {
        if (e.getErrorCode().equals(SQLExecutorConstants.sessionNotSelectException)
            || e.getErrorMsg().contains(SQLExecutorConstants.sessionNotSelectMessage)) {
          queryInfo.setSelect(false);
          return getResultSetByInstanceTunnel(offset, countLimit, sizeLimit, limitEnabled);
        }
        if (e.getErrorCode().equals("TaskFailed")) {
          // wait for success will check task status and throw exception
          queryInfo.getInstance().waitForSuccess();
        } else {
          throw e;
        }
      }
      List<Record> records = new ArrayList<>();
      TunnelRecordReader
          reader =
          downloadSession
              .openRecordReader(offset == null ? 0 : offset,
                                countLimit == null ? downloadSession.getRecordCount() : countLimit,
                                sizeLimit == null ? Long.MAX_VALUE : sizeLimit);
      while (true) {
        Record record = reader.read();
        if (sizeLimit != null && sizeLimit > 0 && reader.getTotalBytes() > sizeLimit) {
          throw new RuntimeException(
              "InvalidArgument: sizeLimit, fetched data is larger than limit size");
        }
        if (record == null) {
          break;
        } else {
          records.add(record);
        }
      }
      TableSchema schema = downloadSession.getSchema();
      return new ResultSet(
          new InMemoryRecordIterator(records),
          schema,
          records.size());
    } else {
      queryInfo.getInstance().waitForSuccess();
      Map<String, String> results = queryInfo.getInstance().getTaskResults();
      String selectResult = results.get(taskName);
      if (StringUtils.isNullOrEmpty(selectResult)) {
        // empty result set
        return new ResultSet(
            new InMemoryRecordIterator(new ArrayList<>()),
            new TableSchema(),
            0
        );
      }
      List<Record> records = CommandUtil.toRecord(selectResult, "Info");
      TableSchema schema = new TableSchema();
      schema.setColumns(Arrays.asList(records.get(0).getColumns()));
      return new ResultSet(
          new InMemoryRecordIterator(records),
          schema,
          records.size());
    }
  }


  @Override
  public boolean hasResultSet() {
    return SqlParserUtil.hasResultSet(queryInfo.getSql());
  }

  @Override
  public boolean isRunningInInteractiveMode() {
    return true;
  }

  @Override
  public void close() {
    if (pool != null) {
      pool.releaseExecutor(this);
    }
  }

  @Override
  public boolean isUseInstanceTunnel() {
    return useInstanceTunnel;
  }

  public boolean isSelect(String sql) throws OdpsException {
    try {
      return SqlParserUtil.isSelect(sql);
    } catch (SQLException e) {
      throw new OdpsException("Sql isSelect failed", e);
    }
  }
}
