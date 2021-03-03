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

package com.aliyun.odps.sqa;

import com.aliyun.odps.*;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.utils.CSVRecordParser;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.odps.TunnelEndpointLocalCache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

class SQLExecutorImpl implements SQLExecutor {

  private String id = null;

  private Odps odps = null;
  private ExecuteMode executeMode = ExecuteMode.INTERACTIVE;
  private FallbackPolicy fallbackPolicy = null;
  private boolean enableReattach = true;
  private boolean useInstanceTunnel = true;
  private Map<String, String> properties = new HashMap<>();
  private String serviceName;
  private String taskName;
  private String runningCluster;
  private int tunnelGetResultMaxRetryTime;
  private int tunnelGetResultRetryCount = 0;
  private static final long cacheSize = 1000;
  private static final int durationSeconds = 900;
  private static TunnelEndpointLocalCache cache = new TunnelEndpointLocalCache(cacheSize,durationSeconds);

  private InstanceTunnel instanceTunnel = null;
  private SQLExecutorPool pool = null;
  // current SQLRT
  Session session = null;

  // current query info
  QueryInfo queryInfo = null;

  enum TunnelRetryStatus {
    NEED_RETRY,
    NON_SELECT_QUERY,
    QUERY_FAILED
  }

  /**
   * 创建一个Executor
   *
   * @param odps
   *     odps 对象
   * @param serviceName
   *     指定 service 的名字
   * @param taskName
   *     task名字
   * @param tunnelEndpoint
   *     默认tunnel地址,若不配置,则走路由
   * @param properties
   *     执行连接参数
   * @param executeMode
   *     默认执行模式,INTERACTIVE/OFFLINE
   * @param fallbackPolicy
   *     允许回退到离线执行的策略
   * @param enableReattach
   *     允许内部重连
   * @param useInstanceTunnel
   *     默认使用instanceTunnel获取结果,如果设置为false,获取的结果将会全部转为string
   * @param pool
   *     如果为连接池模式,则保存连接池,在close时将Executor释放回连接池,否则为null
   * @param recoverInstance
   *     若设置,则从该instance恢复状态,若该instance无效或未设置,则重新attach
   * @param runningCluster
   *     设置运行集群
   * @return
   * @throws OdpsException
   */
  SQLExecutorImpl(Odps odps,
                  String serviceName,
                  String taskName,
                  String tunnelEndpoint,
                  Map<String, String> properties,
                  ExecuteMode executeMode,
                  FallbackPolicy fallbackPolicy,
                  boolean enableReattach,
                  boolean useInstanceTunnel,
                  SQLExecutorPool pool,
                  Instance recoverInstance,
                  String runningCluster,
                  int tunnelGetResultMaxRetryTime) throws OdpsException {
    this.properties.putAll(properties);
    this.serviceName = serviceName;
    this.taskName = taskName;
    this.odps = odps;
    this.executeMode = executeMode;
    this.fallbackPolicy = fallbackPolicy;
    this.enableReattach = enableReattach;
    this.useInstanceTunnel = useInstanceTunnel;
    this.pool = pool;
    this.runningCluster = runningCluster;
    this.tunnelGetResultMaxRetryTime = tunnelGetResultMaxRetryTime;
    if (executeMode.equals(ExecuteMode.INTERACTIVE)) {
      // try recover
      if (recoverInstance != null) {
        if (recoverInstance.getStatus() == Instance.Status.RUNNING) {
          session = new Session(odps, recoverInstance);
        }
      }
      // recover failed, attach new session
      if (session == null) {
        session = Session.attach(odps, serviceName, properties, SQLExecutorConstants.DEFAULT_ATTACH_TIMEOUT, runningCluster, taskName);
      }
    } else if (executeMode.equals(ExecuteMode.OFFLINE)) {
      // pass
    } else {
      throw new OdpsException("Invalid execution mode, can not init with NONE.");
    }
    // each executor has a uuid
    this.id = UUID.randomUUID().toString();
    if (useInstanceTunnel) {
      instanceTunnel = new InstanceTunnel(odps);
      if (StringUtils.isNullOrEmpty(tunnelEndpoint)) {
        //try to get tunnelEndpoint from local cache
        try {
          tunnelEndpoint = cache.getTunnelEndpointFromLocalCache(odps);
        }
        catch (ExecutionException e)
        {
          throw new OdpsException("Get tunnel endpoint from localCache exception:" + e.getMessage());
        }
        instanceTunnel.setEndpoint(tunnelEndpoint);
      } else {
        instanceTunnel.setEndpoint(tunnelEndpoint);
      }
    }
  }

  /**
   * 默认行为关闭该executor,若为连接池模式,则归还Executor到连接池中
   * @return
   * @throws
   */
  @Override
  public void close() {
    if (pool != null) {
      pool.releaseExecutor(this);
    } else {
      try {
        if (session != null) {
          session.stop();
        }
      } catch (OdpsException e) {
        // ignore
      }
    }
  }

  /**
   * 获取Executor的ID

   * @return
   * @throws
   */
  @Override
  public String getId() {
    return id;
  }

  /**
   * 获取当前queryId

   * @return
   * @throws
   */
  public String getQueryId() {
    if (queryInfo != null && executeMode.equals(ExecuteMode.INTERACTIVE)) {
      return queryInfo.getInstance().getId() + "_" + queryInfo.getId();
    }
    return null;
  }

  /**
   * 获取当前query Logview

   * @return
   * @throws
   */
  @Override
  public String getLogView() {
    if (queryInfo != null) {
      // if query running, return query logview
      try {
        if (queryInfo.getExecuteMode().equals(ExecuteMode.INTERACTIVE)) {
          return new LogView(odps).generateSubQueryLogView(
                  queryInfo.getInstance(), queryInfo.getId(), 7 * 24);
        } else {
          return new LogView(odps).generateLogView(
                  queryInfo.getInstance(), 7 * 24);
        }
      } catch (OdpsException e) {
        return null;
      }
    } else if (session != null) {
      // no query running, return session logview if have
      return session.getLogView();
    }
    return null;
  }

  /**
   * 检查该Executor是否活跃

   * @return 是否存活
   * @throws
   */
  @Override
  public boolean isActive() {
    if (session != null) {
      return session.getInstance().getStatus().equals(Instance.Status.RUNNING);
    }
    return false;
  }

  /**
   * 取消当前查询

   * @return
   * @throws
   */
  @Override
  public void cancel() throws OdpsException {
    if (queryInfo != null) {
      if (queryInfo.getExecuteMode().equals(ExecuteMode.OFFLINE)) {
        // fallback to offline
        queryInfo.getInstance().stop();
      } else {
        session.cancelQuery(queryInfo.getId());
      }
    }
  }

  /**
   * 获取当前查询Instance

   * @return
   * @throws
   */
  public Instance getInstance() {
    if (queryInfo != null) {
      // current query is running
      return queryInfo.getInstance();
    } else if (session != null) {
      // session mode
      return session.getInstance();
    } else {
      return null;
    }
  }

  /**
   * 获取当前查询的进度信息

   * @return 各阶段进度信息
   * @throws OdpsException
   */
  public List<Instance.StageProgress> getProgress() throws OdpsException {
    if (queryInfo == null) {
      return null;
    }
    if (queryInfo.getExecuteMode().equals(ExecuteMode.OFFLINE)) {
      return queryInfo.getInstance().getTaskProgress(SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME);
    } else {
      return session.getInstance().getTaskProgress(taskName);
    }
  }


  /**
   * 获取当前查询的Summary信息

   * @return SQL summary
   * @throws OdpsException
   */
  public String getSummary() throws OdpsException  {
    if (queryInfo == null) {
      return null;
    }
    if (queryInfo.getExecuteMode().equals(ExecuteMode.OFFLINE)) {
      return queryInfo.getInstance().getTaskSummary(SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME).getSummaryText();
    } else {
      return session.getQueryStats(queryInfo.getId());
    }
  }

  /**
   * 获取当前查询的执行日志

   * @return 执行信息,包括回退等消息
   * @throws OdpsException
   */
  public List<String> getExecutionLog(){
    if (queryInfo == null) {
      return null;
    }
    return queryInfo.getAndCleanExecutionLog();
  }

  /**
   * 获取当前query的所有结果
   *
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  @Override
  public List<Record> getResult()
          throws OdpsException, IOException  {
    return getResult(null);
  }

  /**
   * 通过InstanceTunnel获取当前query的结果的迭代器
   *
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  @Override
  public ResultSet getResultSet()
          throws OdpsException, IOException {
    return getResultSet(null);
  }

  /**
   * 获取有限集结果

   * @param countLimit
   *     返回结果数量
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  @Override
  public List<Record> getResult(Long countLimit)
          throws OdpsException, IOException  {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    if (!useInstanceTunnel && countLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you wants to get limited result.");
    }
    try {
      return getResultInternal(countLimit, null);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 获取有限集结果

   * @param countLimit
   *     返回结果数量
   * @param sizeLimit
   *     返回结果大小(Bytes)
   * 如果超过count大小会截断
   * 如果超过sizeLimit会直接抛出异常
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  @Override
  public List<Record> getResult(Long countLimit, Long sizeLimit)
      throws OdpsException, IOException  {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    if (!useInstanceTunnel && countLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you wants to get limited result.");
    }
    if (!useInstanceTunnel && sizeLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you wants to get limited result.");
    }
    try {
      return getResultInternal(countLimit, sizeLimit);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 通过InstanceTunnel获取有限集结果的迭代器

   * @param countLimit
   *     返回结果数量
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  @Override
  public ResultSet getResultSet(Long countLimit)
      throws OdpsException, IOException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    if (!useInstanceTunnel && countLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you wants to get limited result.");
    }
    try {
      return getResultSetInternal(countLimit, null);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 通过InstanceTunnel获取有限集结果的迭代器

   * @param countLimit
   *     返回结果数量
   * @param sizeLimit
   *     返回结果大小(Bytes)
   * 如果超过count大小会截断
   * 如果超过sizeLimit会直接抛出异常
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  @Override
  public ResultSet getResultSet(Long countLimit, Long sizeLimit)
          throws OdpsException, IOException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    if (!useInstanceTunnel && countLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you wants to get limited result.");
    }
    if (!useInstanceTunnel && sizeLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you wants to get limited result.");
    }
    try {
      return getResultSetInternal(countLimit, sizeLimit);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 提交一个query
   *
   * @param sql
   *     sql语句
   * @param hint
   *     query需要的hint参数
   * @return
   * @throws OdpsException
   */
  @Override
  public void run(String sql, Map<String, String> hint) throws OdpsException {
    queryInfo = new QueryInfo(sql, hint, executeMode);
    //reset tunnelGetResultRetryCount before query run
    tunnelGetResultRetryCount = 0;
    try {
      runQueryInternal(executeMode, null);
    } catch (Exception e) {
      throw e;
    }
  }

  private void reattach(String errorMessage) throws OdpsException {
    if (enableReattach) {
      session = Session.attach(odps, serviceName, properties, SQLExecutorConstants.DEFAULT_ATTACH_TIMEOUT, runningCluster, taskName);
    } else {
      throw new OdpsException(errorMessage);
    }
  }

  private ExecuteMode handleSessionException(String errorMessage) throws OdpsException {
    if (errorMessage.indexOf(SQLExecutorConstants.sessionReattachFlag) != -1) {
      reattach(errorMessage);
      return ExecuteMode.INTERACTIVE;
    } else if (fallbackPolicy.isFallback4UnsupportedFeature()
            && errorMessage.indexOf(SQLExecutorConstants.sessionUnsupportedFeatureFlag) != -1) {
      return ExecuteMode.OFFLINE;
    } else if (fallbackPolicy.isFallback4Upgrading()
            && errorMessage.indexOf(SQLExecutorConstants.sessionUnavailableFlag) != -1) {
      return ExecuteMode.OFFLINE;
    } else if (fallbackPolicy.isFallback4Upgrading()
            && errorMessage.indexOf(SQLExecutorConstants.sessionAccessDenyFlag) != -1) {
      return ExecuteMode.OFFLINE;
    } else if (fallbackPolicy.isFallback4ResourceNotEnough()
            && errorMessage.indexOf(SQLExecutorConstants.sessionResourceNotEnoughFlag) != -1) {
      return ExecuteMode.OFFLINE;
    } else if (fallbackPolicy.isFallback4RunningTimeout()
            && errorMessage.indexOf(SQLExecutorConstants.sessionQueryTimeoutFlag) != -1) {
      return ExecuteMode.OFFLINE;
    } else if (fallbackPolicy.isFallback4UnknownError()
            && errorMessage.indexOf(SQLExecutorConstants.sessionExceptionFlag) != -1) {
      return ExecuteMode.OFFLINE;
    } else {
      throw new OdpsException(errorMessage);
    }
  }

  private boolean checkIsSelect(String errorCode, String errorMessage) throws OdpsException {
    if (errorCode.equals(SQLExecutorConstants.sessionNotSelectException)
            || errorMessage.indexOf(SQLExecutorConstants.sessionNotSelectMessage) != -1) {
      queryInfo.setSelect(false);
      return false;
    }
    return true;
  }

  private TunnelRetryStatus handleTunnelException(String errorCode, String errorMessage) throws OdpsException {
    boolean isSelect = checkIsSelect(errorCode, errorMessage);
    if (!isSelect) {
      // tunnel do not support non-select query, double check task result
      try {
        session.getSubQueryResult(queryInfo.getId());
        // query success
        return TunnelRetryStatus.NON_SELECT_QUERY;
      } catch (OdpsException e) {
        // query failed
        return TunnelRetryStatus.QUERY_FAILED;
      }
    }
    if (errorCode.equals(SQLExecutorConstants.sessionTunnelTimeoutFlag)
            || errorMessage.indexOf(SQLExecutorConstants.sessionTunnelTimeoutMessage) != -1) {
      // get result timeout
      tunnelGetResultRetryCount++;
      if(tunnelGetResultRetryCount >= tunnelGetResultMaxRetryTime){
        throw new OdpsException(errorCode + ":" + errorMessage);
      }
      return TunnelRetryStatus.NEED_RETRY;
    }
    return TunnelRetryStatus.QUERY_FAILED;
  }

  private ResultSet newEmptyResultSet() {
    return new ResultSet(
            new SessionRecordSetIterator(new ArrayList<>()),
            null,
            0
    );
  }

  private List<Record> getResultInternal(Long countLimit, Long sizeLimit)
          throws OdpsException, IOException  {
    if (useInstanceTunnel) {
      if (queryInfo.getExecuteMode() == ExecuteMode.INTERACTIVE) {
        return getSessionResultByInstanceTunnel(countLimit, sizeLimit);
      } else {
        return getOfflineResultByInstanceTunnel(countLimit);
      }
    } else {
      if (queryInfo.getExecuteMode() == ExecuteMode.INTERACTIVE) {
        return getSessionResult();
      } else {
        return getOfflineResult();
      }
    }
  }

  private ResultSet getResultSetInternal(Long countLimit, Long sizeLimit)
          throws OdpsException, IOException {
    if (useInstanceTunnel) {
      if (queryInfo.getExecuteMode() == ExecuteMode.INTERACTIVE) {
        return getSessionResultSetByInstanceTunnel(countLimit, sizeLimit);
      } else {
        return getOfflineResultSetByInstanceTunnel(countLimit);
      }
    } else {
      if (queryInfo.getExecuteMode() == ExecuteMode.INTERACTIVE) {
        return getSessionResultSet();
      } else {
        return getOfflineResultSet();
      }
    }
  }

  private List<Record> getSessionResult ()
          throws OdpsException, IOException {
    Session.SubQueryResult result = null;
    try {
      result = session.getSubQueryResult(queryInfo.getId());
    } catch (OdpsException e) {
      ExecuteMode executeMode = handleSessionException(e.getMessage());
      runQueryInternal(executeMode, e.getMessage());
      return getResultInternal(null, null);
    }
    return result.getRecords();
  }

  private List<Record> getSessionResultByInstanceTunnel(Long countLimit, Long sizeLimit)
          throws OdpsException, IOException  {
    Instance instance = queryInfo.getInstance();
    List<Record> records = new ArrayList<>();
    try {
      InstanceTunnel.DownloadSession downloadSession =
              instanceTunnel.createDirectDownloadSession(
                      instance.getProject(),
                      instance.getId(),
                      taskName,
                      queryInfo.getId());
      TunnelRecordReader reader = downloadSession.openRecordReader(0, countLimit == null ? -1 : countLimit, sizeLimit == null ? -1 : sizeLimit);
      while (true) {
        Record record = reader.read();
        if (record == null) {
          break;
        } else {
          records.add(record);
        }
      }
    } catch (TunnelException e) {
      TunnelRetryStatus tunnelRetryStatus = handleTunnelException(e.getErrorCode(), e.getMessage());
      if (tunnelRetryStatus.equals(TunnelRetryStatus.NEED_RETRY)) {
        return getResultInternal(countLimit, sizeLimit);
      } else if (tunnelRetryStatus.equals(TunnelRetryStatus.NON_SELECT_QUERY)) {
        return records;
      } else {
        ExecuteMode executeMode = handleSessionException(e.getMessage());
        runQueryInternal(executeMode, e.getMessage());
        return getResultInternal(countLimit, sizeLimit);
      }
    }
    return records;
  }

  private List<Record> getOfflineResult()
          throws OdpsException, IOException {
    queryInfo.getInstance().waitForSuccess();
    return SQLTask.getResult(queryInfo.getInstance(), SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME);
  }

  private List<Record> getOfflineResultByInstanceTunnel(Long limit)
          throws OdpsException, IOException {
    queryInfo.getInstance().waitForSuccess();
    if (queryInfo.isSelect()) {
      return SQLTask.getResultByInstanceTunnel(queryInfo.getInstance(), SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME, limit);
    } else {
      return SQLTask.getResult(queryInfo.getInstance(), SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME);
    }
  }

  private ResultSet getSessionResultSet()
          throws OdpsException, IOException  {
    Session.SubQueryResult result = null;
    try {
      result = session.getSubQueryResult(queryInfo.getId());
    } catch (OdpsException e) {
      ExecuteMode executeMode = handleSessionException(e.getMessage());
      runQueryInternal(executeMode, e.getMessage());
      return getResultSetInternal(null, null);
    }
    return new ResultSet(
            new SessionRecordSetIterator(result.getRecords()),
            result.getSchema(),
            result.getRecords().size());
  }

  private ResultSet getSessionResultSetByInstanceTunnel(Long countLimit, Long sizeLimit)
          throws OdpsException, IOException  {
    Instance instance = queryInfo.getInstance();
    InstanceTunnel.DownloadSession downloadSession = null;
    TunnelRecordReader reader = null;
    try {
      downloadSession =
              instanceTunnel.createDirectDownloadSession(
                      instance.getProject(),
                      instance.getId(),
                      taskName,
                      queryInfo.getId());
      reader = downloadSession.openRecordReader(0, countLimit == null ? -1 : countLimit, sizeLimit == null ? -1 : sizeLimit);
    } catch (TunnelException e) {
      TunnelRetryStatus tunnelRetryStatus = handleTunnelException(e.getErrorCode(), e.getMessage());
      if (tunnelRetryStatus.equals(TunnelRetryStatus.NEED_RETRY)) {
        return getResultSetInternal(countLimit, sizeLimit);
      } else if (tunnelRetryStatus.equals(TunnelRetryStatus.NON_SELECT_QUERY)) {
        return newEmptyResultSet();
      } else {
        ExecuteMode executeMode = handleSessionException(e.getMessage());
        runQueryInternal(executeMode, e.getMessage());
        return getResultSetInternal(countLimit, sizeLimit);
      }
    }
    return new ResultSet(
            new SessionRecordSetIterator(reader),
            downloadSession.getSchema(),
            downloadSession.getRecordCount());
  }


  private ResultSet getOfflineResultSet()
          throws OdpsException, IOException  {
    queryInfo.getInstance().waitForSuccess();
    Map<String, String> results = queryInfo.getInstance().getTaskResults();
    String selectResult = results.get(SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME);
    if (!StringUtils.isNullOrEmpty(selectResult)) {
      CSVRecordParser.ParseResult parseResult = CSVRecordParser.parse(selectResult);
      List<Record> records = parseResult.getRecords();
      return new ResultSet(
              new SessionRecordSetIterator(records),
              parseResult.getSchema(),
              records.size());
    } else {
      return newEmptyResultSet();
    }
  }

  private ResultSet getOfflineResultSetByInstanceTunnel(Long limit)
          throws OdpsException, IOException {
    queryInfo.getInstance().waitForSuccess();
    // getResultSet will use instance tunnel, which do not support non-select query
    if (queryInfo.isSelect()) {
      return SQLTask.getResultSet(queryInfo.getInstance(), SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME, limit);
    } else {
      return newEmptyResultSet();
    }
  }

  private void runInSessionWithRetry(String rerunMsg) throws OdpsException {
    Session.SubQueryInfo subQueryInfo = session.runSubQuery(queryInfo.getSql(), queryInfo.getHint());
    if (subQueryInfo.status.equals(Session.SubQueryInfo.kOKCode)) {
      if (subQueryInfo.queryId == -1) {
        ExecuteMode executeMode = handleSessionException(subQueryInfo.result);
        runQueryInternal(executeMode, subQueryInfo.result);
      } else {
        // submit success
        queryInfo.setId(subQueryInfo.queryId);
        queryInfo.setInstance(session.getInstance(),
                ExecuteMode.INTERACTIVE,
                new LogView(odps).generateSubQueryLogView(session.getInstance(), subQueryInfo.queryId, 7 * 24),
                rerunMsg);
      }
    } else if (subQueryInfo.status.equals(Session.SubQueryInfo.kNotFoundCode)) {
      // odps worker cannot found instance, may stopped, reattach and retry
      String taskTerminateMsg = session.getInstance().getTaskResults().get(taskName);
      reattach("Submit query failed:" + taskTerminateMsg);
      runQueryInternal(ExecuteMode.INTERACTIVE, taskTerminateMsg);
    } else {
      // submit failed
      throw new OdpsException("Submit query failed:" + subQueryInfo.result);
    }
  }

  private void runInOffline(String rerunMsg) throws OdpsException {
    Instance instance = SQLTask.run(
            odps,
            odps.getDefaultProject(),
            queryInfo.getSql(),
            SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME,
            queryInfo.getHint(),
            null);

    queryInfo.setInstance(instance, ExecuteMode.OFFLINE, new LogView(odps).generateLogView(instance, 7 * 24), rerunMsg);
  }

  private void runQueryInternal(ExecuteMode executeMode, String rerunMsg) throws OdpsException {
    if (queryInfo.getRetry() < SQLExecutorConstants.MaxRetryTimes) {
      queryInfo.incRetry();
      if (executeMode == ExecuteMode.OFFLINE) {
        runInOffline(rerunMsg);
      } else {
        runInSessionWithRetry(rerunMsg);
      }
    } else {
      throw new OdpsException(rerunMsg);
    }
  }
}

/**
 * class: SessionRecordSetIterator
 * It is both used in getSessionResultSetByInstanceTunnel and getOfflineResultSet
 * In session mode: it opens a real reader,
 *      records are read from tunnel in batches
 * In offline mode: it passes a List to fake a reader,
 *      so reader is null
 */
class SessionRecordSetIterator implements Iterator<Record> {
  private static final long FETCH_COUNT = 1000L;
  private long actualFetchSize = 0;
  private int idx = 0;
  private boolean finishIterRead = false;
  private TunnelRecordReader reader;
  private int cursor = 0;
  private List<Record> buffer;

  public SessionRecordSetIterator(TunnelRecordReader reader) {
    this.reader = reader;
  }

  public SessionRecordSetIterator(List<Record> buffer) {
    this.buffer = buffer;
  }

  @Override
  public boolean hasNext() {
    //In offline mode
    if (reader == null){
      if (buffer == null){
        return false;
      }
      return cursor < buffer.size();
    }
    //In session mode
    else{
      if (idx == actualFetchSize && finishIterRead){
        return false;
      }
      if (buffer == null || idx == actualFetchSize) {
        fillBuffer();
      }
      return idx < actualFetchSize;
    }
  }

  @Override
  public Record next() {
    //In offline mode
    if (reader == null){
      return buffer.get(cursor++);
    }
    //In session mode
    else{
      return buffer.get(idx++);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  private void fillBuffer() {
    idx = 0;
    actualFetchSize = 0;
    buffer = new ArrayList<>();
    Record r = null;
    try {
      while (actualFetchSize < FETCH_COUNT){
        if ((r = reader.read()) != null) {
          buffer.add(r);
          actualFetchSize++;
        }
        else{
          break;
        }
      }
      if(actualFetchSize != FETCH_COUNT){
        finishIterRead = true;
      }
    } catch (IOException e) {
      throw new RuntimeException("Read result failed:" + e.getMessage(), e);
    }
  }
}
