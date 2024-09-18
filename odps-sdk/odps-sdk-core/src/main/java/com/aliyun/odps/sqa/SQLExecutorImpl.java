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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Session;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TunnelEndpointLocalCache;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.sqa.commandapi.Command;
import com.aliyun.odps.sqa.commandapi.CommandInfo;
import com.aliyun.odps.sqa.commandapi.RecordIter;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.sqa.commandapi.utils.SqlParserUtil;
import com.aliyun.odps.sqa.v2.InMemoryRecordIterator;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.utils.CSVRecordParser;
import com.aliyun.odps.utils.StringUtils;

public class SQLExecutorImpl implements SQLExecutor {

  private String id = null;

  private Odps odps = null;
  private ExecuteMode executeMode = ExecuteMode.INTERACTIVE;
  private FallbackPolicy fallbackPolicy = null;
  private boolean enableReattach = true;
  private boolean useInstanceTunnel = true;
  private boolean attachSuccess = false;
  private Map<String, String> properties = new HashMap<>();
  private String serviceName;
  private String taskName;
  private String runningCluster;
  private String fallbackQuota;
  private int tunnelGetResultMaxRetryTime;
  private int tunnelGetResultRetryCount = 0;
  private Integer offlineJobPriority;
  private static final long cacheSize = 1000;
  private static final int durationSeconds = 900;
  private static TunnelEndpointLocalCache cache = new TunnelEndpointLocalCache(cacheSize,
      durationSeconds);
  private Long attachTimeout = SQLExecutorConstants.DEFAULT_ATTACH_TIMEOUT;

  private InstanceTunnel instanceTunnel = null;
  private SQLExecutorPool pool = null;
  // current SQLRT
  Session session = null;

  // current query info
  QueryInfo queryInfo = null;

  // enable Command API
  private CommandApi commandApi = null;
  private boolean useCommandApi = false;
  private boolean parseSuccess = false;
  private boolean sessionSupportNonSelect = false;

  // 三层模型开关
  private boolean odpsNamespaceSchema = false;
  private String tunnelEndpoint;

  enum TunnelRetryStatus {
    NEED_RETRY,
    NON_SELECT_QUERY,
    QUERY_FAILED
  }

  private class TunnelRetryInfo {

    public TunnelRetryStatus status;
    public String errCode;
    public String errMsg;
  }

  /**
   * 创建一个Executor
   *
   * @param odps              odps 对象
   * @param serviceName       指定 service 的名字
   * @param taskName          task名字
   * @param tunnelEndpoint    默认tunnel地址,若不配置,则走路由
   * @param properties        执行连接参数
   * @param executeMode       默认执行模式,INTERACTIVE/OFFLINE
   * @param fallbackPolicy    允许回退到离线执行的策略
   * @param enableReattach    允许内部重连
   * @param useInstanceTunnel 默认使用instanceTunnel获取结果,如果设置为false,获取的结果将会全部转为string
   * @param pool              如果为连接池模式,则保存连接池,在close时将Executor释放回连接池,否则为null
   * @param recoverInstance   若设置,则从该instance恢复状态,若该instance无效或未设置,则重新attach
   * @param runningCluster    设置运行集群
   * @param useCommandApi     是否开启CommandApi，开启后将能够使用SDK内封装的拓展命令
   * @param odpsNamespaceSchema 是否开启三层模型
   * @return
   * @throws OdpsException
   */
  SQLExecutorImpl(
      Odps odps,
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
      int tunnelGetResultMaxRetryTime,
      boolean useCommandApi,
      String quotaName,
      Long timeout,
      boolean odpsNamespaceSchema,
      int tunnelSocketTimeout,
      int tunnelReadTimeout,
      boolean sessionSupportNonSelect,
      Integer offlineJobPriority) throws OdpsException {
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
    this.useCommandApi = useCommandApi;
    this.odpsNamespaceSchema = odpsNamespaceSchema;
    this.fallbackQuota = quotaName;
    this.commandApi = new CommandApi(odps);
    this.sessionSupportNonSelect = sessionSupportNonSelect;
    this.offlineJobPriority = offlineJobPriority;
    if (timeout != null) {
      this.attachTimeout = timeout;
    }
    if (!StringUtils.isNullOrEmpty(quotaName)) {
      this.properties.put(SQLExecutorConstants.WLM_QUOTA_FLAG, quotaName);
    }
    if (executeMode.equals(ExecuteMode.INTERACTIVE)) {
      // try recover
      if (recoverInstance != null) {
        if (recoverInstance.getStatus() == Instance.Status.RUNNING) {
          session = new Session(odps, recoverInstance);
          attachSuccess = true;
        }
      }
      try {
        // recover failed, attach new session
        if (session == null) {
          session = Session
              .attach(odps, serviceName, this.properties, attachTimeout,
                  runningCluster, taskName);
          attachSuccess = true;
        }
      } catch (OdpsException e) {
        if (fallbackPolicy.isAlwaysFallBack()) {
          // ignore attach failed if fallback for alwaysFallback
        } else {
          throw e;
        }
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
      if (tunnelSocketTimeout >= 0) {
        instanceTunnel.getConfig().setSocketConnectTimeout(tunnelSocketTimeout);
      }
      if (tunnelReadTimeout >= 0) {
        instanceTunnel.getConfig().setSocketTimeout(tunnelReadTimeout);
      }
      if (StringUtils.isNullOrEmpty(tunnelEndpoint)) {
        //try to get tunnelEndpoint from local cache
        try {
          // TODO: support specify tunnel quota name
          tunnelEndpoint = cache.getTunnelEndpointFromLocalCache(odps, null);
        } catch (ExecutionException e) {
          throw new OdpsException(
              "Get tunnel endpoint from localCache exception:" + e.getMessage());
        }
        instanceTunnel.setEndpoint(tunnelEndpoint);
        this.tunnelEndpoint = tunnelEndpoint;
      } else {
        instanceTunnel.setEndpoint(tunnelEndpoint);
        this.tunnelEndpoint = tunnelEndpoint;
      }
    }
  }

  /**
   * 默认行为关闭该executor,若为连接池模式,则归还Executor到连接池中
   *
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
   *
   * @return
   * @throws
   */
  @Override
  public String getId() {
    return id;
  }

  /**
   * 获取当前query taskName
   *
   * @return
   * @throws
   */
  @Override
  public String getTaskName() {
    return taskName;
  }

  /**
   * 获取当前subquery ID
   *
   * @return
   * @throws
   */
  @Override
  public int getSubqueryId() {
    if (queryInfo != null && queryInfo.getExecuteMode().equals(ExecuteMode.INTERACTIVE)) {
      return queryInfo.getId();
    }
    return -1;
  }

  /**
   * 获取当前queryId
   *
   * @return
   * @throws
   */
  @Override
  public String getQueryId() {
    if (queryInfo != null) {
      if (queryInfo.getExecuteMode().equals(ExecuteMode.INTERACTIVE)) {
        return queryInfo.getInstance().getId() + "_" + queryInfo.getId();
      } else {
        return queryInfo.getInstance().getId();
      }
    }
    return null;
  }

  /**
   * 获取当前query Logview
   *
   * @return
   * @throws
   */
  @Override
  public String getLogView() {
    if (parseSuccess) {
      Command command = queryInfo.getCommandInfo().getCommand();
      if (command.isSync()) {
        return null;
      }
      try {
        return new LogView(odps).generateLogView(queryInfo.getCommandInfo().getInstance(), 7 * 24);
      } catch (Exception e) {
        return null;
      }
    }

    if (queryInfo != null) {
      // if query running, return query logview
      try {
        if (queryInfo.getExecuteMode().equals(ExecuteMode.INTERACTIVE)) {
          if (session != null) {
            return new LogView(odps)
                .generateSubQueryLogView(queryInfo.getInstance(), queryInfo.getId(), session.getToken());
          } else {
            return new LogView(odps)
                .generateSubQueryLogView(queryInfo.getInstance(), queryInfo.getId(), 7 * 24);
          }
        } else {
          return new LogView(odps).generateLogView(queryInfo.getInstance(), 7 * 24);
        }
      } catch (Exception e) {
        return null;
      }
    } else if (session != null) {
      try {
        // no query running, return session logview if have
        return session.getLogView();
      } catch (Exception e) {
        return null;
      }
    }
    return null;
  }

  /**
   * 检查该Executor是否活跃
   *
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
   *
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
   *
   * @return
   * @throws
   */
  public Instance getInstance() {
    if (parseSuccess) {
      Command command = queryInfo.getCommandInfo().getCommand();
      if (command.isSync()) {
        return null;
      }
      return queryInfo.getCommandInfo().getInstance();
    }

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
   *
   * @return 各阶段进度信息
   * @throws OdpsException
   */
  public List<Instance.StageProgress> getProgress() throws OdpsException {
    if (queryInfo == null) {
      return null;
    }

    if (parseSuccess) {
      Command command = queryInfo.getCommandInfo().getCommand();
      if (command.isSync()) {
        return null;
      }
      return queryInfo.getCommandInfo().getInstance().getTaskProgress(queryInfo.getCommandInfo().getTaskName());
    }

    if (queryInfo.getExecuteMode().equals(ExecuteMode.OFFLINE)) {
      return queryInfo.getInstance().getTaskProgress(SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME);
    } else {
      return session.getInstance().getTaskProgress(taskName);
    }
  }


  /**
   * 获取当前查询的Summary信息
   *
   * @return SQL summary
   * @throws OdpsException
   */
  public String getSummary() throws OdpsException {
    if (queryInfo == null) {
      return null;
    }

    if (parseSuccess) {
      Command command = queryInfo.getCommandInfo().getCommand();
      if (command.isSync()) {
        return null;
      }
      Instance.TaskSummary
          summary =
          queryInfo.getCommandInfo().getInstance().getTaskSummary(queryInfo.getCommandInfo().getTaskName());
      return summary == null ? null : summary.getSummaryText();
    }

    if (queryInfo.getExecuteMode().equals(ExecuteMode.OFFLINE)) {
      Instance.TaskSummary
          summary =
          queryInfo.getInstance().getTaskSummary(SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME);
      if (summary == null) {
        return null;
      }
      return summary.getSummaryText();
    } else {
      return session.getQueryStats(queryInfo.getId());
    }
  }

  /**
   * 获取当前查询的执行日志
   *
   * @return 执行信息, 包括回退等消息
   * @throws OdpsException
   */
  public List<String> getExecutionLog() {
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
      throws OdpsException, IOException {
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
   *
   * @param countLimit 返回结果数量。在打开command api的情况下，异步command不受该参数限制，
   *                   见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @return query执行的所有结果 注意 : 返回结果类型为 {@link List}, 数据量较大时会带来较多内存开销 大数据量下载建议直接使用{@link
   * #getResultSet(Long)};
   * @throws OdpsException, IOException
   */
  @Override
  public List<Record> getResult(Long countLimit)
      throws OdpsException, IOException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    if (!useInstanceTunnel && countLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    try {
      return getResultInternal(null, countLimit, null, false);
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * 获取有限集结果
   *
   * @param countLimit 返回结果数量。在打开command api的情况下，异步command不受该参数限制，
   *                   见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param sizeLimit  返回结果大小(Bytes) 如果超过count大小会截断 如果超过sizeLimit会直接抛出异常 注意 : 返回结果类型为 {@link List},
   *                   数据量较大时会带来较多内存开销 大数据量下载建议直接使用{@link #getResultSet(Long, Long)};
   *                   在打开command api的情况下，同步和异步command均不受该参数限制。
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  @Override
  public List<Record> getResult(Long countLimit, Long sizeLimit)
      throws OdpsException, IOException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
    }
    if (!useInstanceTunnel && countLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    if (!useInstanceTunnel && sizeLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    try {
      return getResultInternal(null, countLimit, sizeLimit, false);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 通过InstanceTunnel获取有限集结果
   *
   * @param offset     返回结果的开始行数，从第几行开始取结果。在打开command api的情况下，异步command不受该参数限制，
   *                   见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param countLimit 返回结果数量。在打开command api的情况下，异步command不受该参数限制，
   *                   见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param sizeLimit  返回结果大小 注意 : 返回结果类型为 {@link List}, 数据量较大时会带来较多内存开销 大数据量下载建议直接使用{@link
   *                   #getResultSet(Long, Long, Long)};
   *                   在打开command api的情况下，同步和异步command均不受该参数限制。
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  @Override
  public List<Record> getResult(Long offset, Long countLimit, Long sizeLimit)
      throws OdpsException, IOException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
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
    try {
      return getResultInternal(offset, countLimit, sizeLimit, false);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }


  /**
   * 通过InstanceTunnel获取有限集结果
   *
   * @param offset       返回结果的开始行数, 从第几行开始取结果。在打开command api的情况下，异步command不受该参数限制，
   *                     见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param countLimit   返回结果数量。在打开command api的情况下，异步command不受该参数限制，
   *                     见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param sizeLimit    返回结果大小。在打开command api的情况下，同步和异步command均不受该参数限制。
   * @param limitEnabled 是否启用project设置的READ_TABLE_MAX_ROW, 启用后countLimit设置不再生效(被READ_TABLE_MAX_ROW覆盖)
   *                     不启用则取countLimit数量的结果, 但会进行权限校验, 需要提前在policy中为对应SQL中涉及的相应表和视图添加exception,
   *                     否则无权下载。在打开command api的情况下，同步和异步command均不受该参数限制。
   * @return query执行的所有结果 注意 : 返回结果类型为 {@link List}, 数据量较大时会带来较多内存开销 大数据量下载建议直接使用{@link
   * #getResultSet(Long, Long, Long, boolean)};
   * @throws OdpsException, IOException
   */
  public List<Record> getResult(Long offset, Long countLimit, Long sizeLimit, boolean limitEnabled)
      throws OdpsException, IOException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
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
    try {
      return getResultInternal(offset, countLimit, sizeLimit, limitEnabled);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 通过InstanceTunnel获取有限集结果的迭代器
   *
   * @param countLimit 返回结果数量。在打开command api的情况下，异步command不受该参数限制，
   *                   见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
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
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    try {
      return getResultSetInternal(null, countLimit, null, false);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 通过InstanceTunnel获取有限集结果的迭代器
   *
   * @param countLimit 返回结果数量。在打开command api的情况下，异步command不受该参数限制，
   *                   见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param sizeLimit  返回结果大小(Bytes) 如果超过count大小会截断 如果超过sizeLimit会直接抛出异常。
   *                   在打开command api的情况下，同步和异步command均不受该参数限制。
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
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    if (!useInstanceTunnel && sizeLimit != null) {
      throw new OdpsException("Please enable instance tunnel if you want to get limited result.");
    }
    try {
      return getResultSetInternal(null, countLimit, sizeLimit, false);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 通过InstanceTunnel获取有限集结果的迭代器
   *
   * @param offset     返回结果的开始行数，从第几行开始取结果。在打开command api的情况下，异步command不受该参数限制，
   *                   见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param countLimit 返回结果数量。在打开command api的情况下，异步command不受该参数限制，
   *                   见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param sizeLimit  返回结果大小。在打开command api的情况下，同步和异步command均不受该参数限制。
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  @Override
  public ResultSet getResultSet(Long offset, Long countLimit, Long sizeLimit)
      throws OdpsException, IOException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
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
    try {
      return getResultSetInternal(offset, countLimit, sizeLimit, false);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 通过InstanceTunnel获取有限集结果的迭代器
   *
   * @param offset       返回结果的开始行数, 从第几行开始取结果。在打开command api的情况下，异步command不受该参数限制，
   *                     见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param countLimit   返回结果数量。在打开command api的情况下，异步command不受该参数限制，
   *                     见{@link SQLExecutorBuilder#enableCommandApi(boolean)}
   * @param sizeLimit    返回结果大小。在打开command api的情况下，同步和异步command均不受该参数限制。
   * @param limitEnabled 是否启用project设置的READ_TABLE_MAX_ROW, 启用后countLimit设置不再生效(被READ_TABLE_MAX_ROW覆盖)
   *                     不启用则取countLimit数量的结果, 但会进行权限校验, 需要提前在policy中为对应SQL中涉及的相应表和视图添加exception,
   *                     否则无权下载。在打开command api的情况下，同步和异步command均不受该参数限制。
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  public ResultSet getResultSet(Long offset, Long countLimit, Long sizeLimit, boolean limitEnabled)
      throws OdpsException, IOException {
    if (queryInfo == null) {
      throw new OdpsException("No query running now.");
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
    try {
      return getResultSetInternal(offset, countLimit, sizeLimit, limitEnabled);
    } catch (Exception e) {
      throw e;
    } finally {
    }
  }

  /**
   * 提交一个query
   *
   * @param sql  sql语句
   * @param hint query需要的hint参数
   * @return
   * @throws OdpsException
   */
  @Override
  public void run(String sql, Map<String, String> hint) throws OdpsException {
    if (hint == null) {
      hint = new HashMap<>();
    }
    queryInfo = new QueryInfo(sql, hint, executeMode);
    queryInfo.setCommandInfo(new CommandInfo(sql, hint));

    if (useCommandApi) {
      Command command = CommandUtil.parseCommand(sql);
      // 如果command为null说明解析失败, 执行之前的逻辑
      if (command != null) {
        queryInfo.getCommandInfo().setCommand(command);
        queryInfo.getCommandInfo().setOdpsNamespaceSchema(odpsNamespaceSchema);
        if (!command.isSync()) {
          // 异步command非阻塞执行
          command.run(odps, queryInfo.getCommandInfo());
        }
        parseSuccess = true;
        return;
      }
    }

    // 通过odpsSql语法树判断是否是select query
    queryInfo.setSelect(isSelect(sql));

    parseSuccess = false;
    //reset tunnelGetResultRetryCount before query run
    tunnelGetResultRetryCount = 0;
    try {
      runQueryInternal(executeMode, null, false);
    } catch (Exception e) {
      throw e;
    }
  }

  private void reattach(String errorMessage) throws OdpsException {
    if (enableReattach) {
      try {
        attachSuccess = false;
        session =
            Session
                .attach(odps, serviceName, properties, attachTimeout,
                    runningCluster, taskName);
        attachSuccess = true;
      } catch (OdpsException e) {
        if (!fallbackPolicy.isAlwaysFallBack()) {
          throw new OdpsException(errorMessage);
        }
      }
    } else {
      throw new OdpsException(errorMessage);
    }
  }

  private ExecuteMode handleSessionException(String errorMessage) throws OdpsException {
    if (errorMessage.indexOf(SQLExecutorConstants.sessionReattachFlag) != -1) {
      reattach(errorMessage);
      return ExecuteMode.INTERACTIVE;
    } else if (errorMessage.indexOf(SQLExecutorConstants.sessionJobCancelledComplierFlag) != -1 ||
               errorMessage.indexOf(SQLExecutorConstants.sessionJobCancelledFlag) != -1) {
      throw new OdpsException(errorMessage);
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
        && (errorMessage.indexOf(SQLExecutorConstants.sessionQueryTimeoutFlag) != -1 ||
            errorMessage.indexOf(SQLExecutorConstants.sessionTunnelTimeoutMessage) != -1 ||
              errorMessage.indexOf(SQLExecutorConstants.sessionTunnelGetSelectDescTimeoutMessage) != -1)) {
      return ExecuteMode.OFFLINE;
    } else if (fallbackPolicy.isFallback4UnknownError()
        && errorMessage.indexOf(SQLExecutorConstants.sessionExceptionFlag) != -1) {
      return ExecuteMode.OFFLINE;
    } else if (fallbackPolicy.isAlwaysFallBack()) {
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

  private TunnelRetryInfo handleTunnelException(String errorCode, String errorMessage)
      throws OdpsException {
    boolean isSelect = checkIsSelect(errorCode, errorMessage);
    TunnelRetryInfo info = new TunnelRetryInfo();
    info.errCode = errorCode;
    info.errMsg = errorMessage;
    if (!isSelect) {
      // tunnel do not support non-select query, double check task result
      try {
        session.getSubQueryResult(queryInfo.getId());
        // query success
        info.status = TunnelRetryStatus.NON_SELECT_QUERY;
        return info;
      } catch (OdpsException e) {
        // query failed
        info.status = TunnelRetryStatus.QUERY_FAILED;
        info.errCode = e.getErrorCode();
        info.errMsg = e.getMessage();
        return info;
        //throw new OdpsException(e.getErrorCode() + ":" + e.getMessage());
      }
    }
    if (errorCode.equals(SQLExecutorConstants.sessionTunnelTimeoutFlag)
        || errorMessage.indexOf(SQLExecutorConstants.sessionTunnelTimeoutMessage) != -1) {
      // get result timeout
      tunnelGetResultRetryCount++;
      if (tunnelGetResultRetryCount >= tunnelGetResultMaxRetryTime) {
        info.status = TunnelRetryStatus.QUERY_FAILED;
        return info;
      }
      info.status = TunnelRetryStatus.NEED_RETRY;
      return info;
    }
    info.status = TunnelRetryStatus.QUERY_FAILED;
    return info;
  }

  private ResultSet newEmptyResultSet() {
    return new ResultSet(
        new InMemoryRecordIterator(new ArrayList<>()),
        new TableSchema(),
        0
    );
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
    // 同步直接阻塞返回结果
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
        return newEmptyResultSet();
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

  private List<Record> getResultInternal(Long offset, Long countLimit, Long sizeLimit,
      boolean limitEnabled)
      throws OdpsException, IOException {
    // 如果解析成功 走语法树的处理逻辑
    if (parseSuccess) {
      return getCommandResult(offset, countLimit, sizeLimit, limitEnabled);
    }

    if (useInstanceTunnel) {
      if (queryInfo.getExecuteMode() == ExecuteMode.INTERACTIVE && attachSuccess) {
        return getSessionResultByInstanceTunnel(offset, countLimit, sizeLimit, limitEnabled);
      } else {
        return getOfflineResultByInstanceTunnel(countLimit, limitEnabled);
      }
    } else {
      if (queryInfo.getExecuteMode() == ExecuteMode.INTERACTIVE && attachSuccess) {
        return getSessionResult();
      } else {
        return getOfflineResult();
      }
    }
  }

  private ResultSet getResultSetInternal(Long offset, Long countLimit, Long sizeLimit,
      boolean limitEnabled)
      throws OdpsException, IOException {
    // 如果解析成功 走语法树的处理逻辑
    if (parseSuccess) {
      return getCommandResultSet(offset, countLimit, sizeLimit, limitEnabled);
    }

    if (useInstanceTunnel) {
      if (queryInfo.getExecuteMode() == ExecuteMode.INTERACTIVE && attachSuccess) {
        return getSessionResultSetByInstanceTunnel(offset, countLimit, sizeLimit, limitEnabled);
      } else {
        return getOfflineResultSetByInstanceTunnel(countLimit, limitEnabled);
      }
    } else {
      if (queryInfo.getExecuteMode() == ExecuteMode.INTERACTIVE && attachSuccess) {
        return getSessionResultSet();
      } else {
        return getOfflineResultSet();
      }
    }
  }

  private List<Record> getSessionResult()
      throws OdpsException, IOException {
    Session.SubQueryResult result = null;
    try {
      if (queryInfo.isSelect()) {
        result = session.getSubQueryResult(queryInfo.getId());
      } else {
        result = session.getRawSubQueryResult(queryInfo.getId());
      }
    } catch (OdpsException e) {
      ExecuteMode executeMode = handleSessionException(e.getMessage());
      runQueryInternal(executeMode, e.getMessage(), true);
      return getResultInternal(null, null, null, true);
    }
    return result.getRecords();
  }

  private List<Record> getSessionResultByInstanceTunnel(Long offset, Long countLimit,
      Long sizeLimit, boolean limitEnabled)
      throws OdpsException, IOException, RuntimeException {
    Instance instance = queryInfo.getInstance();
    List<Record> records = new ArrayList<>();
    try {
      InstanceTunnel.DownloadSession downloadSession =
          instanceTunnel.createDirectDownloadSession(
              instance.getProject(),
              instance.getId(),
              taskName,
              queryInfo.getId(),
              limitEnabled);
      TunnelRecordReader
          reader =
          downloadSession
              .openRecordReader(offset == null ? 0 : offset, countLimit == null ? -1 : countLimit,
                  sizeLimit == null ? -1 : sizeLimit);
      while (true) {
        Record record = reader.read();
        if (sizeLimit != null && sizeLimit > 0 && reader.getTotalBytes() > sizeLimit) {
          throw new RuntimeException("InvalidArgument: sizeLimit, fetched data is larger than limit size");
        }
        if (record == null) {
          break;
        } else {
          records.add(record);
        }
      }
    } catch (TunnelException e) {
      TunnelRetryInfo retryInfo = handleTunnelException(e.getErrorCode(), e.getMessage());
      if (retryInfo.status.equals(TunnelRetryStatus.NEED_RETRY)) {
        return getResultInternal(offset, countLimit, sizeLimit, limitEnabled);
      } else if (retryInfo.status.equals(TunnelRetryStatus.NON_SELECT_QUERY)) {
        // non-select may have result.
        runQueryInternal(ExecuteMode.OFFLINE, retryInfo.errMsg, true);
        return getResultInternal(offset, countLimit, sizeLimit, limitEnabled);
      } else {
        ExecuteMode executeMode = handleSessionException(retryInfo.errMsg);
        runQueryInternal(executeMode, retryInfo.errMsg, true);
        return getResultInternal(offset, countLimit, sizeLimit, limitEnabled);
      }
    }
    return records;
  }

  private List<Record> getOfflineResult() throws OdpsException {
    queryInfo.getInstance().waitForSuccess();
    Map<String, String> results = queryInfo.getInstance().getTaskResults();
    String selectResult = results.get(SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME);

    if (selectResult != null) {
      if (queryInfo.isSelect()) {
        return SQLTask.parseCsvRecord(selectResult);
      } else {
        // 非select但是具备结果集的命令
        return CommandUtil.toRecord(selectResult, "Info");
      }
    }

    return new ArrayList<>();
  }

  private List<Record> getOfflineResultByInstanceTunnel(Long limit, boolean limitEnabled)
      throws OdpsException, IOException {
    queryInfo.getInstance().waitForSuccess();
    if (queryInfo.isSelect()) {
      try {
        return SQLTask.getResultByInstanceTunnel(queryInfo.getInstance(),
                                                 SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME,
                                                 limit, limitEnabled);
      } catch (TunnelException tunnelException) {
        boolean
            isSelect =
            checkIsSelect(tunnelException.getErrorCode(), tunnelException.getMessage());
        // Tunnel may throw the following exceptions when task failed. In this case, we should
        // get the error message by API
        if (!isSelect || TunnelConstants.INSTANCE_NOT_TERMINATED.equals(
            tunnelException.getErrorCode())
            || TunnelConstants.TASK_FAILED.equals(tunnelException.getErrorCode())) {
          return getOfflineResult();
        } else {
          throw tunnelException;
        }
      }
    } else {
      Map<String, String> results = queryInfo.getInstance().getTaskResults();
      String selectResult = results.get(SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME);
      if (StringUtils.isNullOrEmpty(selectResult)) {
        return new ArrayList<>();
      }
      return CommandUtil.toRecord(selectResult, "Info");
    }
  }

  private ResultSet getSessionResultSet()
      throws OdpsException, IOException {
    Session.SubQueryResult result = null;
    try {
      // subquery -1 的时候，不区分不会报错。之后的subquery，非select的结果就是错的
      if (queryInfo.isSelect()) {
        result = session.getSubQueryResult(queryInfo.getId());
      } else {
        result = session.getRawSubQueryResult(queryInfo.getId());
      }
    } catch (OdpsException e) {
      ExecuteMode executeMode = handleSessionException(e.getMessage());
      runQueryInternal(executeMode, e.getMessage(), true);
      return getResultSetInternal(null, null, null, true);
    }
    return new ResultSet(
        new InMemoryRecordIterator(result.getRecords()),
        result.getSchema(),
        result.getRecords().size());
  }

  private ResultSet getSessionResultSetByInstanceTunnel(Long offset, Long countLimit,
      Long sizeLimit, boolean limitEnabled)
      throws OdpsException, IOException {
    Instance instance = queryInfo.getInstance();
    InstanceTunnel.DownloadSession downloadSession = null;
    TunnelRecordReader reader = null;
    try {
      downloadSession =
          instanceTunnel.createDirectDownloadSession(
              instance.getProject(),
              instance.getId(),
              taskName,
              queryInfo.getId(),
              limitEnabled);
      // remained for :
      // 1.TunnelException check
      // 2.Check size limit
      // 3.Get return result record count
      reader =
          downloadSession
              .openRecordReader(offset == null ? 0 : offset, countLimit == null ? -1 : countLimit,
                  sizeLimit == null ? -1 : sizeLimit);
    } catch (TunnelException e) {
      TunnelRetryInfo retryInfo = handleTunnelException(e.getErrorCode(), e.getMessage());
      if (retryInfo.status.equals(TunnelRetryStatus.NEED_RETRY)) {
        return getResultSetInternal(offset, countLimit, sizeLimit, limitEnabled);
      } else if (retryInfo.status.equals(TunnelRetryStatus.NON_SELECT_QUERY)) {
        // non-select may have result.
        runQueryInternal(ExecuteMode.OFFLINE, retryInfo.errMsg, true);
        return getResultSetInternal(offset, countLimit, sizeLimit, limitEnabled);
      } else {
        ExecuteMode executeMode = handleSessionException(retryInfo.errMsg);
        runQueryInternal(executeMode, retryInfo.errMsg, true);
        return getResultSetInternal(offset, countLimit, sizeLimit, limitEnabled);
      }
    }
    return new ResultSet(
        new SessionRecordSetIterator(downloadSession, reader, downloadSession.getRecordCount(),
            offset == null ? 0 : offset, sizeLimit == null ? -1 : sizeLimit),
        downloadSession.getSchema(),
        downloadSession.getRecordCount());
  }


  private ResultSet getOfflineResultSet()
      throws OdpsException, IOException {
    String selectResult = queryInfo.getInstance().waitForTerminatedAndGetResult();
    if (!StringUtils.isNullOrEmpty(selectResult)) {
      if (queryInfo.isSelect()) {
        CSVRecordParser.ParseResult parseResult = CSVRecordParser.parse(selectResult);
        List<Record> records = parseResult.getRecords();
        return new ResultSet(
            new InMemoryRecordIterator(records),
            parseResult.getSchema(),
            records.size());
      } else {
        List<Record> records = CommandUtil.toRecord(selectResult, "Info");
        TableSchema schema = new TableSchema();
        schema.setColumns(Arrays.asList(records.get(0).getColumns()));
        return new ResultSet(
            new InMemoryRecordIterator(records),
            schema,
            records.size());
      }
    } else {
      return newEmptyResultSet();
    }
  }

  private ResultSet getOfflineResultSetByInstanceTunnel(Long limit, boolean limitEnabled)
      throws OdpsException, IOException {
    queryInfo.getInstance().waitForSuccess();
    // getResultSet will use instance tunnel, which do not support non-select query
    if (queryInfo.isSelect()) {
      URI tunnelEndpoint = null;
      if (!StringUtils.isNullOrEmpty(this.tunnelEndpoint)) {
        try {
          tunnelEndpoint = new URI(this.tunnelEndpoint);
        } catch (URISyntaxException e) {
          throw new RuntimeException("tunnel endpoint syntax error, please check again.");
        }
      }
      try {
        return SQLTask
            .getResultSet(queryInfo.getInstance(), SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME,
                          limit, limitEnabled, tunnelEndpoint);
      } catch (TunnelException tunnelException) {
        boolean isSelect = checkIsSelect(tunnelException.getErrorCode(), tunnelException.getMessage());
        // Tunnel may throw the following exceptions when task failed. In this case, we should
        // get the error message by API
        if (!isSelect || TunnelConstants.INSTANCE_NOT_TERMINATED.equals(
            tunnelException.getErrorCode())
            || TunnelConstants.TASK_FAILED.equals(tunnelException.getErrorCode())) {
          return getOfflineResultSet();
        } else {
          throw tunnelException;
        }
      }
    } else {
      Map<String, String> results = queryInfo.getInstance().getTaskResults();
      String selectResult = results.get(SQLExecutorConstants.DEFAULT_OFFLINE_TASKNAME);
      if (StringUtils.isNullOrEmpty(selectResult)) {
        return newEmptyResultSet();
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

  private void runInSessionWithRetry(String rerunMsg) throws OdpsException {
    Session.SubQueryInfo
        subQueryInfo =
        session.runSubQuery(queryInfo.getSql(), queryInfo.getHint());
    if (subQueryInfo.status.equals(Session.SubQueryInfo.kOKCode)) {
      if (subQueryInfo.queryId == -1) {
        ExecuteMode executeMode = handleSessionException(subQueryInfo.result);
        runQueryInternal(executeMode, subQueryInfo.result, true);
      } else {
        // submit success, do not generate logview now
        // call getLogview() if it's needed
        queryInfo.setId(subQueryInfo.queryId);
        queryInfo.setInstance(session.getInstance(),
            ExecuteMode.INTERACTIVE,
            "",
            rerunMsg);
      }
    } else if (subQueryInfo.status.equals(Session.SubQueryInfo.kNotFoundCode)) {
      // odps worker cannot found instance, may stopped, reattach and retry
      String taskTerminateMsg = session.getInstance().getTaskResults().get(taskName);
      reattach("Submit query failed:" + taskTerminateMsg);
      // if attach failed, will fallback to offline
      // if attach succeed, this will be the first try, no not inc retry count
      runQueryInternal(ExecuteMode.INTERACTIVE, taskTerminateMsg, !attachSuccess);
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
        null,
        offlineJobPriority);

    queryInfo.setInstance(instance, ExecuteMode.OFFLINE,
        new LogView(odps).generateLogView(instance, 7 * 24), rerunMsg);
  }

  private void runQueryInternal(ExecuteMode executeMode, String rerunMsg, boolean isRerun) throws OdpsException {
    boolean fallbackForAttachFailed = false;
    boolean forceRunInOffline = !queryInfo.isSelect() && !sessionSupportNonSelect;
    if (queryInfo.getRetry() < SQLExecutorConstants.MaxRetryTimes) {
      if (isRerun) {
        queryInfo.incRetry();
      }
      // INTERACTIVE mode and attach failed and always fallback, try to attach session
      if (executeMode == ExecuteMode.INTERACTIVE && !attachSuccess && fallbackPolicy
          .isAlwaysFallBack() && !forceRunInOffline) {
        try {
          session =
              Session.attach(odps, serviceName, properties,
                             attachTimeout, runningCluster, taskName);
          attachSuccess = true;
        } catch (OdpsException e) {
          // ignore if attach failed, run SQL in offline mode
          fallbackForAttachFailed = true;
        }
      }
      if (executeMode == ExecuteMode.OFFLINE || fallbackForAttachFailed || forceRunInOffline) {
        queryInfo.setExecuteMode(ExecuteMode.OFFLINE);
        // attach success and query fallback, disable sqa in fallback offline mode
        if (queryInfo != null) {
          if (queryInfo.getHint() == null) {
            queryInfo.setHint(new HashMap<>());
          }
          if (!StringUtils.isNullOrEmpty(fallbackQuota)) {
            queryInfo.getHint().put(SQLExecutorConstants.WLM_QUOTA_FLAG, fallbackQuota);
          }
          queryInfo.getHint().put(SQLExecutorConstants.SQA_TASK_FLAG, "false");
          if (!forceRunInOffline) {
            if (queryInfo.getInstance() != null) {
              queryInfo.getHint().put(SQLExecutorConstants.SESSION_FALLBACK_TRACER,
                                      queryInfo.getInstance().getId() + "_" + queryInfo.getId());
            } else {
              queryInfo.getHint().put(SQLExecutorConstants.SESSION_FALLBACK_TRACER,
                                      "fallback4AttachFailed");
            }
          }
        }
        runInOffline(rerunMsg);
      } else {
        runInSessionWithRetry(rerunMsg);
      }
    } else {
      throw new OdpsException(rerunMsg);
    }
  }

  @Override
  public boolean hasResultSet() {
    if (parseSuccess) {
      return true;
    }
    return SqlParserUtil.hasResultSet(queryInfo.getSql());
  }

  @Override
  public boolean isRunningInInteractiveMode() {
    return queryInfo.getExecuteMode().equals(ExecuteMode.INTERACTIVE);
  }

  public boolean isSelect(String sql) throws OdpsException {
    try {
      return SqlParserUtil.isSelect(sql);
    } catch (SQLException e) {
      throw new OdpsException("Sql isSelect failed", e);
    }
  }

}

/**
 * class: SessionRecordSetIterator It is used in getSessionResultSetByInstanceTunnel
 */
class SessionRecordSetIterator implements Iterator<Record> {

  private List<Record> buffer;
  private static final long FETCH_SIZE = 10000L;
  private long cursor = 0;
  private int idx = 0;
  private long fetchSize = 0;
  private long recordCount;
  private long offset;
  private long sizeLimit;
  private long currentReadSize = 0;
  private InstanceTunnel.DownloadSession session;
  private TunnelRecordReader reader;

  public SessionRecordSetIterator(InstanceTunnel.DownloadSession session
      , TunnelRecordReader reader
      , long recordCount
      , long offset
      , long sizeLimit) {
    this.session = session;
    this.reader = reader;
    this.recordCount = recordCount;
    this.offset = offset;
    this.sizeLimit = sizeLimit;
  }

  @Override
  public boolean hasNext() {
    return cursor < recordCount;
  }

  @Override
  public Record next() {
    if (buffer == null || idx == buffer.size()) {
      fillBuffer();
    }
    cursor++;
    return buffer.get(idx++);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  private void fillBuffer() {
    idx = 0;
    // use original reader when fill buffer at first time if record count less than 10000
    if (cursor == 0 && recordCount <= FETCH_SIZE) {
      // reuse reader
    } else {
      reader = openNewReader();
    }
    buffer = new ArrayList<Record>();
    Record r = null;
    try {
      while ((r = reader.read()) != null) {
        buffer.add(r);
        if (sizeLimit > 0) {
          if (currentReadSize + reader.getTotalBytes() > sizeLimit) {
            throw new RuntimeException("InvalidArgument: sizeLimit, fetched data is larger than limit size");
          }
        }
      }
      currentReadSize += reader.getTotalBytes();
    } catch (IOException e) {
      throw new RuntimeException("Read from reader failed:", e);
    }
  }

  private TunnelRecordReader openNewReader() {
    fetchSize = recordCount - cursor <= FETCH_SIZE ? recordCount - cursor : FETCH_SIZE;
    try {
      return session.openRecordReader(cursor + offset, fetchSize);
    } catch (TunnelException e) {
      throw new RuntimeException("Open reader failed: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new RuntimeException("Open reader failed: " + e.getMessage(), e);
    }
  }
}