package com.aliyun.odps.sqa;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;

/**
 * Created by dongxiao on 2020/3/17.
 */
public class SQLExecutorBuilder {

  private ExecuteMode executeMode = ExecuteMode.INTERACTIVE;
  private boolean enableReattach = true;
  private boolean useInstanceTunnel = true;
  private Odps odps = null;
  private Map<String, String> properties = new ConcurrentHashMap<>();
  private String taskName = SQLExecutorConstants.DEFAULT_TASK_NAME;
  private String serviceName = SQLExecutorConstants.DEFAULT_SERVICE;
  private String tunnelEndpoint = null;
  // fallback offline quota
  private String quotaName = null;
  private SQLExecutorPool pool = null;
  private FallbackPolicy fallbackPolicy = FallbackPolicy.alwaysFallbackPolicy();
  private int tunnelGetResultMaxRetryTime = 3;

  private Long attachTimeout = SQLExecutorConstants.DEFAULT_ATTACH_TIMEOUT;

  private String runningCluster = null;
  private Instance recoverInstance = null;

  private boolean useCommandApi = false;
  private boolean odpsNamespaceSchema = false;

  private int tunnelSocketTimeout = -1;
  private int tunnelReadTimeout = -1;

  private boolean sessionSupportNonSelect = false;

  public static SQLExecutorBuilder builder() {
    return new SQLExecutorBuilder();
  }

  public SQLExecutor build() throws OdpsException {
    return new SQLExecutorImpl(odps, serviceName, taskName, tunnelEndpoint,
                               properties, executeMode, fallbackPolicy, enableReattach,
                               useInstanceTunnel, pool, recoverInstance, runningCluster,
                               tunnelGetResultMaxRetryTime,
                               useCommandApi, quotaName, attachTimeout, odpsNamespaceSchema,
                               tunnelSocketTimeout, tunnelReadTimeout, sessionSupportNonSelect);
  }

  public SQLExecutorBuilder odps(Odps odps) {
    this.odps = odps;
    return this;
  }

  public SQLExecutorBuilder properties(Map<String, String> properties) {
    this.properties.clear();
    this.properties.putAll(properties);
    return this;
  }

  public SQLExecutorBuilder taskName(String taskName) {
    this.taskName = taskName;
    return this;
  }

  public SQLExecutorBuilder serviceName(String serviceName) {
    this.serviceName = serviceName;
    return this;
  }

  public SQLExecutorBuilder tunnelEndpoint(String tunnelEndpoint) {
    this.tunnelEndpoint = tunnelEndpoint;
    return this;
  }

  public SQLExecutorBuilder quotaName(String quotaName) {
    this.quotaName = quotaName;
    return this;
  }

  public SQLExecutorBuilder executeMode(ExecuteMode executeMode) {
    this.executeMode = executeMode;
    return this;
  }

  public SQLExecutorBuilder attachTimeout(Long timeout) {
    this.attachTimeout = timeout;
    return this;
  }

  public SQLExecutorBuilder enableCommandApi(boolean useCommandApi) {
    this.useCommandApi = useCommandApi;
    return this;
  }

  public SQLExecutorBuilder enableOdpsNamespaceSchema(boolean odpsNamespaceSchema) {
    this.odpsNamespaceSchema = odpsNamespaceSchema;
    return this;
  }

  public SQLExecutorBuilder enableReattach(boolean enableReattach) {
    this.enableReattach = enableReattach;
    return this;
  }

  public SQLExecutorBuilder recoverFrom(Instance instance) {
    this.recoverInstance = instance;
    return this;
  }

  public SQLExecutorBuilder runningCluster(String runningCluster) {
    this.runningCluster = runningCluster;
    return this;
  }

  public SQLExecutorBuilder useInstanceTunnel(boolean useInstanceTunnel) {
    this.useInstanceTunnel = useInstanceTunnel;
    return this;
  }

  public SQLExecutorBuilder tunnelGetResultMaxRetryTime(int tunnelGetResultMaxRetryTime) {
    this.tunnelGetResultMaxRetryTime = tunnelGetResultMaxRetryTime;
    return this;
  }

  public SQLExecutorBuilder fallbackPolicy(FallbackPolicy fallbackPolicy) {
    this.fallbackPolicy = fallbackPolicy;
    return this;
  }

  SQLExecutorBuilder setPool(SQLExecutorPool pool) {
    this.pool = pool;
    return this;
  }

  public SQLExecutorBuilder tunnelSocketTimeout(int tunnelSocketTimeout) {
    this.tunnelSocketTimeout = tunnelSocketTimeout;
    return this;
  }

  public SQLExecutorBuilder tunnelReadTimeout(int tunnelReadTimeout) {
    this.tunnelReadTimeout = tunnelReadTimeout;
    return this;
  }

  public SQLExecutorBuilder sessionSupportNonSelect(boolean sessionSupportNonSelect) {
    this.sessionSupportNonSelect = sessionSupportNonSelect;
    return this;
  }
}
