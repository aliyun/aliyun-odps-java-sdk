package com.aliyun.odps.sqa;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dongxiao on 2020/3/17.
 */
public class SQLExecutorBuilder {
  private ExecuteMode executeMode = ExecuteMode.INTERACTIVE;
  private boolean enableReattach = true;
  private boolean useInstanceTunnel = true;
  private Odps odps = null;
  private Map<String, String> properties = new HashMap();
  private String taskName = SQLExecutorConstants.DEFAULT_TASK_NAME;
  private String serviceName = SQLExecutorConstants.DEFAULT_SERVICE;
  private String tunnelEndpoint = null;
  private SQLExecutorPool pool = null;
  private FallbackPolicy fallbackPolicy = new FallbackPolicy();

  private String runningCluster = null;
  private Instance recoverInstance = null;

  public static SQLExecutorBuilder builder() {
    return new SQLExecutorBuilder();
  }

  public SQLExecutor build() throws OdpsException {
    return new SQLExecutorImpl(odps, serviceName, taskName, tunnelEndpoint,
        properties, executeMode, fallbackPolicy, enableReattach, useInstanceTunnel, pool, recoverInstance,
        runningCluster);
  }

  public SQLExecutorBuilder odps(Odps odps) {
    this.odps = odps;
    return this;
  }

  public SQLExecutorBuilder properties(Map<String, String> properties) {
    this.properties = properties;
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

  public SQLExecutorBuilder executeMode(ExecuteMode executeMode) {
    this.executeMode = executeMode;
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

  public SQLExecutorBuilder fallbackPolicy(FallbackPolicy fallbackPolicy) {
    this.fallbackPolicy = fallbackPolicy;
    return this;
  }

  SQLExecutorBuilder setPool(SQLExecutorPool pool) {
    this.pool = pool;
    return this;
  }
}
