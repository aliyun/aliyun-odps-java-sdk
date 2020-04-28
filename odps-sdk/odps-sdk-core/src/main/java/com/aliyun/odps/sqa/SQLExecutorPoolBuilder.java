package com.aliyun.odps.sqa;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;

import java.util.Map;

/**
 * Created by dongxiao on 2020/3/17.
 */
public class SQLExecutorPoolBuilder {
  private SQLExecutorBuilder sqlExecutorBuilder = SQLExecutorBuilder.builder();
  private int initPoolSize = 1;
  private int maxPoolSize = 5;

  public static SQLExecutorPoolBuilder builder() {
    return new SQLExecutorPoolBuilder();
  }

  public SQLExecutorPool build() throws OdpsException{
    return SQLExecutorPool.create(initPoolSize, maxPoolSize, sqlExecutorBuilder);
  }

  public SQLExecutorPoolBuilder initPoolSize(int initPoolSize) {
    this.initPoolSize = initPoolSize;
    return this;
  }

  public SQLExecutorPoolBuilder maxPoolSize(int maxPoolSize) {
    this.maxPoolSize = maxPoolSize;
    return this;
  }

  public SQLExecutorPoolBuilder odps(Odps odps) {
    this.sqlExecutorBuilder.odps(odps);
    return this;
  }

  public SQLExecutorPoolBuilder properties(Map<String, String> properties) {
    this.sqlExecutorBuilder.properties(properties);
    return this;
  }

  public SQLExecutorPoolBuilder taskName(String taskName) {
    this.sqlExecutorBuilder.taskName(taskName);
    return this;
  }

  public SQLExecutorPoolBuilder serviceName(String serviceName) {
    this.sqlExecutorBuilder.serviceName(serviceName);
    return this;
  }

  public SQLExecutorPoolBuilder tunnelEndpoint(String tunnelEndpoint) {
    this.sqlExecutorBuilder.tunnelEndpoint(tunnelEndpoint);
    return this;
  }

  public SQLExecutorPoolBuilder executeMode(ExecuteMode executeMode) {
    this.sqlExecutorBuilder.executeMode(executeMode);
    return this;
  }

  public SQLExecutorPoolBuilder fallbackPolicy(FallbackPolicy fallbackPolicy) {
    this.sqlExecutorBuilder.fallbackPolicy(fallbackPolicy);
    return this;
  }

  public SQLExecutorPoolBuilder enableReattach(boolean enableReattach) {
    this.sqlExecutorBuilder.enableReattach(enableReattach);
    return this;
  }
  
  public SQLExecutorPoolBuilder runningCluster(String runningCluster) {
    this.sqlExecutorBuilder.runningCluster(runningCluster);
    return this;
  }

  public SQLExecutorPoolBuilder useInstanceTunnel(boolean useInstanceTunnel) {
    this.sqlExecutorBuilder.useInstanceTunnel(useInstanceTunnel);
    return this;
  }
}
