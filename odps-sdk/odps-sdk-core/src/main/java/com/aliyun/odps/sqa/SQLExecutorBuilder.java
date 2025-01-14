package com.aliyun.odps.sqa;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Quota;
import com.aliyun.odps.table.utils.Preconditions;

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
  private boolean useMcqaV2 = false;
  private Integer offlineJobPriority = null;
  private String regionId = null;
  private Quota quota = null;
  private int logviewVersion = 1;

  public static SQLExecutorBuilder builder() {
    return new SQLExecutorBuilder();
  }

  public SQLExecutor build() throws OdpsException {
    if (useMcqaV2 || executeMode == ExecuteMode.INTERACTIVE_V2) {
      Preconditions.checkArgument(executeMode != ExecuteMode.OFFLINE,
                                  "offline executeMode is not supported in mcqa");
      return new com.aliyun.odps.sqa.v2.SQLExecutorImpl(this);
    }
    return new SQLExecutorImpl(odps, serviceName, taskName, tunnelEndpoint,
                               properties, executeMode, fallbackPolicy, enableReattach,
                               useInstanceTunnel, pool, recoverInstance, runningCluster,
                               tunnelGetResultMaxRetryTime,
                               useCommandApi, quotaName, attachTimeout, odpsNamespaceSchema,
                               tunnelSocketTimeout, tunnelReadTimeout, sessionSupportNonSelect,
                               offlineJobPriority, logviewVersion);
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

  public SQLExecutorBuilder quota(Quota quota) {
    this.quota = quota;
    this.quotaName = quota.getNickname();
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

  public SQLExecutorBuilder enableMcqaV2(boolean mcqaV2) {
    this.useMcqaV2 = mcqaV2;
    return this;
  }

  public SQLExecutorBuilder offlineJobPriority(Integer offlineJobPriority) {
    this.offlineJobPriority = offlineJobPriority;
    return this;
  }

  public SQLExecutorBuilder regionId(String regionId) {
    this.regionId = regionId;
    return this;
  }

  public SQLExecutorBuilder logviewVersion(int logviewVersion) {
    this.logviewVersion = logviewVersion;
    return this;
  }

  public ExecuteMode getExecuteMode() {
    return executeMode;
  }

  public boolean isEnableReattach() {
    return enableReattach;
  }

  public boolean isUseInstanceTunnel() {
    return useInstanceTunnel;
  }

  public Odps getOdps() {
    return odps;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public String getTaskName() {
    return taskName;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getTunnelEndpoint() {
    return tunnelEndpoint;
  }

  public String getQuotaName() {
    return quotaName;
  }

  public Quota getQuota() {
    return quota;
  }

  public SQLExecutorPool getPool() {
    return pool;
  }

  public FallbackPolicy getFallbackPolicy() {
    return fallbackPolicy;
  }

  public int getTunnelGetResultMaxRetryTime() {
    return tunnelGetResultMaxRetryTime;
  }

  public Long getAttachTimeout() {
    return attachTimeout;
  }

  public String getRunningCluster() {
    return runningCluster;
  }

  public Instance getRecoverInstance() {
    return recoverInstance;
  }

  public boolean isUseCommandApi() {
    return useCommandApi;
  }

  public boolean isOdpsNamespaceSchema() {
    return odpsNamespaceSchema;
  }

  public int getTunnelSocketTimeout() {
    return tunnelSocketTimeout;
  }

  public int getTunnelReadTimeout() {
    return tunnelReadTimeout;
  }

  public boolean isSessionSupportNonSelect() {
    return sessionSupportNonSelect;
  }

  public boolean isUseMcqaV2() {
    return useMcqaV2;
  }

  public Integer getOfflineJobPriority() {
    return offlineJobPriority;
  }

  public String getRegionId() {
    return regionId;
  }

  public int getLogviewVersion() {
    return logviewVersion;
  }
}
