package com.aliyun.odps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Params;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementMap;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class Quota extends LazyLoad {

  @Root(name = "Quota", strict = false)
  public static class QuotaModel {

    @Element(name = "Cluster", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String cluster;

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String name;

    @Element(name = "ID", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String id;

    @Element(name = "IsEnabled", required = false)
    public Boolean isEnabled = true;

    @Element(name = "ResourceSystemType", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String resourceSystemType;

    @Deprecated
    @Element(name = "SessionServiceName", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String sessionServiceName;

    @Element(name = "CreateTimeMs", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String createTimeMs;

    @Element(name = "CPU", required = false)
    public Long cpu;

    @Element(name = "MinCPU", required = false)
    public Long minCpu;

    @Element(name = "ElasticCPUMax", required = false)
    public Long elasticCpuMax;

    @Element(name = "ElasticCPUMin", required = false)
    public Long elasticCpuMin;

    @Element(name = "AdhocCPU", required = false)
    public Long adhocCpu;

    @Element(name = "Memory", required = false)
    public Long memory;

    @Element(name = "MinMemory", required = false)
    public Long minMemory;

    @Element(name = "ElasticMemoryMax", required = false)
    public Long elasticMemoryMax;

    @Element(name = "ElasticMemoryMin", required = false)
    public Long elasticMemoryMin;

    @Element(name = "AdhocMemory", required = false)
    public Long adhocMemory;

    @Element(name = "AdhocGPU", required = false)
    public Long adhocGpu;

    @Element(name = "Strategy", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String strategy;

    @Element(name = "SchedulerType", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String schedulerType;

    @Element(name = "GPU", required = false)
    public Long gpu;

    @Element(name = "MinGPU", required = false)
    public Long minGpu;

    @Element(name = "ElasticGPUMax", required = false)
    public Long elasticGpuMax;

    @Element(name = "ElasticGPUMin", required = false)
    public Long elasticGpuMin;

    /**
     * @cpuUsage is read only
     */
    @Element(name = "CPUUsage", required = false)
    public Double cpuUsage;

    @Element(name = "AdhocCPUUsage", required = false)
    public Double adhocCpuUsage;

    /**
     * @memoryUsage is read only
     */
    @Element(name = "MemoryUsage", required = false)
    public Double memoryUsage;

    @Element(name = "AdhocMemoryUsage", required = false)
    public Double adhocMemoryUsage;

    @Element(name = "CPUReadyRatio", required = false)
    public Double cpuReadyRatio;

    @Element(name = "MemoryReadyRatio", required = false)
    public Double memoryReadyRatio;

    @Element(name = "IsParGroup", required = false)
    public Boolean isParentGroup;

    @Element(name = "ParGroupId", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String parentId;

    @Element(name = "ParentName", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String parentName;

    @ElementMap(
        name = "UserDefinedTag",
        required = false,
        entry = "entry",
        key = "key",
        value = "value",
        empty = false)
    public Map<String, String> userDefinedTag = new HashMap<>();

    @Element(name = "VirtualClusterConfig", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String virtualClusterConfig;

    @Element(name = "VirtualClusterDebugInfoMap", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String virtualClusterDebugInfoMap;

    @Element(name = "TenantId", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String tenantId;

    @Element(name = "Status", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String status;

    @Element(name = "Nickname", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String nickname;

    /* TODO(habai.zc):
     * @parentNickname is read only currently.
     * Will open to write in later version.
     */
    @Element(name = "ParentNickname", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String parentNickname;

    @Element(name = "CreatorId", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String creatorId;

    @Element(name = "Region", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String regionId;

    @Element(name = "BillingPolicy", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String billingPolicy;

    @Element(name = "NeedAuth", required = false)
    public Boolean needAuth;

    @Element(name = "IsPureLink", required = false)
    public Boolean isPureLink;

    @Element(name = "QuotaVersion", required = false)
    public Long quotaVersion;

    @Element(name = "IsMetaOnly", required = false)
    public Boolean isMetaOnly;

    @Element(name = "Properties", required = false)
    @Convert(SimpleXmlUtils.JsonMapConverter.class)
    public Map<String, String> properties;
  }

  public enum Strategy {
    NoPreempt,
    Preempt
  }

  public enum SchedulerType {
    Fifo,
    Fair
  }

  public enum Status {
    ON,
    OFF,
    INITIALIZING,
    ABNORMAL
  }

  public enum ResourceSystemType {
    FUXI_OFFLINE,
    FUXI_ONLINE,
    FUXI_VW
  }

  public static class BillingPolicy {
    public enum BillingMethod {
      payasyougo,
      subscription
    }

    @SerializedName("billingMethod")
    public String billingMethod;
    @SerializedName("OdpsSpecCode")
    public String specification;
    @SerializedName("orderId")
    public String orderId;

    public static BillingPolicy SUBSCRIPTION(String specification) {
      BillingPolicy policy = new BillingPolicy();
      policy.billingMethod = String.valueOf(BillingMethod.subscription);
      policy.specification = specification;
      return policy;
    }

    public static BillingPolicy PAYASYOUGO(String specification) {
      BillingPolicy policy = new BillingPolicy();
      policy.billingMethod = String.valueOf(BillingMethod.payasyougo);
      policy.specification = specification;
      return policy;
    }

    public BillingPolicy withOrderId(String orderId) {
      this.orderId = orderId;
      return this;
    }

    public boolean isSubscription() {
      if (billingMethod.equals(String.valueOf(BillingMethod.subscription))) {
        return true;
      }
      return false;
    }

    public boolean isPayAsYouGo() {
      if (billingMethod.equals(String.valueOf(BillingMethod.payasyougo))) {
        return true;
      }
      return false;
    }

    public String toString() {
      Gson gson = new Gson();
      return gson.toJson(this);
    }
  }

  public static class AffinityRuleItem {
    public enum RuleMode {
      // Jobs matching the rule should run in the quota
      NORMAL("NORMAL"),
      // Only the jobs matching the rule should run in the quota
      EXCLUSIVE("EXCLUSIVE"),
      // Jobs matching the rule cannot run in the quota
      ANTI("ANTI");

      private String val;
      private RuleMode(String val) {
          this.val = val;
      }
      @Override
      public String toString() {
        return this.val;
      }
    }
    @SerializedName("RuleMode")
    public RuleMode ruleMode;
    @SerializedName("UserList")
    public List<String> userList;
    @SerializedName("ProjectList")
    public List<String> projectList;
    @SerializedName("TaskTypeList")
    public List<String> taskTypeList;
    @SerializedName("ProductIdList")
    public List<String> productIdList;
    @SerializedName("PriorityRange")
    public List<Integer> priorityRange;
    @SerializedName("Settings")
    public Map<String, String> settings;

    public void addUser(String user) {
      if (userList == null) {
        userList = new ArrayList<>();
      }
      userList.add(user);
    }

    public boolean removeUser(String user) {
      if (userList != null) {
        return userList.remove(user);
      }
      return false;
    }

    public void addProject(String project) {
      if (projectList == null) {
        projectList = new ArrayList<>();
      }
      projectList.add(project);
    }

    public boolean removeProject(String project) {
      if (projectList != null) {
        return projectList.remove(project);
      }
      return false;
    }

    public void addTaskType(String taskType) {
      if (taskTypeList == null) {
        taskTypeList = new ArrayList<>();
      }
      taskTypeList.add(taskType);
    }

    public boolean removeTaskType(String taskType) {
      if (taskTypeList != null) {
        return taskTypeList.remove(taskType);
      }
      return false;
    }

    public void addProductId(String prodId) {
      if (productIdList == null) {
        productIdList = new ArrayList<>();
      }
      productIdList.add(prodId);
    }

    public boolean removeProductId(String prodId) {
      if (productIdList != null) {
        return productIdList.remove(prodId);
      }
      return false;
    }

    public void setPriorityRange(int low, int high) {
      if (priorityRange == null) {
        priorityRange = new ArrayList<>(2);
      }
      priorityRange.clear();
      priorityRange.add(low);
      priorityRange.add(high);
    }

    public void addSetting(String key, String value) {
      if (settings == null) {
        settings = new HashMap<>();
      }
      settings.put(key, value);
    }

    public String removeSetting(String key) {
      if (settings != null) {
        return settings.remove(key);
      }
      return null;
    }
  }

  public static class AffinityRule extends HashMap<String, AffinityRuleItem> {}

  static final String VERSION = "wlm";
  static final String MCQA_VERSION = "mcqaVersion";
  private Odps odps;
  private QuotaModel model;
  private String mcqaConnHeader;

  Quota(Odps odps, String regionId, String name) {
    this(odps, regionId, name, null);
  }

  Quota(Odps odps, String regionId, String name, String tenantId) {
    this.odps = odps;
    model = new QuotaModel();
    model.regionId = regionId;
    model.nickname = name;
    model.tenantId = tenantId;
  }

  Quota(Odps odps, QuotaModel model) {
    this.odps = odps;
    this.model = model;
    setLoaded(true);
  }

  public String getSystemInnerName() {
    lazyLoad();
    return model.name;
  }

  public Boolean isEnabled() {
    lazyLoad();
    return model.isEnabled;
  }

  public Boolean forceReservedMin() {
    lazyLoad();
    return model.properties.getOrDefault("ForceReservedMin", "false").equals("true");
  }

  public String getResourceSystemType() {
    lazyLoad();
    return model.resourceSystemType;
  }

  public String sessionServiceName() {
    lazyLoad();
    return model.sessionServiceName;
  }

  public String createTimeMs() {
    lazyLoad();
    return model.createTimeMs;
  }

  public Long getCpu() {
    lazyLoad();
    return model.cpu;
  }

  public Long getMinCpu() {
    lazyLoad();
    return model.minCpu;
  }

  public Long getElasticCpuMax() {
    lazyLoad();
    return model.elasticCpuMax;
  }

  public Long getMemory() {
    lazyLoad();
    return model.memory;
  }

  public Long getMinMemory() {
    lazyLoad();
    return model.minMemory;
  }

  public Long getElasticMemoryMax() {
    lazyLoad();
    return model.elasticMemoryMax;
  }

  public Long getGpu() {
    lazyLoad();
    return model.gpu;
  }

  public Long getMinGpu() {
    lazyLoad();
    return model.minGpu;
  }

  public Long getElasticGpuMax() {
    lazyLoad();
    return model.elasticGpuMax;
  }

  public Long getAdhocCpu() {
    lazyLoad();
    return model.adhocCpu;
  }

  public Long getAdhocMemory() {
    lazyLoad();
    return model.adhocMemory;
  }

  public Strategy getStrategy() {
    lazyLoad();
    return Strategy.valueOf(model.strategy);
  }

  public SchedulerType getSchedulerType() {
    lazyLoad();
    return SchedulerType.valueOf(model.schedulerType);
  }

  public Boolean isParentQuota() {
    lazyLoad();
    return model.isParentGroup;
  }

  public String getParentQuotaId() {
    lazyLoad();
    return model.parentId;
  }

  public Status getStatus() {
    lazyLoad();
    return Status.valueOf(model.status);
  }

  public String getNickname() {
    lazyLoad();
    return model.nickname;
  }

  /**
   * NOTE: DO NOT regard it as a value that never changed.
   * @return System internal id which changes after quota migrating.
   */
  public String getQuotaId() {
    lazyLoad();
    return model.id;
  }

  public String getParentNickname() {
    lazyLoad();
    return model.parentNickname;
  }

  public Double getCpuUsage() {
    lazyLoad();
    return model.cpuUsage;
  }

  public Double getAdhocCpuUsage() {
    lazyLoad();
    return model.adhocCpuUsage;
  }

  public Double getCpuReadyRatio() {
    lazyLoad();
    return model.cpuReadyRatio;
  }

  public Double getMemoryUsage() {
    lazyLoad();
    return model.memoryUsage;
  }

  public Double getAdhocMemoryUsage() {
    lazyLoad();
    return model.adhocMemoryUsage;
  }

  public Double getMemoryReadyRatio() {
    lazyLoad();
    return model.memoryReadyRatio;
  }

  public String getCreatorId() {
    lazyLoad();
    return model.creatorId;
  }

  public String getRegionId() {
    lazyLoad();
    return model.regionId;
  }

  public String getBillingPolicy() {
    lazyLoad();
    return model.billingPolicy;
  }

  public boolean isInteractiveQuota() {
    lazyLoad();
    if (model.resourceSystemType != null && model.resourceSystemType.equalsIgnoreCase(
        ResourceSystemType.FUXI_VW.name())) {
      // current mcqa 1.5 type is also fuxi_vw
      if (model.userDefinedTag != null && model.userDefinedTag.containsKey(MCQA_VERSION)) {
        return false;
      }
      return true;
    }
    return false;
  }

  public String getMcqaConnHeader() {
    lazyLoad();
    return mcqaConnHeader;
  }

  public void setMcqaConnHeader(String mcqaConnHeader) {
    if (StringUtils.isNullOrEmpty(mcqaConnHeader)) {
      throw new IllegalArgumentException("McqaConnHeader cannot be null or empty.");
    }
    setLoaded(true);
    if (model == null) {
      model = new QuotaModel();
    }
    this.model.resourceSystemType = ResourceSystemType.FUXI_VW.name();
    this.mcqaConnHeader = mcqaConnHeader;
  }

  public Map<String, String> getProperties() {
    lazyLoad();
    if (model.properties != null) {
      return new HashMap<>(model.properties);
    }
    return Collections.emptyMap();
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildQuotaResource(model.nickname);
    Map<String, String> params = new HashMap<>();
    // A quota does not belong to a specific project. The project name is used to infer the
    // tenant ID and region ID.
    params.put(Params.ODPS_QUOTA_PROJECT, odps.getDefaultProject());
    params.put(Params.ODPS_QUOTA_VERSION, VERSION);

    // Quotas in different regions could have same nickname. So it is good to specify the region ID
    // when it is available.
    if (!StringUtils.isNullOrEmpty(model.regionId)) {
      params.put(Params.ODPS_QUOTA_REGION_ID, model.regionId);
    }
    if (!StringUtils.isNullOrEmpty(model.tenantId)) {
      params.put(Params.ODPS_QUOTA_TENANT_ID, model.tenantId);
    }
    Response resp = odps.getRestClient().request(resource, "GET", params, null, null);
    try {
      model = SimpleXmlUtils.unmarshal(resp, QuotaModel.class);
      mcqaConnHeader = resp.getHeader(Headers.ODPS_MCQA_CONN);
    } catch (Exception e) {
      throw new OdpsException("Can't bind xml to " + QuotaModel.class, e);
    }

    setLoaded(true);
  }
}
