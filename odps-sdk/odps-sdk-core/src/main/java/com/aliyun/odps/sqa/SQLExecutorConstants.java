package com.aliyun.odps.sqa;

/**
 * Created by dongxiao on 2020/3/16.
 */
public class SQLExecutorConstants {
  public static final String DEFAULT_TASK_NAME = "console_sqlrt_task";
  public static final String DEFAULT_SERVICE = "public.default";
  public static final String DEFAULT_OFFLINE_TASKNAME = "sqlrt_fallback_task";
  public static final int MaxRetryTimes = 1;
  public static final String sessionExceptionFlag = "ODPS-180";
  public static final String sessionReattachFlag = "ODPS-181";
  public static final String sessionAccessDenyFlag = "ODPS-182";
  public static final String sessionResourceNotEnoughFlag = "ODPS-183";
  public static final String sessionUnavailableFlag = "ODPS-184";
  public static final String sessionUnsupportedFeatureFlag = "ODPS-185";
  public static final String sessionQueryTimeoutFlag = "ODPS-186";
  public static final String sessionNoPublicServiceFlag = "ODPS-1820002";
  public static final String sessionTunnelTimeoutFlag = "OdpsTaskTimeout";
  public static final String sessionJobCancelledFlag = "Job is cancelled";
  public static final String sessionJobCancelledComplierFlag = "OdpsJobCancelledException";
  public static final Long DEFAULT_ATTACH_TIMEOUT = 60L;
  public static final String sessionNotSelectException = "InstanceTypeNotSupported";
  public static final String sessionNotSelectMessage = "Non select query not supported";
  public static final String sessionTunnelTimeoutMessage = "Wait for cache data timeout";
  public static final String sessionTunnelGetSelectDescTimeoutMessage = "Get select desc from SQLRTTask timeout";
  public static final String SQA_TASK_FLAG = "odps.task.sql.sqa.enable";
  public static final String SESSION_FALLBACK_TRACER = "odps.sql.session.fallback.instance";
  public static final String WLM_QUOTA_FLAG = "odps.task.wlm.quota";
  public static final String QUOTA_BILLING_METHOD = "billingMethod";
}
