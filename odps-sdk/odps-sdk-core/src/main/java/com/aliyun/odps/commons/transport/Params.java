package com.aliyun.odps.commons.transport;

public class Params {

  /**
   * Used by {@link com.aliyun.odps.Resource}
   */
  // Temp resources created with ODPS_RESOURCE_IS_PART will overwrite the existing one even the HTTP
  // method is a POST
  public static final String ODPS_RESOURCE_IS_PART = "rIsPart";
  public static final String ODPS_RESOURCE_OP_MERGE = "rOpMerge";
  public static final String ODPS_RESOURCE_FETCH_OFFSET = "rOffset";
  public static final String ODPS_RESOURCE_FETCH_READ_SIZE = "rSize";

  public static final String ODPS_SCHEMA_NAME = "curr_schema";

  /**
   * Used by {@link com.aliyun.odps.Quota}
   */
  public static final String ODPS_QUOTA_PROJECT = "project";
  public static final String ODPS_QUOTA_REGION_ID = "region";
  public static final String ODPS_QUOTA_TENANT_ID = "tenant";
  public static final String ODPS_QUOTA_VERSION = "version";

  public static final String ODPS_PERMANENT= "permanent";
}
