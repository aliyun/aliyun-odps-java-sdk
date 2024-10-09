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

/**
 *
 */
package com.aliyun.odps.tunnel;

/**
 * TunnelConstants defined for OPDS.
 */
public interface TunnelConstants {

  public static int VERSION = 5;
  public static String RES_PARTITION = "partition";
  public static String RES_SHARD = "shard";
  public static String RES_COLUMNS = "columns";
  public static String UPLOADS = "uploads";
  public static String UPLOADID = "uploadid";
  public static String DOWNLOADS = "downloads";
  public static String DOWNLOADID = "downloadid";
  public static String BLOCKID = "blockid";
  public static String ROW_RANGE = "rowrange";
  public static String SIZE_LIMIT = "sizelimit";
  public static String RANGE = "range";
  public static String TARGET = "target";
  public static String TYPE = "type";
  public static String RESUME_MODE = "resume";
  public static String RECORD_COUNT = "record_count";
  public static String PACK_ID = "packid";
  public static String PACK_NUM = "packnum";
  public static String ITERATE_MODE = "iteratemode";
  public static String ITER_MODE_AT_PACKID = "AT_PACKID";
  public static String ITER_MODE_AFTER_PACKID = "AFTER_PACKID";
  public static String SHARD_NUMBER = "shardnumber";
  public static String SHARD_STATUS = "shardstatus";
  public static String LOCAL_ERROR_CODE = "Local Error";
  public static String SEEK_TIME = "timestamp";
  public static String MODE = "mode";
  public static String STREAM_UPLOAD = "streamupload";
  public static String INSTANCE_TUNNEL_LIMIT_ENABLED = "instance_tunnel_limit_enabled";
  public static String ASYNC_MODE ="asyncmode";
  public static String TUNNEL_DATE_TRANSFORM_VERSION = "v1";
  public static String OVERWRITE = "overwrite";
  public static String CACHED = "cached";
  public static String TASK_NAME = "taskname";
  public static String QUERY_ID = "queryid";
  public static String SCHEMA_IN_STREAM = "schema_in_stream";
  public static String STREAMS = "streams";
  public static String SLOT_ID = "slotid";
  public static String CREATE_PARTITION = "create_partition";
  public static String ZORDER_COLUMNS = "zorder_columns";
  public static String PARAM_ARROW = "arrow";
  public static String META_FIELD_VERSION = "__version";
  public static String META_FIELD_APP_VERSION = "__app_version";
  public static String META_FIELD_OPERATION = "__operation";
  public static String META_FIELD_KEY_COLS = "__key_cols";
  public static String META_FIELD_VALUE_COLS = "__value_cols";
  public static String UPSERTS = "upserts";
  public static String SLOT_NUM = "slotnum";
  public static String UPSERT_ID = "upsertid";
  public static String BUCKET_ID = "bucketid";
  public static String SESSION_STATUS_NORMAL = "normal";
  public static String SESSION_STATUS_COMMITTING = "committing";
  public static String SESSION_STATUS_COMMITTED = "committed";
  public static String SESSION_STATUS_EXPIRED = "expired";
  public static String SESSION_STATUS_CRITICAL = "critical";
  public static String SESSION_STATUS_ABORTED = "aborted";
  public static String PARAM_QUOTA_NAME= "quotaName";
  public static String PARAM_SHARING_QUOTA_TOKEN= "sharingQuotaToken";
  public static String PARAM_BLOCK_VERSION = "block_version";
  public static String PARAM_CHECK_LATEST_SCHEMA = "check_latest_schema";
  public static String ENABLE_PARTIAL_UPDATE = "enable_partial_update";
  public static String SCHEMA_VERSION = "schema_version";
  public static String PARAM_DISABLE_MODIFIED_CHECK = "disable_modified_check";
  public static String GET_BLOCK_ID = "getblockid";
  static final String INSTANCE_NOT_TERMINATED = "InstanceNotTerminate";
  static final String TASK_FAILED = "TaskFailed";
  public static String LIFECYCLE = "lifecycle";
}
