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

  public static int VERSION = 4;
  public static String RES_PARTITION = "partition";
  public static String RES_SHARD = "shard";
  public static String RES_COLUMNS = "columns";
  public static String UPLOADS = "uploads";
  public static String UPLOADID = "uploadid";
  public static String DOWNLOADS = "downloads";
  public static String DOWNLOADID = "downloadid";
  public static String BLOCKID = "blockid";
  public static String ROW_RANGE = "rowrange";
  public static String RANGE = "range";
  public static String TARGET = "target";
  public static String TYPE = "type";
  public static String RESUME_MODE = "resume";
  public static String RECORD_COUNT = "recordcount";
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
  public static String CACHED = "cached";
  public static String TASK_NAME = "taskname";
  public static String OVERWRITE = "overwrite";
}
