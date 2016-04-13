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
package com.aliyun.odps.datahub;

/**
 * DatahubConstants defined for ODPS.
 */

public interface DatahubConstants {

  public static int VERSION = 4;
  public static String RES_PARTITION = "partition";
  public static String RECORD_COUNT = "recordcount";
  public static String PACK_ID = "packid";
  public static String PACK_NUM = "packnum";
  public static String PACK_FETCHMODE = "packfetchmode";
  public static String ITERATE_MODE = "iteratemode";
  public static String ITER_MODE_AT_PACKID = "AT_PACKID";
  public static String ITER_MODE_AFTER_PACKID = "AFTER_PACKID";
  public static String ITER_MODE_FIRST_PACK = "FIRST_PACK";
  public static String ITER_MODE_LAST_PACK = "LAST_PACK";
  public static String SHARD_NUMBER = "shardnumber";
  public static String SHARD_STATUS = "shardstatus";
  public static String LOCAL_ERROR_CODE = "Local Error";
  public static String SEEK_TIME = "timestamp";
  public static String NORMAL_TABLE_TYPE = "normal_type";
  public static String HUB_TABLE_TYPE = "hub_type";
  public static String RESERVED_META_PARTITION = "__partition__";
  public static String TABLE_REPLICATED_TIMESTAMP = "table_replicated_timestamp";
}
