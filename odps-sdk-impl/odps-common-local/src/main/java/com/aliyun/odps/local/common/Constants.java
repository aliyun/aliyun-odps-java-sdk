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

package com.aliyun.odps.local.common;

public class Constants {

  // Warehouse
  public static final String WAREHOUSE_HOUSE_DIR = "warehouse";
  public static final String TABLES_DIR = "__tables__";
  public static final String RESOURCES_DIR = "__resources__";
  public static final String SCHEMA_FILE = "__schema__";

  public static final String NULL_TOKEN = "\\N";

  // LocalConf
  public static final String JOB_NAME = "odps.mapred.job.name";
  public static final String LOCAL_TEMP_DIR = "odps.mapred.local.temp.dir";
  public static final String LOCAL_TEMP_RETAIN = "odps.mapred.local.temp.retain";
  public static final String LOCAL_RECORD_LIMIT = "odps.mapred.local.record.download.limit";
  public static final String
      LOCAL_INPUT_COLUMN_SEPERATOR =
      "odps.mapred.local.input.column.seperator";
  public static final String
      LOCAL_OUTPUT_COLUMN_SEPERATOR =
      "odps.mapred.local.output.column.seperator";
  public static final int LIMIT_DOWNLOAD_RECORD = 10000;
  public static final int DEFAULT_DOWNLOAD_RECORD = 100;

  public static final String DATE_FORMAT_1 = "yyyyMMddHHmmss_SSS";
  public static final String DATE_FORMAT_2 = "yyyy-MM-dd HH:mm:ss SSS";
  
  //security
  public static final String LOCAL_SECURITY_ENABLE="odps.local.security.enable";
  public static final String LOCAL_SECURITY_JNI_ENABLE="odps.local.security.jni.enable";
  public static final String LOCAL_USER_DEFINE_POLICY="odps.local.user.define.policy";

}
