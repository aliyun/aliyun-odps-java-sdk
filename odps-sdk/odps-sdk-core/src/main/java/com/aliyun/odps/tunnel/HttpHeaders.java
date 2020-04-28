/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps.tunnel;

import com.aliyun.odps.commons.transport.Headers;

// TODO: merge to Headers
public class HttpHeaders extends Headers {

  public static final String HEADER_ODPS_REQUEST_ID = "x-odps-request-id";
  public static final String HEADER_ODPS_TUNNEL_VERSION = "x-odps-tunnel-version";
  public static final String HEADER_STREAM_VERSION = "x-odps-tunnel-stream-version";
  public static final String HEADER_ODPS_CURRENT_PACKID = "x-odps-current-packid";
  public static final String HEADER_ODPS_PACK_TIMESTAMP = "x-odps-pack-timestamp";
  public static final String HEADER_ODPS_NEXT_PACKID = "x-odps-next-packid";
  public static final String HEADER_ODPS_PACK_NUM = "x-odps-pack-num";
  public static final String HEADER_ODPS_VOLUME_FS_PATH = "x-odps-volume-fs-path";
  public static final String HEADER_ODPS_VOLUME_SESSIONID = "x-odps-volume-sessionid";
  public static final String HEADER_ODPS_DATE_TRANSFORM = "odps-tunnel-date-transform";
  public static final String HEADER_ODPS_ROUTED_SERVER = "odps-tunnel-routed-server";
  public static final String HEADER_ODPS_SLOT_NUM = "odps-tunnel-slot-num";

}
