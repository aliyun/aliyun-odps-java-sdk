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

package com.aliyun.odps.tunnel;

public interface HttpHeaders {

  public static final String AUTHORIZATION = "Authorization";
  public static final String CACHE_CONTROL = "Cache-Control";
  public static final String CONTENT_DISPOSITION = "Content-Disposition";
  public static final String CONTENT_ENCODING = "Content-Encoding";
  public static final String ACCEPT_ENCODING = "Accept-Encoding";
  public static final String CONTENT_LENGTH = "Content-Length";
  public static final String CONTENT_MD5 = "Content-MD5";
  public static final String CONTENT_TYPE = "Content-Type";
  public static final String TRANSFER_ENCODING = "Transfer-Encoding";
  public static final String CHUNKED = "chunked";
  public static final String DATE = "Date";
  public static final String ETAG = "ETag";
  public static final String EXPIRES = "Expires";
  public static final String HOST = "Host";
  public static final String LAST_MODIFIED = "Last-Modified";
  public static final String RANGE = "Range";
  public static final String LOCATION = "Location";
  public static final String USER_AGENT = "USER-AGENT";
  public static final String HEADER_ODPS_CREATION_TIME = "x-odps-creation-time";
  public static final String HEADER_ODPS_OWNER = "x-odps-owner";
  public static final String HEADER_ODPS_START_TIME = "x-odps-start-time";
  public static final String HEADER_ODPS_END_TIME = "x-odps-end-time";
  public static final String HEADER_ODPS_COPY_TABLE_SOURCE = "x-odps-copy-table-source";
  public static final String HEADER_ODPS_COMMENT = "x-odps-comment";
  public static final String HEADER_ODPS_RESOURCE_NAME = "x-odps-resource-name";
  public static final String HEADER_ODPS_RESOURCE_TYPE = "x-odps-resource-type";
  public static final String HEADER_ODPS_AUTHORIZATION = "Authorization";
  public static final String HEADER_ODPS_PREFIX = "x-odps-";
  public static final String HEADER_ODPS_REQUEST_ID = "x-odps-request-id";
  public static final String HEADER_ALI_DATA_PREFIX = "x-ali-data-";
  public static final String HEADER_ALI_DATA_SERVICE = "x-ali-data-service";
  public static final String HEADER_ALI_DATA_AUTH_METHOD = "x-ali-data-auth-method";
  public static final String HEADER_ALI_DATA_AUTH_SIGNATURE_TYPE = "x-ali-data-auth-signature-type";
  public static final String HEADER_ODPS_TUNNEL_VERSION = "x-odps-tunnel-version";
  public static final String HEADER_STREAM_VERSION = "x-odps-tunnel-stream-version";
  public static final String HEADER_ODPS_BEARER_TOKEN = "x-odps-bearer-token";
  public static final String HEADER_ODPS_CURRENT_PACKID = "x-odps-current-packid";
  public static final String HEADER_ODPS_PACK_TIMESTAMP = "x-odps-pack-timestamp";
  public static final String HEADER_ODPS_NEXT_PACKID = "x-odps-next-packid";
  public static final String HEADER_ODPS_PACK_NUM = "x-odps-pack-num";
}
