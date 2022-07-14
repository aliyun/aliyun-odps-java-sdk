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

package com.aliyun.odps.commons.transport;

import java.util.HashMap;
import java.util.Map;

public class Headers {

  /**
   * Used by multiple modules
   */
  public static final String CACHE_CONTROL = "Cache-Control";
  public static final String CONTENT_ENCODING = "Content-Encoding";
  public static final String CONTENT_LENGTH = "Content-Length";
  public static final String CONTENT_MD5 = "Content-MD5";
  public static final String CONTENT_TYPE = "Content-Type";
  public static final String DATE = "Date";
  public static final String ETAG = "ETag";
  public static final String EXPIRES = "Expires";
  public static final String HOST = "Host";
  // TODO: the last modified header used by volume is "Last_Modified", hard coded in the server
  //  side. Should fix it later.
  public static final String LAST_MODIFIED = "Last-Modified";
  public static final String RANGE = "Range";
  public static final String LOCATION = "Location";
  public static final String TRANSFER_ENCODING = "Transfer-Encoding";
  public static final String CHUNKED = "chunked";
  public static final String ACCEPT_ENCODING = "Accept-Encoding";
  public static final String USER_AGENT = "User-Agent";
  public static final String ODPS_OWNER = "x-odps-owner";
  public static final String ODPS_CREATION_TIME = "x-odps-creation-time";
  public static final String ODPS_REQUEST_ID = "x-odps-request-id";
  public static final String ODPS_USER_AGENT = "x-odps-user-agent";
  public static final String ODPS_SUPERVISION_TOKEN = "odps-x-supervision-token";
  public static final String TUNNEL_SCHEMA = "odps-tunnel-schema";
  public static final String TUNNEL_RECORD_COUNT = "odps-tunnel-record-count";

  /**
   * Used by Resource
   */
  public static final String ODPS_COPY_TABLE_SOURCE = "x-odps-copy-table-source";
  public static final String ODPS_COPY_FILE_SOURCE = "x-odps-copy-file-source";
  public static final String ODPS_COMMENT = "x-odps-comment";
  public static final String ODPS_RESOURCE_NAME = "x-odps-resource-name";
  public static final String ODPS_RESOURCE_TYPE = "x-odps-resource-type";
  public static final String ODPS_RESOURCE_IS_TEMP = "x-odps-resource-istemp";
  public static final String ODPS_RESOURCE_LAST_UPDATOR = "x-odps-updator";
  public static final String ODPS_RESOURCE_SIZE = "x-odps-resource-size";
  public static final String SCHEMA_NAME = "schema-name";
  public static final String CONTENT_DISPOSITION = "Content-Disposition";
  public static final String ODPS_RESOURCE_HAS_REMAINING_CONTENT = "x-odps-resource-has-remaining";
  public static final String ODPS_RESOURCE_MERGE_TOTAL_BYTES = "x-odps-resource-merge-total-bytes";

  /**
   * Used by Instance
   */
  public static final String ODPS_START_TIME = "x-odps-start-time";
  public static final String ODPS_END_TIME = "x-odps-end-time";

  /**
   * Used by Quota
   */
  public static final String QUOTA_ID = "quota-id";

  /**
   * Used by Account
   */
  public static final String AUTHORIZATION = "Authorization";
  public static final String AUTHORIZATION_STS_TOKEN = "authorization-sts-token";
  public static final String GROUP_AUTHORIZATION = "Group-Authorization";
  public static final String APP_AUTHENTICATION = "application-authentication";
  public static final String STS_AUTHENTICATION = "sts-authentication";
  public static final String STS_TOKEN = "sts-token";
  public static final String ODPS_BEARER_TOKEN = "x-odps-bearer-token";
  public static final String ALI_DATA_SERVICE = "x-ali-data-service";
  public static final String ALI_DATA_AUTH_SIGNATURE_TYPE = "x-ali-data-auth-signature-type";
  public static final String ALI_DATA_AUTH_METHOD = "x-ali-data-auth-method";

  public static final Map<String, String> LOWER_CASE_HEADER_NAME_TO_HEADER_NAME = new HashMap<String, String>(){{
    put(CACHE_CONTROL.toLowerCase(), CACHE_CONTROL);
    put(CONTENT_ENCODING.toLowerCase(), CONTENT_ENCODING);
    put(CONTENT_LENGTH.toLowerCase(), CONTENT_LENGTH);
    put(CONTENT_MD5.toLowerCase(), CONTENT_MD5);
    put(CONTENT_TYPE.toLowerCase(), CONTENT_TYPE);
    put(DATE.toLowerCase(), DATE);
    put(ETAG.toLowerCase(), ETAG);
    put(EXPIRES.toLowerCase(), EXPIRES);
    put(HOST.toLowerCase(), HOST);
    put(LAST_MODIFIED.toLowerCase(), LAST_MODIFIED);
    put(RANGE.toLowerCase(), RANGE);
    put(LOCATION.toLowerCase(), LOCATION);
    put(TRANSFER_ENCODING.toLowerCase(), TRANSFER_ENCODING);
    put(CHUNKED.toLowerCase(), CHUNKED);
    put(ACCEPT_ENCODING.toLowerCase(), ACCEPT_ENCODING);
    put(USER_AGENT.toLowerCase(), USER_AGENT);
    put(ODPS_OWNER.toLowerCase(), ODPS_OWNER);
    put(ODPS_CREATION_TIME.toLowerCase(), ODPS_CREATION_TIME);
    put(ODPS_REQUEST_ID.toLowerCase(), ODPS_REQUEST_ID);
    put(ODPS_USER_AGENT.toLowerCase(), ODPS_USER_AGENT);
    put(ODPS_SUPERVISION_TOKEN.toLowerCase(), ODPS_SUPERVISION_TOKEN);
    put(TUNNEL_SCHEMA.toLowerCase(), TUNNEL_SCHEMA);
    put(TUNNEL_RECORD_COUNT.toLowerCase(), TUNNEL_RECORD_COUNT);
    put(ODPS_COPY_TABLE_SOURCE.toLowerCase(), ODPS_COPY_TABLE_SOURCE);
    put(ODPS_COPY_FILE_SOURCE.toLowerCase(), ODPS_COPY_FILE_SOURCE);
    put(ODPS_COMMENT.toLowerCase(), ODPS_COMMENT);
    put(ODPS_RESOURCE_NAME.toLowerCase(), ODPS_RESOURCE_NAME);
    put(ODPS_RESOURCE_TYPE.toLowerCase(), ODPS_RESOURCE_TYPE);
    put(ODPS_RESOURCE_IS_TEMP.toLowerCase(), ODPS_RESOURCE_IS_TEMP);
    put(ODPS_RESOURCE_LAST_UPDATOR.toLowerCase(), ODPS_RESOURCE_LAST_UPDATOR);
    put(ODPS_RESOURCE_SIZE.toLowerCase(), ODPS_RESOURCE_SIZE);
    put(CONTENT_DISPOSITION.toLowerCase(), CONTENT_DISPOSITION);
    put(ODPS_START_TIME.toLowerCase(), ODPS_START_TIME);
    put(ODPS_END_TIME.toLowerCase(), ODPS_END_TIME);
    put(QUOTA_ID.toLowerCase(), QUOTA_ID);
    put(AUTHORIZATION.toLowerCase(), AUTHORIZATION);
    put(GROUP_AUTHORIZATION.toLowerCase(), GROUP_AUTHORIZATION);
    put(APP_AUTHENTICATION.toLowerCase(), APP_AUTHENTICATION);
    put(STS_AUTHENTICATION.toLowerCase(), STS_AUTHENTICATION);
    put(STS_TOKEN.toLowerCase(), STS_TOKEN);
    put(ODPS_BEARER_TOKEN.toLowerCase(), ODPS_BEARER_TOKEN);
    put(ALI_DATA_SERVICE.toLowerCase(), ALI_DATA_SERVICE);
    put(ALI_DATA_AUTH_SIGNATURE_TYPE.toLowerCase(), ALI_DATA_AUTH_SIGNATURE_TYPE);
    put(ALI_DATA_AUTH_METHOD.toLowerCase(), ALI_DATA_AUTH_METHOD);
  }};

  public static String toCaseSensitiveHeaderName(String headerName) {
    if (headerName == null) {
      return null;
    }

    String caseSensitiveHeaderName =
        LOWER_CASE_HEADER_NAME_TO_HEADER_NAME.get(headerName.toLowerCase());
    return caseSensitiveHeaderName == null ? headerName : caseSensitiveHeaderName;
  }
}
