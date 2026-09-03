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

package com.aliyun.odps.storage.internal.models;

import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CreateWriteStreamResponse {

  @SerializedName("TableSchema")
  protected WriteSchema dataSchema;

  /**
   * TableId returned in streaming mode, required for flush operations.
   */
  @SerializedName("TableId")
  protected String tableId;

  /**
   * SchemaVersion returned in streaming mode, required for flush operations.
   */
  @SerializedName("SchemaVersion")
  protected Long schemaVersion;

  private String routeToken;

  @SerializedName("AccessToken")
  private String accessToken;

  /**
   * One-time quota reservation token returned for batch-compatible block writes.
   */
  @SerializedName("QuotaToken")
  private String quotaToken;

  public WriteSchema getDataSchema() {
    return dataSchema;
  }

  public void setDataSchema(WriteSchema dataSchema) {
    this.dataSchema = dataSchema;
  }

  public String getRouteToken() {
    return routeToken;
  }

  public void setRouteToken(String routeToken) {
    this.routeToken = routeToken;
  }

  public String getTableId() {
    return tableId;
  }

  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  public Long getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(Long schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public String getQuotaToken() {
    return quotaToken;
  }

  public void setQuotaToken(String quotaToken) {
    this.quotaToken = quotaToken;
  }
}
