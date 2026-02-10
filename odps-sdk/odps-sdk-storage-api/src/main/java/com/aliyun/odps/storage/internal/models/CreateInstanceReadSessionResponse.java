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

public class CreateInstanceReadSessionResponse {

  @SerializedName("DownloadID")
  private String downloadId;
  @SerializedName("RecordCount")
  private long recordCount;
  @SerializedName("Status")
  private String status;
  @SerializedName("TableSchema")
  private WriteSchema schema;
  @SerializedName("QuotaName")
  private String quotaName;

  public String getDownloadId() {
    return downloadId;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public String getStatus() {
    return status;
  }

  public WriteSchema getSchema() {
    return schema;
  }

  public String getQuotaName() {
    return quotaName;
  }
}