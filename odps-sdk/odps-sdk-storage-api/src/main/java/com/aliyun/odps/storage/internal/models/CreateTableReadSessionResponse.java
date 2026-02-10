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

import java.util.List;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.storage.models.DataFormat;
import com.aliyun.odps.storage.models.SessionStats;
import com.aliyun.odps.storage.models.SplitMode;
import com.aliyun.odps.storage.settings.IncrementalReadOptions;
import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CreateTableReadSessionResponse {

  @SerializedName("DataSchema")
  private ReadSchema dataSchema;

  @SerializedName("EnableLargeString")
  private boolean enableLargeString;

  @SerializedName("ExpirationTime")
  private long expirationTime;

  @SerializedName("IncrementalReadOptions")
  private IncrementalReadOptions incrementalReadOptions; // 复用之前的类

  @SerializedName("LatestVersion")
  private long latestVersion;

  @SerializedName("Message")
  private String message;

  @SerializedName("RecordCount")
  private long recordCount;

  @SerializedName("SessionId")
  private String sessionId;

  @SerializedName("SessionStats")
  private SessionStats sessionStats;

  @SerializedName("SessionStatus")
  private String sessionStatus;

  @SerializedName("SessionType")
  private String sessionType;

  @SerializedName("SplitBucketId")
  private List<Integer> splitBucketId;

  @SerializedName("SplitMode")
  private SplitMode splitMode;

  @SerializedName("SplitsCount")
  private int splitsCount;

  @SerializedName("SupportedDataFormat")
  private List<DataFormat> supportedDataFormat;


  public ReadSchema getDataSchema() {
    return dataSchema;
  }

  public void setDataSchema(ReadSchema dataSchema) {
    this.dataSchema = dataSchema;
  }

  public boolean isEnableLargeString() {
    return enableLargeString;
  }

  public void setEnableLargeString(boolean enableLargeString) {
    this.enableLargeString = enableLargeString;
  }

  public long getExpirationTime() {
    return expirationTime;
  }

  public void setExpirationTime(long expirationTime) {
    this.expirationTime = expirationTime;
  }

  public IncrementalReadOptions getIncrementalReadOptions() {
    return incrementalReadOptions;
  }

  public void setIncrementalReadOptions(
    IncrementalReadOptions incrementalReadOptions) {
    this.incrementalReadOptions = incrementalReadOptions;
  }

  public long getLatestVersion() {
    return latestVersion;
  }

  public void setLatestVersion(long latestVersion) {
    this.latestVersion = latestVersion;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(long recordCount) {
    this.recordCount = recordCount;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public SessionStats getSessionStats() {
    return sessionStats;
  }

  public void setSessionStats(SessionStats sessionStats) {
    this.sessionStats = sessionStats;
  }

  public String getSessionStatus() {
    return sessionStatus;
  }

  public void setSessionStatus(String sessionStatus) {
    this.sessionStatus = sessionStatus;
  }

  public String getSessionType() {
    return sessionType;
  }

  public void setSessionType(String sessionType) {
    this.sessionType = sessionType;
  }

  public List<Integer> getSplitBucketId() {
    return splitBucketId;
  }

  public void setSplitBucketId(List<Integer> splitBucketId) {
    this.splitBucketId = splitBucketId;
  }

  public SplitMode getSplitMode() {
    return splitMode;
  }

  public void setSplitMode(SplitMode splitMode) {
    this.splitMode = splitMode;
  }

  public int getSplitsCount() {
    return splitsCount;
  }

  public void setSplitsCount(int splitsCount) {
    this.splitsCount = splitsCount;
  }

  public List<DataFormat> getSupportedDataFormat() {
    return supportedDataFormat;
  }

  public void setSupportedDataFormat(
    List<DataFormat> supportedDataFormat) {
    this.supportedDataFormat = supportedDataFormat;
  }
}