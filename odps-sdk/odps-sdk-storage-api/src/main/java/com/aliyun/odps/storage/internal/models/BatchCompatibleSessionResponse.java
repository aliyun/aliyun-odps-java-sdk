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

import com.google.gson.annotations.SerializedName;

/** Session response returned by the batch-compatible block protocol. */
public class BatchCompatibleSessionResponse {

  @SerializedName("SessionId")
  private String sessionId;

  @SerializedName("SessionStatus")
  private String sessionStatus;

  @SerializedName("DataSchema")
  private DataSchema dataSchema;

  @SerializedName("MaxBlockNumber")
  private long maxBlockNumber;

  @SerializedName("EnhanceWriteCheck")
  private boolean enhanceWriteCheck;

  @SerializedName("Message")
  private String message;

  private String routeToken;

  public String getSessionId() {
    return sessionId;
  }

  public String getSessionStatus() {
    return sessionStatus;
  }

  public DataSchema getDataSchema() {
    return dataSchema;
  }

  public long getMaxBlockNumber() {
    return maxBlockNumber;
  }

  public boolean isEnhanceWriteCheck() {
    return enhanceWriteCheck;
  }

  public String getMessage() {
    return message;
  }

  public String getRouteToken() {
    return routeToken;
  }

  public void setRouteToken(String routeToken) {
    this.routeToken = routeToken;
  }

  /** Table schema returned by the block protocol. */
  public static class DataSchema {

    @SerializedName("DataColumns")
    private List<Column> dataColumns;

    @SerializedName("PartitionColumns")
    private List<Column> partitionColumns;

    public List<Column> getDataColumns() {
      return dataColumns;
    }

    public List<Column> getPartitionColumns() {
      return partitionColumns;
    }
  }

  /** Column definition returned by the block protocol. */
  public static class Column {

    @SerializedName("Name")
    private String name;

    @SerializedName("Type")
    private String type;

    @SerializedName("Comment")
    private String comment;

    @SerializedName("Nullable")
    private boolean nullable = true;

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public String getComment() {
      return comment;
    }

    public boolean isNullable() {
      return nullable;
    }
  }
}
