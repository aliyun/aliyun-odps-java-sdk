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

import com.aliyun.odps.table.TableIdentifier;
import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class GetWriteStreamRequest {

  @SerializedName("TableId")
  private String tableId;

  private TableIdentifier tableIdentifier;

  private String sessionId;

  @SerializedName("StreamId")
  private String streamId;

  @SerializedName("StreamVersion")
  private Long streamVersion;


  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getStreamId() {
    return streamId;
  }

  public Long getStreamVersion() {
    return streamVersion;
  }


  public static GetWriteStreamRequestBuilder newBuilder() {
    return new GetWriteStreamRequestBuilder();
  }

  public static final class GetWriteStreamRequestBuilder {

    private TableIdentifier tableIdentifier;

    private String sessionId;

    private String streamId;

    private Long streamVersion;


    private GetWriteStreamRequestBuilder() {
    }

    public GetWriteStreamRequestBuilder withTableIdentifier(TableIdentifier tableIdentifier) {
      this.tableIdentifier = tableIdentifier;
      return this;
    }

    public GetWriteStreamRequestBuilder withSessionId(String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public GetWriteStreamRequestBuilder withStreamId(String streamId) {
      this.streamId = streamId;
      return this;
    }

    public GetWriteStreamRequestBuilder withStreamVersion(Long streamVersion) {
      this.streamVersion = streamVersion;
      return this;
    }


    public GetWriteStreamRequest build() {
      GetWriteStreamRequest getWriteStreamRequest = new GetWriteStreamRequest();
      getWriteStreamRequest.tableIdentifier = this.tableIdentifier;
      getWriteStreamRequest.sessionId = this.sessionId;
      getWriteStreamRequest.streamId = this.streamId;
      getWriteStreamRequest.streamVersion = this.streamVersion;
      return getWriteStreamRequest;
    }
  }
}