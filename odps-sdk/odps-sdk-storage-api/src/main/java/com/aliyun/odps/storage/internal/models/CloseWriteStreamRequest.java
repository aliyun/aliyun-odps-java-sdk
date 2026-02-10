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
public class CloseWriteStreamRequest {

  @SerializedName("SessionId")
  private String sessionId;

  @SerializedName("StreamId")
  private String streamId;

  @SerializedName("StreamVersion")
  private Long streamVersion;

  public String getSessionId() {
    return sessionId;
  }

  public String getStreamId() {
    return streamId;
  }

  public Long getStreamVersion() {
    return streamVersion;
  }


  public static CloseWriteStreamRequestBuilder newBuilder() {
    return new CloseWriteStreamRequestBuilder();
  }

  public static final class CloseWriteStreamRequestBuilder {

    private String sessionId;

    private String streamId;

    private Long streamVersion;


    private CloseWriteStreamRequestBuilder() {
    }

    public CloseWriteStreamRequestBuilder withSessionId(String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public CloseWriteStreamRequestBuilder withStreamId(String streamId) {
      this.streamId = streamId;
      return this;
    }

    public CloseWriteStreamRequestBuilder withStreamVersion(Long streamVersion) {
      this.streamVersion = streamVersion;
      return this;
    }


    public CloseWriteStreamRequest build() {
      CloseWriteStreamRequest closeWriteStreamRequest = new CloseWriteStreamRequest();
      closeWriteStreamRequest.sessionId = this.sessionId;
      closeWriteStreamRequest.streamId = this.streamId;
      closeWriteStreamRequest.streamVersion = this.streamVersion;
      return closeWriteStreamRequest;
    }
  }
}