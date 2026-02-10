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

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.table.TableIdentifier;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CommitTableWriteSessionRequest {

  private TableIdentifier tableIdentifier;

  private String sessionId;

  private List<String> streamIds;

  private List<Long> streamVersions;

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public String getSessionId() {
    return sessionId;
  }

  public List<String> getStreamIds() {
    return streamIds;
  }

  public List<Long> getStreamVersions() {
    return streamVersions;
  }

  public static CommitTableWriteSessionRequestBuilder newBuilder() {
    return new CommitTableWriteSessionRequestBuilder();
  }


  public static final class CommitTableWriteSessionRequestBuilder {

    private TableIdentifier tableIdentifier;
    private String sessionId;
    private List<String> streamIds = new ArrayList<>();
    private List<Long> streamVersions = new ArrayList<>();

    private CommitTableWriteSessionRequestBuilder() {
    }

    public CommitTableWriteSessionRequestBuilder withTableIdentifier(
      TableIdentifier tableIdentifier) {
      this.tableIdentifier = tableIdentifier;
      return this;
    }

    public CommitTableWriteSessionRequestBuilder withSessionId(String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public CommitTableWriteSessionRequestBuilder withStreamIds(List<String> streamIds) {
      this.streamIds = streamIds;
      return this;
    }

    public CommitTableWriteSessionRequestBuilder withStreamVersions(List<Long> streamVersions) {
      this.streamVersions = streamVersions;
      return this;
    }

    public CommitTableWriteSessionRequest build() {
      CommitTableWriteSessionRequest
        commitTableWriteSessionRequest =
        new CommitTableWriteSessionRequest();
      commitTableWriteSessionRequest.tableIdentifier = this.tableIdentifier;
      commitTableWriteSessionRequest.sessionId = this.sessionId;
      commitTableWriteSessionRequest.streamIds = this.streamIds;
      commitTableWriteSessionRequest.streamVersions = this.streamVersions;
      return commitTableWriteSessionRequest;
    }
  }
}
