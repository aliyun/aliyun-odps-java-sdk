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

package com.aliyun.odps.storage.write;

import java.io.Serializable;

/**
 * Serializable result produced by a successful {@link TableBlockWriter#commit()}.
 *
 * <p>The service commit message remains an implementation detail. Applications should retain
 * or transport this object and pass it to {@link TableWriteSession#commit(java.util.Collection)}.
 */
public final class BlockWriteResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String sessionId;
  private final int blockNumber;
  private final int attemptNumber;
  private final long recordCount;
  private final String commitMessage;

  BlockWriteResult(String sessionId,
                   int blockNumber,
                   int attemptNumber,
                   long recordCount,
                   String commitMessage) {
    this.sessionId = sessionId;
    this.blockNumber = blockNumber;
    this.attemptNumber = attemptNumber;
    this.recordCount = recordCount;
    this.commitMessage = commitMessage;
  }

  public int getBlockNumber() {
    return blockNumber;
  }

  public int getAttemptNumber() {
    return attemptNumber;
  }

  public long getRecordCount() {
    return recordCount;
  }

  String getCommitMessage() {
    return commitMessage;
  }

  String getSessionId() {
    return sessionId;
  }
}
