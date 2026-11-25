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

import java.util.Collection;

import com.google.gson.annotations.SerializedName;

/** Commit body used by the batch-compatible block protocol. */
public class BatchCompatibleCommitRequest {

  @SerializedName("CommitMessages")
  private final Collection<String> commitMessages;

  @SerializedName("WaitFlyingWritersTimeoutSeconds")
  private final Integer waitFlyingWritersTimeoutSeconds;

  public BatchCompatibleCommitRequest(Collection<String> commitMessages) {
    this(commitMessages, null);
  }

  public BatchCompatibleCommitRequest(Collection<String> commitMessages,
                                      Integer waitFlyingWritersTimeoutSeconds) {
    if (waitFlyingWritersTimeoutSeconds != null
        && (waitFlyingWritersTimeoutSeconds < 1 || waitFlyingWritersTimeoutSeconds > 600)) {
      throw new IllegalArgumentException(
          "WaitFlyingWritersTimeoutSeconds must be between 1 and 600 seconds");
    }
    this.commitMessages = commitMessages;
    this.waitFlyingWritersTimeoutSeconds = waitFlyingWritersTimeoutSeconds;
  }
}
