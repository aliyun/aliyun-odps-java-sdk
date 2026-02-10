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

package com.aliyun.odps.storage.models;


import com.google.gson.annotations.SerializedName;

/**
 * Statistics information for a read/write session.
 *
 * <p>This class provides estimated statistics about a session, including
 * the estimated data size and row count.
 *
 * <p>Example usage:
 * <pre>{@code
 * SessionStats stats = SessionStats.newBuilder()
 *     .withEstimatedSize(1024 * 1024 * 100)
 *     .withEstimatedRowCount(1000000)
 *     .build();
 * }</pre>
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SessionStats {

  @SerializedName("EstimatedSize")
  private long estimatedSize;

  @SerializedName("EstimatedRowCount")
  private long estimatedRowCount;

  private SessionStats(Builder builder) {
    this.estimatedSize = builder.estimatedSize;
    this.estimatedRowCount = builder.estimatedRowCount;
  }

  /**
   * Creates a new builder for constructing SessionStats instances.
   *
   * @return A new Builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Gets the estimated data size in bytes.
   *
   * @return The estimated data size in bytes
   */
  public long getEstimatedSize() {
    return estimatedSize;
  }

  /**
   * Gets the estimated row count.
   *
   * @return The estimated row count
   */
  public long getEstimatedRowCount() {
    return estimatedRowCount;
  }

  /**
   * Builder class for constructing SessionStats instances.
   */
  public static class Builder {

    private long estimatedSize;
    private long estimatedRowCount;

    private Builder() {
    }

    /**
     * Sets the estimated data size in bytes.
     *
     * @param estimatedSize The estimated data size in bytes
     * @return This builder instance for method chaining
     */
    public Builder withEstimatedSize(long estimatedSize) {
      this.estimatedSize = estimatedSize;
      return this;
    }

    /**
     * Sets the estimated row count.
     *
     * @param estimatedRowCount The estimated row count
     * @return This builder instance for method chaining
     */
    public Builder withEstimatedRowCount(long estimatedRowCount) {
      this.estimatedRowCount = estimatedRowCount;
      return this;
    }

    /**
     * Builds and returns a new SessionStats instance with the configured settings.
     *
     * @return A new SessionStats instance
     */
    public SessionStats build() {
      return new SessionStats(this);
    }
  }
}
