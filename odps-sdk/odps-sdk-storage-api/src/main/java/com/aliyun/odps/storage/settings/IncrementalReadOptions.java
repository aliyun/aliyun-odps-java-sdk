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

package com.aliyun.odps.storage.settings;

import com.google.gson.annotations.SerializedName;

/**
 * Configuration class for incremental read options used by the MaxCompute Storage API client.
 *
 * <p>This class allows configuration of incremental read settings including
 * time ranges and version ranges for reading data incrementally.
 *
 * <p>Example usage:
 * <pre>{@code
 * IncrementalReadOptions options = IncrementalReadOptions.newBuilder()
 *     .withMode("timestamp")
 *     .withStartTimeStamp("2024-01-01 00:00:00")
 *     .withEndTimeStamp("2024-01-02 00:00:00")
 *     .build();
 * }</pre>
 */
public class IncrementalReadOptions {

  @SerializedName("EndTimeStamp")
  private String endTimeStamp;

  @SerializedName("EndVersion")
  private long endVersion = -1L;

  @SerializedName("Mode")
  private String mode;

  @SerializedName("StartTimeStamp")
  private String startTimeStamp;

  @SerializedName("StartVersion")
  private long startVersion = -1L;

  @SerializedName("StartTxnId")
  private String startTxnId;

  private IncrementalReadOptions() {
    // Used by Gson; an absent version is not version zero.
  }

  private IncrementalReadOptions(Builder builder) {
    this.endTimeStamp = builder.endTimeStamp;
    this.endVersion = builder.endVersion;
    this.mode = builder.mode;
    this.startTimeStamp = builder.startTimeStamp;
    this.startVersion = builder.startVersion;
    this.startTxnId = builder.startTxnId;
  }

  /**
   * Creates a new builder for constructing IncrementalReadOptions instances.
   *
   * @return A new Builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Gets the end timestamp.
   *
   * @return The end timestamp
   */
  public String getEndTimeStamp() {
    return endTimeStamp;
  }

  /**
   * Gets the end version.
   *
   * @return The end version
   */
  public long getEndVersion() {
    return endVersion;
  }

  /**
   * Gets the mode.
   *
   * @return The mode
   */
  public String getMode() {
    return mode;
  }

  /**
   * Gets the start timestamp.
   *
   * @return The start timestamp
   */
  public String getStartTimeStamp() {
    return startTimeStamp;
  }

  /**
   * Gets the start version.
   *
   * @return The start version
   */
  public long getStartVersion() {
    return startVersion;
  }

  /**
   * Gets the start transaction id.
   *
   * @return The start transaction id, may be null
   */
  public String getStartTxnId() {
    return startTxnId;
  }

  /**
   * Builder class for constructing IncrementalReadOptions instances.
   */
  public static class Builder {

    private String endTimeStamp;
    private long endVersion = -1L;
    private String mode;
    private String startTimeStamp;
    private long startVersion = -1L;
    private String startTxnId;

    private Builder() {
    }

    /**
     * Sets the end timestamp.
     *
     * @param endTimeStamp The end timestamp
     * @return This builder instance for method chaining
     */
    public Builder withEndTimeStamp(String endTimeStamp) {
      this.endTimeStamp = endTimeStamp;
      return this;
    }

    /**
     * Sets the end version.
     *
     * @param endVersion The end version
     * @return This builder instance for method chaining
     */
    public Builder withEndVersion(long endVersion) {
      this.endVersion = endVersion;
      return this;
    }

    /**
     * Sets the mode for incremental read.
     *
     * @param mode The mode (e.g., "timestamp" or "version")
     * @return This builder instance for method chaining
     */
    public Builder withMode(String mode) {
      this.mode = mode;
      return this;
    }

    /**
     * Sets the start timestamp.
     *
     * @param startTimeStamp The start timestamp
     * @return This builder instance for method chaining
     */
    public Builder withStartTimeStamp(String startTimeStamp) {
      this.startTimeStamp = startTimeStamp;
      return this;
    }

    /**
     * Sets the start version.
     *
     * @param startVersion The start version
     * @return This builder instance for method chaining
     */
    public Builder withStartVersion(long startVersion) {
      this.startVersion = startVersion;
      return this;
    }

    /**
     * Sets the start transaction id for exactly-once incremental reads.
     *
     * @param startTxnId The start transaction id
     * @return This builder instance for method chaining
     */
    public Builder withStartTxnId(String startTxnId) {
      this.startTxnId = startTxnId;
      return this;
    }

    /**
     * Builds and returns a new IncrementalReadOptions instance with the configured settings.
     *
     * @return A new IncrementalReadOptions instance
     */
    public IncrementalReadOptions build() {
      return new IncrementalReadOptions(this);
    }
  }
}
