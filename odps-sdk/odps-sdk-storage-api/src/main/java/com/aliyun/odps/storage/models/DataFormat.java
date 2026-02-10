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
 * Data format configuration for read/write operations.
 *
 * <p>This class defines the data format type and version to be used for
 * reading or writing data. The default format is Arrow V5.
 *
 * <p>Example usage:
 * <pre>{@code
 * DataFormat format = DataFormat.newBuilder()
 *     .withType("Arrow")
 *     .withVersion("V5")
 *     .build();
 * }</pre>
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class DataFormat {

  private static final String DEFAULT_TYPE = "Arrow";
  private static final String DEFAULT_VERSION = "V5";

  @SerializedName("Type")
  private String type;

  @SerializedName("Version")
  private String version;

  private DataFormat(Builder builder) {
    this.type = builder.type;
    this.version = builder.version;
  }

  /**
   * Creates a new builder for constructing DataFormat instances.
   *
   * @return A new Builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Gets the data format type.
   *
   * @return The data format type
   */
  public String getType() {
    return type;
  }

  /**
   * Gets the data format version.
   *
   * @return The data format version
   */
  public String getVersion() {
    return version;
  }

  /**
   * Builder class for constructing DataFormat instances.
   */
  public static class Builder {

    private String type = DEFAULT_TYPE;
    private String version = DEFAULT_VERSION;

    private Builder() {
    }

    /**
     * Sets the data format type.
     *
     * @param type The data format type (e.g., "Arrow")
     * @return This builder instance for method chaining
     */
    public Builder withType(String type) {
      this.type = type;
      return this;
    }

    /**
     * Sets the data format version.
     *
     * @param version The data format version (e.g., "V5")
     * @return This builder instance for method chaining
     */
    public Builder withVersion(String version) {
      this.version = version;
      return this;
    }

    /**
     * Builds and returns a new DataFormat instance with the configured settings.
     *
     * @return A new DataFormat instance
     */
    public DataFormat build() {
      return new DataFormat(this);
    }
  }
}
