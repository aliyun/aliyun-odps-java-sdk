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

import com.aliyun.odps.storage.models.SplitMode;
import com.google.gson.annotations.SerializedName;

/**
 * Configuration class for data split options used when reading from MaxCompute tables.
 *
 * <p>This class defines the strategy for splitting data into chunks for parallel processing.
 * It allows configuration of split mode, unit, number, and whether to allow cross-partition splits.
 *
 * <p>Example usage:
 * <pre>{@code
 * SplitOptions splitOptions = SplitOptions.newBuilder()
 *     .withSplitMode(SplitMode.Size)
 *     .withSplitUnit("ByteSize")
 *     .withSplitNumber(256 * 1024 * 1024)
 *     .withCrossPartition(true)
 *     .build();
 * }</pre>
 */
public class SplitOptions {

  @SerializedName("SplitMode")
  private SplitMode splitMode;

  @SerializedName("SplitUnit")
  private String splitUnit;

  @SerializedName("SplitNumber")
  private long splitNumber;

  @SerializedName("CrossPartition")
  private boolean crossPartition;

  private SplitOptions() {
  }

  public static SplitOptionsBuilder newBuilder() {
    return new SplitOptionsBuilder();
  }

  /**
   * Gets the split mode.
   *
   * @return The split mode
   */
  public SplitMode getSplitMode() {
    return splitMode;
  }

  /**
   * Sets the split mode.
   *
   * @param splitMode The split mode
   */
  public void setSplitMode(SplitMode splitMode) {
    this.splitMode = splitMode;
  }

  /**
   * Gets the split unit.
   *
   * @return The split unit
   */
  public String getSplitUnit() {
    return splitUnit;
  }

  /**
   * Sets the split unit.
   *
   * @param splitUnit The split unit
   */
  public void setSplitUnit(String splitUnit) {
    this.splitUnit = splitUnit;
  }

  /**
   * Gets the split number.
   *
   * @return The split number
   */
  public long getSplitNumber() {
    return splitNumber;
  }

  /**
   * Sets the split number.
   *
   * @param splitNumber The split number
   */
  public void setSplitNumber(long splitNumber) {
    this.splitNumber = splitNumber;
  }

  /**
   * Checks if cross-partition splits are allowed.
   *
   * @return true if cross-partition splits are allowed, false otherwise
   */
  public boolean isCrossPartition() {
    return crossPartition;
  }

  /**
   * Sets whether cross-partition splits are allowed.
   *
   * @param crossPartition true to allow cross-partition splits, false otherwise
   */
  public void setCrossPartition(boolean crossPartition) {
    this.crossPartition = crossPartition;
  }

  public static final class SplitOptionsBuilder {

    private SplitMode splitMode = SplitMode.SIZE;
    private String splitUnit = "ByteSize";
    private long splitNumber = 256 * 1024 * 1024;
    private boolean crossPartition = true;

    private SplitOptionsBuilder() {
    }

    /**
     * Sets the split mode.
     *
     * @param splitMode The split mode
     * @return This builder instance for method chaining
     */
    public SplitOptionsBuilder withSplitMode(SplitMode splitMode) {
      this.splitMode = splitMode;
      return this;
    }

    /**
     * Sets the split raw size, unit Byte
     *
     * @param splitSize The raw size per split raw size, unit Byte
     * @return This builder instance for method chaining
     */
    public SplitOptionsBuilder withSplitSize(long splitSize) {
      this.splitMode = SplitMode.SIZE;
      this.splitNumber = splitSize;
      this.splitUnit = "ByteSize";
      return this;
    }

    /**
     * Sets the split row count
     *
     * @param rowCount The row count per split
     * @return This builder instance for method chaining
     */
    public SplitOptionsBuilder withSplitRowCount(long rowCount) {
      this.splitMode = SplitMode.ROW_OFFSET;
      this.splitNumber = rowCount;
      return this;
    }

    /**
     * Sets whether cross-partition splits are allowed.
     *
     * @param crossPartition true to allow cross-partition splits, false otherwise
     * @return This builder instance for method chaining
     */
    public SplitOptionsBuilder withCrossPartition(boolean crossPartition) {
      this.crossPartition = crossPartition;
      return this;
    }

    /**
     * Builds and returns a new SplitOptions instance with the configured settings.
     *
     * @return A new SplitOptions instance
     */
    public SplitOptions build() {
      SplitOptions splitOptions = new SplitOptions();
      splitOptions.setSplitMode(splitMode);
      splitOptions.setSplitUnit(splitUnit);
      splitOptions.setSplitNumber(splitNumber);
      splitOptions.setCrossPartition(crossPartition);
      return splitOptions;
    }
  }
}
