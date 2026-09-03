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

/**
 * Immutable advanced settings for {@link WriteMode#BATCH_COMPATIBLE}.
 *
 * <p>Use {@link #newBuilder()} and pass the result to
 * {@link TableWriteSessionBuilder#withBatchCompatibleOptions(BatchCompatibleOptions)}.
 */
public final class BatchCompatibleOptions {

  private final boolean enhanceWriteCheck;
  private final long maxFieldSize;
  private final int dynamicPartitionLimit;

  private BatchCompatibleOptions(Builder builder) {
    this.enhanceWriteCheck = builder.enhanceWriteCheck;
    this.maxFieldSize = builder.maxFieldSize;
    this.dynamicPartitionLimit = builder.dynamicPartitionLimit;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static BatchCompatibleOptions createDefault() {
    return newBuilder().build();
  }

  public boolean isEnhanceWriteCheck() {
    return enhanceWriteCheck;
  }

  /** Returns zero when the service should use its project-level default. */
  public long getMaxFieldSize() {
    return maxFieldSize;
  }

  public int getDynamicPartitionLimit() {
    return dynamicPartitionLimit;
  }

  /** Builder for batch-compatible settings. */
  public static final class Builder {

    private boolean enhanceWriteCheck;
    private long maxFieldSize;
    private int dynamicPartitionLimit = -1;

    private Builder() {
    }

    public Builder withEnhanceWriteCheck(boolean enhanceWriteCheck) {
      this.enhanceWriteCheck = enhanceWriteCheck;
      return this;
    }

    /**
     * Sets the maximum field size in bytes.
     *
     * @param maxFieldSize a value of at least 1024; omit this option to use the service default
     */
    public Builder withMaxFieldSize(long maxFieldSize) {
      if (maxFieldSize < 1024) {
        throw new IllegalArgumentException("maxFieldSize must be at least 1024 bytes");
      }
      this.maxFieldSize = maxFieldSize;
      return this;
    }

    /** Sets the dynamic partition limit, or {@code -1} to use the service default. */
    public Builder withDynamicPartitionLimit(int dynamicPartitionLimit) {
      if (dynamicPartitionLimit < -1) {
        throw new IllegalArgumentException("dynamicPartitionLimit must be at least -1");
      }
      this.dynamicPartitionLimit = dynamicPartitionLimit;
      return this;
    }

    public BatchCompatibleOptions build() {
      return new BatchCompatibleOptions(this);
    }
  }
}
