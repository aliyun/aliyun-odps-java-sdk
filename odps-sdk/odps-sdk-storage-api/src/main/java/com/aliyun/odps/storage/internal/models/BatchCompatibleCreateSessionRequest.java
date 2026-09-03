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

import com.aliyun.odps.table.configuration.ArrowOptions;
import com.google.gson.annotations.SerializedName;

/** Request body used by {@code WriteMode.BATCH_COMPATIBLE} session creation. */
public class BatchCompatibleCreateSessionRequest {

  @SerializedName("PartitionSpec")
  private String partitionSpec = "";

  @SerializedName("Overwrite")
  private boolean overwrite;

  @SerializedName("DynamicPartitionOptions")
  private DynamicPartitionOptions dynamicPartitionOptions = new DynamicPartitionOptions();

  @SerializedName("ArrowOptions")
  private ArrowOptions arrowOptions = ArrowOptions.createDefault();

  @SerializedName("SupportWriteCluster")
  private boolean supportWriteCluster;

  @SerializedName("MaxFieldSize")
  private long maxFieldSize;

  @SerializedName("EnhanceWriteCheck")
  private boolean enhanceWriteCheck;

  @SerializedName("SupportSaveToPangu")
  private boolean supportSaveToPangu;

  public void setPartitionSpec(String partitionSpec) {
    this.partitionSpec = partitionSpec;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public void setDynamicPartitionOptions(DynamicPartitionOptions dynamicPartitionOptions) {
    this.dynamicPartitionOptions = dynamicPartitionOptions;
  }

  public void setArrowOptions(ArrowOptions arrowOptions) {
    this.arrowOptions = arrowOptions;
  }

  public void setSupportWriteCluster(boolean supportWriteCluster) {
    this.supportWriteCluster = supportWriteCluster;
  }

  public void setMaxFieldSize(long maxFieldSize) {
    this.maxFieldSize = maxFieldSize;
  }

  public void setEnhanceWriteCheck(boolean enhanceWriteCheck) {
    this.enhanceWriteCheck = enhanceWriteCheck;
  }

  public void setSupportSaveToPangu(boolean supportSaveToPangu) {
    this.supportSaveToPangu = supportSaveToPangu;
  }

  /** Dynamic-partition settings in the batch-compatible wire format. */
  public static class DynamicPartitionOptions {

    @SerializedName("InvalidStrategy")
    private String invalidStrategy = "Exception";

    @SerializedName("InvalidLimit")
    private int invalidLimit = -1;

    @SerializedName("DynamicPartitionLimit")
    private int dynamicPartitionLimit = -1;

    public DynamicPartitionOptions() {
    }

    public DynamicPartitionOptions(int dynamicPartitionLimit) {
      this.dynamicPartitionLimit = dynamicPartitionLimit;
    }
  }
}
