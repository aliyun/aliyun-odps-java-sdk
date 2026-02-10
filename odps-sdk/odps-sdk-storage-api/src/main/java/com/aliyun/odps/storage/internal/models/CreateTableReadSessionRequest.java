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

import java.util.Collections;
import java.util.List;

import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.storage.settings.IncrementalReadOptions;
import com.aliyun.odps.storage.settings.SplitOptions;
import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CreateTableReadSessionRequest {

  @SerializedName("RequiredDataColumns")
  private List<String> requiredDataColumns = Collections.emptyList();

  @SerializedName("RequiredPartitionColumns")
  private List<String> requiredPartitionColumns = Collections.emptyList();

  @SerializedName("RequiredPartitions")
  private List<String> requiredPartitions = Collections.emptyList();

  @SerializedName("RequiredBucketIds")
  private List<Integer> requiredBucketIds = Collections.emptyList();

  @SerializedName("SplitOptions")
  private SplitOptions splitOptions = SplitOptions.newBuilder().build();

  @SerializedName("ArrowOptions")
  private ArrowOptions arrowOptions = ArrowOptions.newBuilder().build();

  @SerializedName("FilterPredicateFallback")
  private boolean filterPredicateFallback = false;

  @SerializedName("FilterPredicate")
  private String filterPredicate = "";

  @SerializedName("SplitMaxFileNum")
  private int splitMaxFileNum = 0;

  @SerializedName("IncrementalReadOptions")
  private IncrementalReadOptions incrementalReadOptions;

  @SerializedName("IncrementalRead")
  private boolean incrementalRead = false;



  public List<String> getRequiredDataColumns() {
    return requiredDataColumns;
  }

  public void setRequiredDataColumns(List<String> requiredDataColumns) {
    this.requiredDataColumns = requiredDataColumns;
  }

  public List<String> getRequiredPartitionColumns() {
    return requiredPartitionColumns;
  }

  public void setRequiredPartitionColumns(List<String> requiredPartitionColumns) {
    this.requiredPartitionColumns = requiredPartitionColumns;
  }

  public List<String> getRequiredPartitions() {
    return requiredPartitions;
  }

  public void setRequiredPartitions(List<String> requiredPartitions) {
    this.requiredPartitions = requiredPartitions;
  }

  public List<Integer> getRequiredBucketIds() {
    return requiredBucketIds;
  }

  public void setRequiredBucketIds(List<Integer> requiredBucketIds) {
    this.requiredBucketIds = requiredBucketIds;
  }

  public SplitOptions getSplitOptions() {
    return splitOptions;
  }

  public void setSplitOptions(SplitOptions splitOptions) {
    this.splitOptions = splitOptions;
  }

  public ArrowOptions getArrowOptions() {
    return arrowOptions;
  }

  public void setArrowOptions(ArrowOptions arrowOptions) {
    this.arrowOptions = arrowOptions;
  }

  public boolean isFilterPredicateFallback() {
    return filterPredicateFallback;
  }

  public void setFilterPredicateFallback(boolean filterPredicateFallback) {
    this.filterPredicateFallback = filterPredicateFallback;
  }

  public String getFilterPredicate() {
    return filterPredicate;
  }

  public void setFilterPredicate(String filterPredicate) {
    this.filterPredicate = filterPredicate;
  }

  public int getSplitMaxFileNum() {
    return splitMaxFileNum;
  }

  public void setSplitMaxFileNum(int splitMaxFileNum) {
    this.splitMaxFileNum = splitMaxFileNum;
  }

  public IncrementalReadOptions getIncrementalReadOptions() {
    return incrementalReadOptions;
  }

  public void setIncrementalReadOptions(
    IncrementalReadOptions incrementalReadOptions) {
    this.incrementalReadOptions = incrementalReadOptions;
  }

  public boolean isIncrementalRead() {
    return incrementalRead;
  }

  public void setIncrementalRead(boolean incrementalRead) {
    this.incrementalRead = incrementalRead;
  }
}