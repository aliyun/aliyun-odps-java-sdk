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

import com.aliyun.odps.storage.models.DataFormat;
import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CreateTableReadStreamRequest {

  @SerializedName("MaxBatchRows")
  private long maxBatchRows = 4096;

  @SerializedName("SkipRowNum")
  private long skipRowNum = 0;

  @SerializedName("MaxBatchRawSize")
  private long maxBatchRawSize = 0;

  @SerializedName("DataFormat")
  private DataFormat dataFormat = DataFormat.newBuilder().build();

  @SerializedName("DataColumns")
  private List<String> dataColumns = new ArrayList<>();

  @SerializedName("DataColumnsUnordered")
  private boolean dataColumnsUnordered = false;

  public long getMaxBatchRows() {
    return maxBatchRows;
  }

  public void setMaxBatchRows(long maxBatchRows) {
    this.maxBatchRows = maxBatchRows;
  }

  public long getSkipRowNum() {
    return skipRowNum;
  }

  public void setSkipRowNum(long skipRowNum) {
    this.skipRowNum = skipRowNum;
  }

  public long getMaxBatchRawSize() {
    return maxBatchRawSize;
  }

  public void setMaxBatchRawSize(long maxBatchRawSize) {
    this.maxBatchRawSize = maxBatchRawSize;
  }

  public DataFormat getDataFormat() {
    return dataFormat;
  }

  public void setDataFormat(DataFormat dataFormat) {
    this.dataFormat = dataFormat;
  }

  public List<String> getDataColumns() {
    return dataColumns;
  }

  public void setDataColumns(List<String> dataColumns) {
    this.dataColumns = dataColumns;
  }

  public boolean isDataColumnsUnordered() {
    return dataColumnsUnordered;
  }

  public void setDataColumnsUnordered(boolean dataColumnsUnordered) {
    this.dataColumnsUnordered = dataColumnsUnordered;
  }
}