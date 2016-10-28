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

package com.aliyun.odps.mapred.local;

public class StageStatic {

  private String taskId;
  private int workerCount;
  private long totalInputRecords;
  private long maxInputRecords;
  private long minInputRecords;
  private long avgInputRecords;
  private long totalOutputRecords;
  private long maxOutputRecords;
  private long minOutputRecords;
  private long avgOutputRecords;
  private String nextTaskId;

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public int getWorkerCount() {
    return workerCount;
  }

  public void setWorkerCount(int workerCount) {
    this.workerCount = workerCount;
  }

  public long getTotalInputRecords() {
    return totalInputRecords;
  }

  public void setTotalInputRecords(long totalInputRecords) {
    this.totalInputRecords = totalInputRecords;
  }

  public long getMaxInputRecords() {
    return maxInputRecords;
  }

  public void setMaxInputRecords(long maxInputRecords) {
    this.maxInputRecords = maxInputRecords;
  }

  public long getMinInputRecords() {
    return minInputRecords;
  }

  public void setMinInputRecords(long minInputRecords) {
    this.minInputRecords = minInputRecords;
  }

  public long getAvgInputRecords() {
    return avgInputRecords;
  }

  public void setAvgInputRecords(long avgInputRecords) {
    this.avgInputRecords = avgInputRecords;
  }

  public long getTotalOutputRecords() {
    return totalOutputRecords;
  }

  public void setTotalOutputRecords(long totalOutputRecords) {
    this.totalOutputRecords = totalOutputRecords;
  }

  public long getMaxOutputRecords() {
    return maxOutputRecords;
  }

  public void setMaxOutputRecords(long maxOutputRecords) {
    this.maxOutputRecords = maxOutputRecords;
  }

  public long getMinOutputRecords() {
    return minOutputRecords;
  }

  public void setMinOutputRecords(long minOutputRecords) {
    this.minOutputRecords = minOutputRecords;
  }

  public long getAvgOutputRecords() {
    return avgOutputRecords;
  }

  public void setAvgOutputRecords(long avgOutputRecords) {
    this.avgOutputRecords = avgOutputRecords;
  }

  public String getNextTaskId() {
    return nextTaskId;
  }

  public void setNextTaskId(String nextTaskId) {
    this.nextTaskId = nextTaskId;
  }

  public void setInputRecordCount(long recordCount) {
    if (recordCount <= 0) {
      return;
    }
    totalInputRecords += recordCount;

    avgInputRecords = totalInputRecords / workerCount;

    if (recordCount > maxInputRecords) {
      maxInputRecords = recordCount;
    }

    if (minInputRecords == 0 || recordCount < minInputRecords) {
      minInputRecords = recordCount;
    }

  }

  public void setOutputRecordCount(long recordCount) {
    if (recordCount <= 0) {
      return;
    }
    totalOutputRecords += recordCount;

    avgOutputRecords = totalOutputRecords / workerCount;

    if (recordCount > maxOutputRecords) {
      maxOutputRecords = recordCount;
    }

    if (minOutputRecords == 0 || recordCount < minOutputRecords) {
      minOutputRecords = recordCount;
    }

  }

}
