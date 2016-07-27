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

package com.aliyun.odps.graph.local;

import java.text.NumberFormat;

public class GraphTaskAttemptID {

  protected static final String ATTEMPT = "attempt";

  protected static final char SEPARATOR = '_';

  protected static final NumberFormat taskidFormat = NumberFormat.getInstance();

  private String jobId;

  private int taskId;

  private int attempt;

  static {
    taskidFormat.setGroupingUsed(false);
    taskidFormat.setMinimumIntegerDigits(6);
  }

  /**
   * 构造TaskAttemptID
   *
   * @param jobId
   *     作业一次运行的唯一标识
   * @param taskId
   *     Task序号，从0计起
   * @param attempt
   *     重试次数
   */
  public GraphTaskAttemptID(String jobId, int taskId, int attempt) {
    super();
    this.jobId = jobId;
    this.taskId = taskId;
    this.attempt = attempt;
  }

  /**
   * 获取作业一次运行的唯一标识
   *
   * @return 作业一次运行的唯一标识
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * 获取Task序号
   *
   * @return Task序号
   */
  public int getTaskId() {
    return taskId;
  }

  /**
   * 获取重试次数
   *
   * @return 重试次数
   */
  public int getAttempt() {
    return attempt;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(ATTEMPT);
    builder.append("_" + jobId);
    builder.append("_" + taskidFormat.format(taskId));
    builder.append("_" + attempt);
    return builder.toString();
  }

}
