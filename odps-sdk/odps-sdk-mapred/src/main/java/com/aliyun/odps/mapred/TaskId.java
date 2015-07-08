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

package com.aliyun.odps.mapred;

import java.text.NumberFormat;

/**
 * {@link Mapper} 或 {@link Reducer} 在运行时会分成多个Task并发执行，TaskID是Task的唯一标识.
 *
 * <p>
 * TaskID格式为：StageId_InstanceId，例如： <br/>
 * M1_Stg1_00000， 其中：<br/>
 * <ul>
 * <li>Stage Id：M1_Stg1<br/>
 * <li>Instance Id： Instance序号，从0计起，格式化为六位，不足前面添0<br/>
 * </ul>
 *
 * </p>
 */
public class TaskId {

  protected static final char SEPARATOR = '_';

  protected static final NumberFormat taskidFormat = NumberFormat.getInstance();

  private String taskId;

  private int instId;

  static {
    taskidFormat.setGroupingUsed(false);
    taskidFormat.setMinimumIntegerDigits(6);
  }

  /**
   * 构造TaskID
   *
   * @param taskId
   *     Task的唯一标识
   * @param instId
   *     Instance序号，从0计起
   */
  public TaskId(String taskId, int instId) {
    super();
    this.taskId = taskId;
    this.instId = instId;
  }

  /**
   * 是否map
   *
   * @return 是否map
   */
  public boolean isMap() {
    return taskId.toUpperCase().charAt(0) == 'M';
  }

  /**
   * 获取Task序号
   *
   * @return Task序号
   */
  public String getTaskId() {
    return taskId;
  }

  /**
   * 获取Instance序号
   *
   * @return Instance序号
   */
  public int getInstId() {
    return instId;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(taskId);
    builder.append("_" + taskidFormat.format(instId));
    return builder.toString();
  }

}
