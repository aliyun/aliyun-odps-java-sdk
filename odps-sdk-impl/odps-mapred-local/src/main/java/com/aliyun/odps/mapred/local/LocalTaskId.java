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

import com.aliyun.odps.mapred.TaskId;

public class LocalTaskId extends TaskId {

  private String projectName;

  public LocalTaskId(String taskId, int instId, String projectName) {
    super(taskId, instId);
    this.projectName = projectName;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(getTaskId());
    builder.append(SEPARATOR);
    builder.append(taskidFormat.format(getInstId()));
    builder.append(SEPARATOR);
    builder.append(projectName);
    builder.append(SEPARATOR);
    builder.append("LOT");
    builder.append(SEPARATOR);
    builder.append("0");
    builder.append(SEPARATOR);
    builder.append("0");
    builder.append(SEPARATOR);
    builder.append("0");
    builder.append(SEPARATOR);
    builder.append("job0");
    return builder.toString();
  }

}
