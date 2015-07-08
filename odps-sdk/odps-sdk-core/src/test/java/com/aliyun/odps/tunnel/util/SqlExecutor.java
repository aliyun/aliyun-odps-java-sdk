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

package com.aliyun.odps.tunnel.util;

import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Odps;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;

public class SqlExecutor {

  public static String exec(String projectName, String sql) {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    String taskName = "SqlTask";
    SQLTask task = new SQLTask();
    task.setQuery(sql);
    task.setName(taskName);

    String result = null;
    try {
      Instance instance = odps.instances().create(task);
      instance.waitForSuccess();

      Map<String, String> resultMap = instance.getTaskResults();
      result = resultMap.get(task.getName());

      TaskStatus taskStatus = instance.getTaskStatus().get(taskName);
      if (TaskStatus.Status.FAILED.equals(taskStatus.getStatus())) {
        throw new Exception(result);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }
}
