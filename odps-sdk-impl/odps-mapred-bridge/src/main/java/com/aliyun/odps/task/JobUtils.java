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

package com.aliyun.odps.task;

import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mapred.bridge.MetaExplorer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JobUtils {

  public static Instance submitOpenMR(Odps odps, String project, String sql, String taskName,
                                      Map<String, String> hints, Map<String, String> alias)
      throws OdpsException {
    return SQLTask.run(odps, project, sql, taskName, hints, alias, "mr");
  }

  public static Instance submitOpenMR(MetaExplorer metaExplorer, String sql, String taskName,
                                      Map<String, String> hints, Map<String, String> aliases,
                                      int priority) throws OdpsException {
    SQLTask task = new SQLTask();
    task.setQuery(sql);
    task.setName(taskName);
    task.setProperty("type", "sql");

    if (hints != null) {
      try {
        hints.put("odps.sql.submit.mode", "script");
        String json = new GsonBuilder().disableHtmlEscaping().create().toJson(hints);
        task.setProperty("settings", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }

    }

    if (aliases != null) {
      try {
        String json = new GsonBuilder().disableHtmlEscaping().create().toJson(aliases);
        task.setProperty("aliases", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }

    }

    return metaExplorer.createInstance(task, priority);
  }
}
