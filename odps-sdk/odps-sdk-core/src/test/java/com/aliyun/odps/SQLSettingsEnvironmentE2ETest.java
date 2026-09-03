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

package com.aliyun.odps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.task.SQLTask;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SQLSettingsEnvironmentE2ETest {

  @Test
  public void testSQLAndMergeTaskSubmission() throws Exception {
    String platformId = System.getenv("MC_PLATFORM_ID");
    String traceparent = System.getenv("TRACEPARENT");
    assumeTrue(platformId != null && !platformId.isEmpty()
                   && traceparent != null && !traceparent.isEmpty());

    Odps odps = OdpsTestUtils.newDefaultOdps();
    String tableName = "odps_java_sdk_ut_agent_lineage_" + System.currentTimeMillis();
    String qualifiedTableName = odps.getDefaultProject() + "." + tableName;

    try {
      SQLTask createTask = new SQLTask();
      createTask.setName("agent_lineage_create_table");
      createTask.setQuery("create table " + tableName + " (id bigint) lifecycle 1;");
      Instance createInstance = odps.instances().create(createTask);
      createInstance.waitForSuccess();
      assertEnvironmentSettings(createTask, platformId, traceparent);
      assertSubmittedTaskSettings(createInstance, platformId, traceparent);
      System.out.println("SQL instance: " + createInstance.getId());

      MergeTask mergeTask = new MergeTask("agent_lineage_merge_table", qualifiedTableName);
      Instance mergeInstance = odps.instances().create(mergeTask);
      mergeInstance.waitForSuccess();
      assertEnvironmentSettings(mergeTask, platformId, traceparent);
      assertSubmittedTaskSettings(mergeInstance, platformId, traceparent);
      System.out.println("MergeTask instance: " + mergeInstance.getId());
    } finally {
      Instance dropInstance = SQLTask.run(odps, "drop table if exists " + tableName + ";");
      dropInstance.waitForSuccess();
      System.out.println("Cleanup instance: " + dropInstance.getId());
    }
  }

  private void assertSubmittedTaskSettings(Instance instance, String platformId,
                                           String traceparent) throws OdpsException {
    assertEquals(1, instance.getTasks().size());
    assertEnvironmentSettings(instance.getTasks().get(0), platformId, traceparent);
  }

  private void assertEnvironmentSettings(Task task, String platformId, String traceparent) {
    String settingsValue = task.getProperties().get("settings");
    assertNotNull(settingsValue);
    JsonObject settings = JsonParser.parseString(settingsValue).getAsJsonObject();
    assertEquals(platformId, settings.get("EXT_PLATFORM_ID").getAsString());
    assertEquals(traceparent, settings.get("EXT_TASK_ID").getAsString());
  }
}
