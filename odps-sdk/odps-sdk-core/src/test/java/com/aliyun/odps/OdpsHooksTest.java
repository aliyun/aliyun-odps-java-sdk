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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Instance.Status;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TunnelException;

public class OdpsHooksTest extends TestBase {

  static boolean beforeDone = false;
  static boolean afterDone = false;

  private static String TABLE_NAME = "test_test_hook";


  public static class TestHook extends OdpsHook {

    boolean beforeMessage = false;

    @Override
    public void before(Job job, Odps odps) throws OdpsException {
      assertTrue(job.getTasks().get(0) instanceof SQLTask);
      SQLTask task = (SQLTask) job.getTasks().get(0);

      assertEquals(task.getQuery(), "select count(*) from " + TABLE_NAME + ";");
      beforeMessage = true;
      beforeDone = true;
    }

    @Override
    public void after(Instance instance, Odps odps) throws OdpsException {
      assertTrue(beforeMessage);
      assertTrue(instance.getStatus() == Status.TERMINATED);
      System.out.println(instance.getTaskSummary("AnonymousSQLTask"));
      afterDone = true;
    }

  }

  @Test
  public void testOdpsHooks() throws OdpsException {
    OdpsHooks.registerHook(TestHook.class);

    Instance i = SQLTask.run(odps, "select count(*) from " + TABLE_NAME + ";");
    i.waitForSuccess();

    assertTrue(beforeDone == true);
    assertTrue(afterDone == true);
  }

  @BeforeClass
  public static void setup() throws TunnelException, OdpsException, IOException {
    OdpsTestUtils.createTableForTest(TABLE_NAME);
    OdpsHooks.getRegisteredHooks().clear();
  }

  @AfterClass
  public static void cleanup() {
    OdpsHooks.getRegisteredHooks().clear();
  }
}
