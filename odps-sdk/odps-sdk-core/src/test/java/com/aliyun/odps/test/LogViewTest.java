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

package com.aliyun.odps.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.task.SQLTask;

public class LogViewTest extends TestBase {

  @Test
  public void testLogView() throws OdpsException {
    try {
      LogView log = odps.logview();
      Instance i = SQLTask.run(odps, "select 1;");
      String logview = log.generateLogView(i, 7 * 24);
      System.out.println(logview);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Request timeout"));
    }
  }


  @Test
  public void testLogViewNotNull() {
    LogView log = odps.logview();
    System.out.println(log.getLogViewHost());
    assertNotNull(log.getLogViewHost());
  }


  @Test
  public void testLogViewHost() throws OdpsException {
    try {
      odps.options().setUseLegacyLogview(true);
      LogView log = odps.logview();
      log.setLogViewHost("http://test.a.b.c");
      Instance i = SQLTask.run(odps, "select 1;");
      assertTrue(
          log.generateLogView(i, 7 * 24).startsWith("http://test.a.b.c/logview"));
    } catch (OdpsException e) {
      assertTrue(e.getMessage().contains("Request timeout"));
    } finally {
      odps.options().setUseLegacyLogview(null);
    }
  }

  @Test
  public void testLogViewHostV2() throws OdpsException {
    try {
      LogView log = odps.logview();
      log.setJobInsightHost("http://test.a.b.c");
      Instance i = SQLTask.run(odps, "select 1;");
      assertTrue(
          log.generateLogView(i).startsWith("http://test.a.b.c"));
      assertTrue(
          log.generateLogView(i).contains("job-insights"));
    } catch (OdpsException e) {
      assertTrue(e.getMessage().contains("Request timeout"));
    }
  }
}
