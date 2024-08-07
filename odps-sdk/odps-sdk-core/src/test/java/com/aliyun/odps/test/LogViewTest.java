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

import java.io.IOException;
import java.util.Iterator;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.tunnel.TunnelException;

public class LogViewTest extends TestBase {

  @BeforeClass
  public static void setup() throws TunnelException, OdpsException, IOException {
    // skip run in 5ktest env
    Assume.assumeFalse(OdpsTestUtils.getEnv() == OdpsTestUtils.Env._5ktest);
  }

  @Test
  public void testLogView() throws OdpsException {
    LogView log = odps.logview();
    Iterator<Instance> instIter = odps.instances().iterator();
    instIter.hasNext();
    Instance i = instIter.next();
    String logview = log.generateLogView(i, 7 * 24);
    System.out.println(logview);
  }



  @Test
  public void testLogViewNotNull() {
    LogView log = odps.logview();
    System.out.println(log.getLogViewHost());
    assertNotNull(log.getLogViewHost());
  }


  @Test
  public void testLogViewHost() throws OdpsException {
    LogView log = odps.logview();
    log.setLogViewHost("http://test.a.b.c");
    Iterator<Instance> instIter = odps.instances().iterator();
    instIter.hasNext();
    Instance i = instIter.next();
    assertTrue(
        log.generateLogView(i, 7 * 24).startsWith("http://test.a.b.c/logview"));
  }
}
