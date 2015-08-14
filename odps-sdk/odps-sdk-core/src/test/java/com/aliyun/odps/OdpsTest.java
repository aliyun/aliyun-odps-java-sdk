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
import static org.junit.Assert.assertFalse;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;

public class OdpsTest {

  @Test
  public void testSets() {
    Odps odps = OdpsTestUtils.newDefaultOdps();

    // endpoint
    odps.setEndpoint("xxxx");
    Assert.assertEquals("xxxx", odps.getEndpoint());

    // default project
    odps.setDefaultProject("prj");
    Assert.assertEquals("prj", odps.getDefaultProject());
  }

  @Test
  public void testClone() {
    Odps odps1 = OdpsTestUtils.newDefaultOdps();
    odps1.instances().setDefaultRunningCluster("test_cluster");
    Odps odps2 = odps1.clone();
    assertFalse(odps1 == odps2);
    assertEquals(odps1.getAccount(), odps2.getAccount());
    assertEquals(odps1.getUserAgent(), odps2.getUserAgent());
    assertEquals(odps1.getEndpoint(), odps2.getEndpoint());
    assertEquals(odps1.getDefaultProject(), odps2.getDefaultProject());
    assertEquals(odps1.instances().getDefaultRunningCluster(), odps2.instances().getDefaultRunningCluster());
  }

  @Test
  public void testHttps() throws OdpsException {
    Odps odps = OdpsTestUtils.newHttpsOdps();
    odps.getRestClient().setIgnoreCerts(true);
    assertFalse(odps.projects().exists("NOT_EXIST_PROJECT"));
  }
}
