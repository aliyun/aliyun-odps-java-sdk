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
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

public class ProjectTest extends TestBase {

  @Test
  public void testGetName() throws OdpsException {
    boolean flag = odps.projects().exists("project_not_exist");
    assertEquals(false, flag);
  }

  @Test
  public void testGetExtendedProperties() throws Exception {
    Map<String, String> properties = odps.projects().get().getExtendedProperties();
    assertTrue(properties.size() > 0);
    System.out.println(properties);
  }

  @Test
  public void testGetBuid() throws OdpsException {
    Project p = odps.projects().get();
    assertTrue(p.getProperties().containsKey("buid"));
  }

  @Test
  public void testGetTunnelEndpoint() throws Exception {
    String server = odps.projects().get().getTunnelEndpoint();
    assertNotNull(server);
    System.out.println(server);

    try {
      odps.projects().get("not_exists").getTunnelEndpoint();
      assertNotNull("Expect exception when project not found", null);
    } catch (OdpsException e) {
      assertTrue(e.getMessage().contains("not found"));
    }
  }

}
