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

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TenantTest extends TestBase {

  @Test
  @Ignore
  public void testGetTenantUseCache() throws OdpsException {
    Odps cacheOdps = odps.clone();
    cacheOdps.options().setAllowStaleMetadataRead(true);
    cacheOdps.getRestClient().addRequestInterceptor(context -> {
      // test get project and extended info
      assertEquals("true", context.getRequest().getParameters().getOrDefault("cached", ""));
      return context.getRequest();
    });
    String schema = cacheOdps.tenant().getProperty("odps.namespace.schema");
    assertEquals("false", schema);
  }
}
