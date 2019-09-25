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

package com.aliyun.odps.security;

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;

import junit.framework.Assert;

public class ListRoleForUserTest extends TestBase {

  SecurityManager sm;
  String ADMIN_ROLE = "admin";

  // RAM 子账号
  String RAM_USER = OdpsTestUtils.getRamUser();

  // 多重身份
  String MULTI_ID_USER = OdpsTestUtils.getMultiIDUser();
  String MULTI_ID_KP = OdpsTestUtils.getMultiIDKp();
  String MULTI_ID_NICKNAME = OdpsTestUtils.getMultiIDNickname();

  // 只有 nickname
  String NICKER_NICKANME = OdpsTestUtils.getNickerNickname();

  @Before
  public void setUp() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
    sm = odps.projects().get().getSecurityManager();

    tryRunSecurityQuery("revoke " + ADMIN_ROLE + " from " + RAM_USER);
    tryRunSecurityQuery("remove user " + RAM_USER);
    tryRunSecurityQuery("revoke " + ADMIN_ROLE + " from " + MULTI_ID_USER);
    tryRunSecurityQuery("remove user " + MULTI_ID_USER);
    tryRunSecurityQuery("revoke " + ADMIN_ROLE + " from " + NICKER_NICKANME);
    tryRunSecurityQuery("remove user " + NICKER_NICKANME);

    tryRunSecurityQuery("add user " + RAM_USER);
    tryRunSecurityQuery("grant " + ADMIN_ROLE + " to " + RAM_USER);
    tryRunSecurityQuery("add user " + MULTI_ID_USER);
    tryRunSecurityQuery("grant " + ADMIN_ROLE + " to " + MULTI_ID_USER);
    tryRunSecurityQuery("add user " + NICKER_NICKANME);
    tryRunSecurityQuery("grant " + ADMIN_ROLE + " to " + NICKER_NICKANME);
  }

  @After
  public void tearDown() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
    sm = odps.projects().get().getSecurityManager();

    tryRunSecurityQuery("revoke " + ADMIN_ROLE + " from " + RAM_USER);
    tryRunSecurityQuery("remove user " + RAM_USER);
    tryRunSecurityQuery("revoke " + ADMIN_ROLE + " from " + MULTI_ID_USER);
    tryRunSecurityQuery("remove user " + MULTI_ID_USER);
    tryRunSecurityQuery("revoke " + ADMIN_ROLE + " from " + NICKER_NICKANME);
    tryRunSecurityQuery("remove user " + NICKER_NICKANME);
  }

  private void tryRunSecurityQuery(String sql) {
    try {
      sm.runQuery(sql, false);
    } catch (OdpsException ignore) {
    }
  }

  @Test
  public void testListRolesForUser() throws OdpsException, IOException {
    List<Role> roles = sm.listRolesForUserName(RAM_USER);
    Assert.assertEquals(1, roles.size());
    for (Role r : roles) {
      Assert.assertEquals(ADMIN_ROLE, r.getName());
    }

    List<Role> roles2 = sm.listRolesForUserName(MULTI_ID_USER);
    Assert.assertEquals(1, roles.size());
    for (Role r : roles2) {
      Assert.assertEquals(ADMIN_ROLE, r.getName());
    }

    List<Role> roles3 = sm.listRolesForUserID(MULTI_ID_KP);
    Assert.assertEquals(1, roles.size());
    for (Role r : roles3) {
      Assert.assertEquals(ADMIN_ROLE, r.getName());
    }

    List<Role> roles4 = sm.listRolesForUserName(MULTI_ID_NICKNAME);
    Assert.assertEquals(1, roles.size());
    for (Role r : roles4) {
      Assert.assertEquals(ADMIN_ROLE, r.getName());
    }

    List<Role> roles5 = sm.listRolesForUserName(NICKER_NICKANME);
    Assert.assertEquals(1, roles.size());
    for (Role r : roles5) {
      Assert.assertEquals(ADMIN_ROLE, r.getName());
    }
  }
}
