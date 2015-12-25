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

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.security.CheckPermissionConstants.ActionType;
import com.aliyun.odps.security.CheckPermissionConstants.CheckPermissionResult;
import com.aliyun.odps.security.CheckPermissionConstants.ObjectType;

public class SecurityManagerTest extends TestBase {

  SecurityManager sm;

  @Before
  public void setUp() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
    sm = odps.projects().get().getSecurityManager();
  }

  @Test(expected = OdpsException.class)
  public void testNoPermission() throws Exception {
    String query = sm.runQuery("add user aliyun$NOT_EXIST@aliyun.com", false);
  }

  @Test
  public void testGetRolePolicy() throws OdpsException {
    String result = sm.getRolePolicy("admin");
    Assert.assertNotNull(result);
  }

  @Test
  public void testListUsers() throws OdpsException {
    List<User> list = sm.listUsers();
    for (User user : list) {
      Assert.assertNotNull(user.getID());
    }
  }

  @Test
  public void testListRoles() throws OdpsException {
    List<Role> roles = sm.listRoles();
    Assert.assertNotEquals(0, roles.size());
  }

  @Test
  public void testListRolesForUser() {

  }

  @Test
  public void testListUsersForRole() throws OdpsException {
    List<User> list = sm.listUsersForRole("admin");
  }

  @Test
  public void testCheckPermission() throws OdpsException {
    CheckPermissionResult r;
    r = sm.checkPermission(ObjectType.Project, "", ActionType.List);
    Assert.assertEquals(CheckPermissionResult.Allow, r);
  }
}
