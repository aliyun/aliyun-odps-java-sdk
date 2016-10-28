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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
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
    String grantUser = OdpsTestUtils.getGrantUser();
    if (!grantUser.toUpperCase().startsWith("ALIYUN$")) {
      grantUser = "ALIYUN$" + grantUser;
    }
    try {
      sm.runQuery("add user " + grantUser, false);
    } catch (OdpsException e) {
    }

    try {
      sm.runQuery("grant admin to " + grantUser, false);
    } catch (OdpsException e) {
    }
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
      user.reload(); // reload user
    }
  }

  @Test
  public void testListRoles() throws OdpsException {
    List<Role> roles = sm.listRoles();
    Assert.assertNotEquals(0, roles.size());
  }

  @Test
  public void testListRolesForUser() throws OdpsException, IOException {
    List<User> list = sm.listUsers();
    Assert.assertNotEquals(0, list.size());
    User u = list.get(0);
    System.err.println("list roles for uid " + u.getID());
    List<Role> r1 = sm.listRolesForUserID(u.getID());
    for (Role role : r1) {
      System.err.println(role.getName());
    }
    System.err.println("list roles for username " + u.getDisplayname());
    List<Role> r2 = sm.listRolesForUserName(u.getDisplayname());
    for (Role role : r2) {
      System.err.println(role.getName());
    }
    Assert.assertEquals(r1.size(), r2.size());
    for (int i = 0; i < r1.size(); i++) {
      Assert.assertEquals(r1.get(i).getName(), r2.get(i).getName());
    }
  }

  @Test
  public void testListRolesForUserName() throws OdpsException, IOException {
    List<Role> roles = sm.listRolesForUserName(OdpsTestUtils.getGrantUser());
    for (Role role : roles) {
      System.err.println(role.getName());
    }
  }

  @Test
  public void testListUsersForRole() throws OdpsException {
    List<User> list = sm.listUsersForRole("admin");
  }

  @Test
  public void testCheckPermission() throws OdpsException {
    CheckPermissionResult r;
    r = sm.checkPermission(ObjectType.Project, "", ActionType.List);
    assertEquals(CheckPermissionResult.Allow, r);
  }

}
