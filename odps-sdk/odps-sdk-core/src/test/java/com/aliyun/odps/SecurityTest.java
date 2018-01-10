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

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.security.SecurityConfiguration;
import com.aliyun.odps.security.SecurityManager;

public class SecurityTest extends TestBase {

  private Odps odps = OdpsTestUtils.newSecurityOdps();

  @Test
  public void securityConfigrationTest() {
    try {
      SecurityManager sm = odps.projects().get().getSecurityManager();
      SecurityConfiguration sc = sm.getSecurityConfiguration();
      sc.disableCheckPermissionUsingAcl();
      sc.disableCheckPermissionUsingPolicy();
      sc.disableLabelSecurity();
      sc.disableObjectCreatorHasAccessPermission();
      sc.disableObjectCreatorHasGrantPermission();
      sc.disableProjectProtection();
      sm.setSecurityConfiguration(sc);
      sc.reload();

      // test CheckPermissionUsingAcl
      sc.enableCheckPermissionUsingAcl();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.checkPermissionUsingAcl(), true);
      sc.disableCheckPermissionUsingAcl();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.checkPermissionUsingAcl(), false);

      // test CheckPermissionUsingPolicy
      sc.enableCheckPermissionUsingPolicy();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.checkPermissionUsingPolicy(), true);
      sc.disableCheckPermissionUsingPolicy();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.checkPermissionUsingPolicy(), false);

      // test LabelSecurity
      sc.enableLabelSecurity();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.labelSecurity(), true);
      sc.disableLabelSecurity();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.labelSecurity(), false);

      // test ObjectCreatorHasAccessPermission
      sc.enableObjectCreatorHasAccessPermission();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.objectCreatorHasAccessPermission(), true);
      sc.disableObjectCreatorHasAccessPermission();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.objectCreatorHasAccessPermission(), false);

      // test ObjectCreatorHasGrantPermission
      sc.enableObjectCreatorHasGrantPermission();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.objectCreatorHasGrantPermission(), true);
      sc.disableObjectCreatorHasGrantPermission();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.objectCreatorHasGrantPermission(), false);

      // test projectProtection
      sc.enableProjectProtection();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.projectProtection(), true);
//      Assert.assertEquals(sc.getProjectProtectionExceptionPolicy(), "");
      sc.disableProjectProtection();
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.projectProtection(), false);
      String
          policy =
          "{\"Statement\": [{\"Action\": [\"*\"],\"Effect\": \"Allow\",\"Principal\": [\"ALIYUN$odpstest1@aliyun.com\"],\"Resource\": [\"*\"]}],\"Version\": \"1\"}";
      sc.enableProjectProtection(policy);
      sm.setSecurityConfiguration(sc);
      sc.reload();
      Assert.assertEquals(sc.projectProtection(), true);
      Assert.assertTrue(sc.getProjectProtectionExceptionPolicy().contains(
          "ALIYUN$odpstest1@aliyun.com"));

      // Auth version
      System.out.println(sc.getAuthorizationVersion());
    } catch (OdpsException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }


  public void queryTest() {
    SecurityManager sm = null;
    try {
      sm = odps.projects().get().getSecurityManager();
      sm.runQuery("add user ALIYUN$odpsteyyyyst1@aliyun.com;", true);
    } catch (OdpsException e) {
      e.printStackTrace();
    }

    String result;
    try {
      result = sm.runQuery("list users;", false);
      Assert.assertTrue(result.contains("odpstest1@aliyun.com"));
    } catch (OdpsException e) {
      Assert.assertTrue(false);
    }

    try {
      sm = odps.projects().get().getSecurityManager();
      sm.runQuery("remove user ALIYUN$odpstest1@aliyun.com;", true);
    } catch (OdpsException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void policyTest() {
    SecurityManager sm = null;
    try {
      sm = odps.projects().get().getSecurityManager();
      sm.runQuery("create role testrole;", true);
    } catch (OdpsException e) {
      //e.printStackTrace();
    }
    String result;
    try {
      // test project policy
      sm.putProjectPolicy("");
      result = sm.getProjectPolicy();
      Assert.assertTrue(!result.contains("odpstest1@aliyun.com"));
      String
          policy =
          "{\"Statement\": [{\"Action\": [\"*\"],\"Effect\": \"Allow\",\"Principal\": [\"ALIYUN$odpstest1@aliyun.com\"],\"Resource\": [\"*\"]}],\"Version\": \"1\"}";
      sm.putProjectPolicy(policy);
      result = sm.getProjectPolicy();
      Assert.assertTrue(result.contains("odpstest1@aliyun.com"));

      // test role poicy
      sm.putRolePolicy("testrole", "");
      result = sm.getRolePolicy("testrole");
      Assert.assertTrue(!result.contains("odps:*"));
      policy =
          "{\"Statement\": [{\"Action\": [\"odps:*\"],\"Effect\": \"Allow\",\"Resource\": [\"*\"]}],\"Version\": \"1\"}";
      sm.putRolePolicy("testrole", policy);
      result = sm.getRolePolicy("testrole");
      Assert.assertTrue(result.contains("odps:*"));
    } catch (OdpsException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
    try {
      sm = odps.projects().get().getSecurityManager();
      sm.runQuery("drop role testrole;", true);
    } catch (OdpsException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void getAuthTokenTest() {
    try {
      SecurityManager sm = odps.projects().get().getSecurityManager();
      String
          policy =
          "{\"expires_in_hours\": 24, \"policy\" : {\"Statement\": [{\"Action\": [\"odps:*\"],\"Effect\": \"Allow\",\"Resource\": [\"*\"]}],\"Version\": \"1\"}}";
      String result = sm.generateAuthorizationToken(policy, "bearer");
      Assert.assertTrue(result.length() > policy.length());
    } catch (OdpsException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }
}
