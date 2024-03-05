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

package com.aliyun.odps.sqa.commandapi;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.TestBase;

public class AuthorizationCommandTest extends TestBase {

  /**
   * 语法检查
   */
  @Test
  public void parseAuthorizationCommandTest() {
    String[] positiveCommands = new String[]{
        "add tenant user username;",
        "remove tenant user username;",
        "create tenant role rolename;",
        "drop tenant role rolename;",
        "describe tenant role rolename;",
        "grant tenant role rolename to user username;",
        "revoke tenant role rolename from user username;",
        "list tenant users;",
        "grant select on table project.table to tenant role rolename;",
        "revoke select on table project.table from tenant role rolename;",
        "show grants for tenant role rolename;",
        "show grants for tenant user username;",
        "show principals for tenant role rolename;",
        "add tenant role rolename to project projectname;",
        "remove tenant role rolename from project projectname;",
        "get policy;"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = new String[]{
        "add tenant1 user username;",
        "remove -tenant user username;",
        "create tenant rolename;",
        "drop packagee role rolename;",
        "describe tenant package role rolename;",
        "list groups users;",
        "remove tenant roles rolename from project projectname;"
    };
    Assert.assertEquals(negativeCommands.length, CommandTestUtil.getErrorNum(negativeCommands));

  }

}