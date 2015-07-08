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

package com.aliyun.odps.local.common.security;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;

import sun.security.util.SecurityConstants;

public class DefaultSecurityManager extends SecurityManager {

  @Override
  public void checkExit(int status) {
    super.checkExit(status);
    try {
      checkPermission(new RuntimePermission("odpsExitVM"));
    } catch (SecurityException e) {
      throw new SecurityException("ODPS-0730001: " + e.getMessage());
    }
  }

  @Override
  public void checkAccess(ThreadGroup g) {
    super.checkAccess(g);
    if (g == null) {
      throw new NullPointerException("ODPS-0730001: thread group can't be null");
    }

    try {
      checkPermission(SecurityConstants.MODIFY_THREADGROUP_PERMISSION);
    } catch (SecurityException e) {
      throw new SecurityException("ODPS-0730001: " + e.getMessage());
    }
  }

  @Override
  public void checkAccess(Thread t) {
    super.checkAccess(t);
    if (t == null) {
      throw new NullPointerException("ODPS-0730001: thread can't be null");
    }

    try {
      checkPermission(SecurityConstants.MODIFY_THREADGROUP_PERMISSION);
    } catch (SecurityException e) {
      throw new SecurityException("ODPS-0730001: " + e.getMessage());
    }
  }

  private final String[] blockedPropertyKey = new String[] {"java.endorsed.dirs",
      "java.class.path", "java.ext.dirs", "java.library.path", "java.security.policy",
      "java.securiy.manager", "java.vm.version", "os.version", "sun.boot.class.path",
      "sun.boot.library.path"};
  private final List<String> blockedPropertyKeys = Arrays.asList(blockedPropertyKey);

  @Override
  public void checkPropertyAccess(String key) {
    super.checkPropertyAccess(key);
    if (blockedPropertyKeys.contains(key)) {
      try {
        checkPermission(new RuntimePermission("readSystemProperty"));
      } catch (SecurityException e) {
        throw new SecurityException("ODPS-0730001: " + e.getMessage());
      }
    }
  }


}
