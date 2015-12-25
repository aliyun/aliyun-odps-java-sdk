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

package com.aliyun.odps.commons.util;

import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

/**
 * Created by nizheming on 15/11/26.
 */
public class SvnRevisionUtilsTest {
  @Test
  public void testRevision() {
    System.err.println(SvnRevisionUtils.getMavenVersion());
    System.err.println(SvnRevisionUtils.getJavaVersion());
    System.err.println(SvnRevisionUtils.getSvnRevision());

    assertNotEquals(SvnRevisionUtils.getMavenVersion(), "UNKNOWN");
    assertNotEquals(SvnRevisionUtils.getJavaVersion(), "UNKNOWN");
    assertNotEquals(SvnRevisionUtils.getSvnRevision(), "UNKNOWN");
  }
}