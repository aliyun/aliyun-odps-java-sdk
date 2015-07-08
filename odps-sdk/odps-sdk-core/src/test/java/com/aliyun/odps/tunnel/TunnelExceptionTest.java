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

package com.aliyun.odps.tunnel;

import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

public class TunnelExceptionTest {

  private final static String
      ERROR_MESSAGE =
      "{\"Code\":\"500\", \"Message\" : \"System Internal Error.\"}";

  @Test
  public void testCreateStream() {
    TunnelException e = new TunnelException(new ByteArrayInputStream(ERROR_MESSAGE.getBytes()));
    assertEquals(e.getErrorCode(), "500");
    assertEquals(e.getErrorMsg(), "System Internal Error.");
  }

  @Test
  public void testLocalErrorCode() {
    TunnelException e = new TunnelException("testabc");
    assertEquals(e.getErrorMsg(), "testabc");
    assertEquals(e.getErrorCode(), TunnelConstants.LOCAL_ERROR_CODE);
  }

}
