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

package com.aliyun.odps.account;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;

public class StsAccountTest {
  @Test
  public void testStsToken() {
    StsAccount stsAccount =
        new StsAccount("access_id", "access_key", "sts_token");
    Request req = new Request(null);
    stsAccount.getRequestSigner().sign("resource", req);
    Assert.assertTrue(req.getHeaders().containsKey(Headers.AUTHORIZATION_STS_TOKEN));
    Assert.assertNotNull(req.getHeaders().get(Headers.AUTHORIZATION_STS_TOKEN));
    Assert.assertTrue(req.getHeaders().containsKey(Headers.AUTHORIZATION));
    Assert.assertNotNull(req.getHeaders().get(Headers.AUTHORIZATION));
  }
}
