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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.commons.transport.Request;

/**
 * ODPS请求签名工具
 *
 * Created by zhenhong.gzh on 16/7/22.
 */
public class BearerTokenRequestSigner implements RequestSigner {

  private static final Logger LOG = LoggerFactory.getLogger(BearerTokenRequestSigner.class);

  private String token;

  public BearerTokenRequestSigner(String token) {
    if (token == null || token.length() == 0) {
      throw new IllegalArgumentException("Logview token should not be empty.");
    }

    this.token = token;
  }

  @Override
  public void sign(String resource, Request req) {
    LOG.trace("Beare token is: {}", token);

    req.getHeaders().put("x-odps-bearer-token", token);
  }
}
