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

/**
 * 使用临时授权 Token 的认证账号
 *
 * <p>
 * token 是 logview 提供的凭证。
 * </p>
 *
 *
 * Created by zhenhong.gzh on 16/7/22.
 */

public class BearerTokenAccount implements Account {

  private String token;

  private BearerTokenRequestSigner signer;

  /**
   * 构造 BearerTokenAccount 对象
   *
   * @param token
   *     token
   */
  public BearerTokenAccount(String token) {
    this.token = token;
    signer = new BearerTokenRequestSigner(token);
  }

  /**
   * 获取 token
   *
   * @return 当前帐号 token
   */
  public String getToken() {
    return token;
  }

  @Override
  public AccountProvider getType() {
    return AccountProvider.BEARER_TOKEN;
  }

  @Override
  public RequestSigner getRequestSigner() {
    return signer;
  }
}
