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

import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.utils.StringUtils;

public class AppStsRequestSigner implements RequestSigner {

  private Account account;
  private String stsToken;

  public AppStsRequestSigner(Account account, String stsToken) {

    if (account == null) {
      throw new IllegalArgumentException("Account cannot be null");
    }

    if (StringUtils.isNullOrEmpty(stsToken)) {
      throw new IllegalArgumentException("STS token should not be empty.");
    }

    this.account = account;
    this.stsToken = stsToken;
  }

  @Override
  public void sign(String resource, Request req) {
    Account.AccountProvider accountProvider = this.account.getType();
    switch(accountProvider) {
      case APSARA:
        // TODO: Case sensitive
        String providerStr = accountProvider.toString().toLowerCase();
        String signature = SecurityUtils.getFormattedSignature(
            providerStr,
            ((ApsaraAccount) account).getAccessId(),
            getAliyunSignature(req));
        req.getHeaders().put(Headers.STS_AUTHENTICATION, signature);
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported account provider for application account.");
    }

    req.getHeaders().put(Headers.STS_TOKEN, stsToken);
  }

  public String getAliyunSignature(Request request) {
    String strToSign = request.getHeaders().get(Headers.AUTHORIZATION);
    return getEncodedAliyunSignature(strToSign);
  }

  public String getEncodedAliyunSignature(String strToSign) {
    if (StringUtils.isNullOrEmpty(strToSign)) {
      throw new RuntimeException("String to sign cannot be empty or null.");
    }

    byte[] crypto;
    crypto = SecurityUtils.hmacsha1Signature(
        strToSign.getBytes(StandardCharsets.UTF_8),
        ((ApsaraAccount) account).getAccessKey().getBytes());

    return Base64.encodeBase64String(crypto).trim();
  }
}
