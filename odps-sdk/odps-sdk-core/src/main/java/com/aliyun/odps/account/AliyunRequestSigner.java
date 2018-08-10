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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Base64;

import com.aliyun.odps.commons.transport.Request;

/**
 * ODPS请求签名工具
 */
public class AliyunRequestSigner implements RequestSigner {

  private static final Logger log = Logger.getLogger(AliyunRequestSigner.class
                                                         .getName());

  private String accessId;
  private String accessKey;

  public AliyunRequestSigner(String accessId, String accessKey) {
    if (accessId == null || accessId.length() == 0) {
      throw new IllegalArgumentException("AccessId should not be empty.");
    }
    if (accessKey == null || accessKey.length() == 0) {
      throw new IllegalArgumentException("AccessKey should not be empty.");
    }
    this.accessId = accessId;
    this.accessKey = accessKey;
  }

  @Override
  public void sign(String resource, Request req) {
    req.getHeaders().put("Authorization", getSignature(resource, req));
  }

  public String getSignature(String resource, Request req) {
    try {
      resource = URLDecoder.decode(resource, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    String strToSign = SecurityUtils.buildCanonicalString(resource, req, "x-odps-");

    if (log.isLoggable(Level.FINE)) {
      log.fine("String to sign: " + strToSign);
    }

    byte[] crypto = new byte[0];
    try {
      crypto = SecurityUtils.hmacsha1Signature(strToSign.getBytes("UTF-8"),
                                               accessKey.getBytes());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    String signature = Base64.encodeBase64String(crypto).trim();

    return "ODPS " + accessId + ":" + signature;
  }


}
