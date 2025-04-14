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

import static com.aliyun.odps.account.SecurityUtils.hmacsha256Signature;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Base64;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.utils.StringUtils;

/**
 * ODPS请求签名工具
 */
public class ApsaraRequestSigner implements RequestSigner {

  private static final Logger log = Logger.getLogger(ApsaraRequestSigner.class.getName());
  private final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");

  private final String accessId;
  private final String accessKey;
  private String regionName;

  public ApsaraRequestSigner(String accessId, String accessKey) {
    this(accessId, accessKey, null);
  }

  public ApsaraRequestSigner(String accessId, String accessKey, String regionName) {
    if (StringUtils.isBlank(accessId)) {
      throw new IllegalArgumentException("AccessId should not be empty.");
    }
    if (StringUtils.isBlank(accessKey)) {
      throw new IllegalArgumentException("AccessKey should not be empty.");
    }
    this.accessId = accessId;
    this.accessKey = accessKey;
    this.regionName = regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  @Override
  public void sign(String resource, Request req) {
    req.getHeaders().put(Headers.AUTHORIZATION, getSignature(resource, req));
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
    if (StringUtils.isNullOrEmpty(regionName)) {
      return calculateSignatureV2(strToSign);
    } else {
      return calculateSignatureV4(strToSign, regionName);
    }
  }

  private String calculateSignatureV2(String strToSign) {
    byte[] crypto;
    crypto = SecurityUtils.hmacsha1Signature(strToSign.getBytes(StandardCharsets.UTF_8),
                                             accessKey.getBytes());

    String signature = Base64.encodeBase64String(crypto).trim();
    return "ODPS " + accessId + ":" + signature;
  }

  private String calculateSignatureV4(String strToSign, String regionName) {
    String currentDate = getDate();
    String credential = accessId + "/" + currentDate + "/" + regionName + "/odps/aliyun_v4_request";

    byte[] signatureKey = getSignatureKey(accessKey, currentDate, regionName);
    byte[]
        signature =
        SecurityUtils.hmacsha1Signature(strToSign.getBytes(StandardCharsets.UTF_8), signatureKey);
    return "ODPS " + credential + ":" + java.util.Base64.getEncoder().encodeToString(signature);
  }

  private byte[] getSignatureKey(String key, String date, String regionName) {
    byte[] kSecret = ("aliyun_v4" + key).getBytes(StandardCharsets.UTF_8);
    byte[] kDate = hmacsha256Signature(date.getBytes(StandardCharsets.UTF_8), kSecret);
    byte[] kRegion = hmacsha256Signature(regionName.getBytes(StandardCharsets.UTF_8), kDate);
    byte[] kService = hmacsha256Signature("odps".getBytes(StandardCharsets.UTF_8), kRegion);
    return hmacsha256Signature("aliyun_v4_request".getBytes(StandardCharsets.UTF_8), kService);
  }

  private String getDate() {
    LocalDateTime utcDate = LocalDateTime.now(ZoneOffset.UTC);
    return utcDate.format(dateFormat);
  }
}
