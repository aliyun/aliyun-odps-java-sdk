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

package com.aliyun.odps.utils;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

import com.aliyun.credentials.api.ICredentials;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CredentialUtils {

  /**
   * get timestamp format like 'Fri, 13 Dec 2024 02:57:00 GMT'
   *
   * @return timestamp string
   */
  public static String getApiTimestamp() {
    SimpleDateFormat rfc822DateFormat = new SimpleDateFormat(
        "EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
    rfc822DateFormat.setTimeZone(new SimpleTimeZone(0, "GMT"));
    return rfc822DateFormat.format(new Date());
  }

  static final String CONTENT_MD5 = "Content-MD5";
  static final String CONTENT_TYPE = "Content-Type";
  static final String PREFIX = "x-odps-";
  public static final String DATE = "Date";
  public static final String AUTHORIZATION = "Authorization";
  public static final String AUTHORIZATION_STS_TOKEN = "authorization-sts-token";
  public static final String ODPS_BEARER_TOKEN = "x-odps-bearer-token";

  /**
   * Returns whether the given credentials represent a bearer token.
   *
   * <p>A bearer token (e.g. obtained from {@code SecurityManager.generateAuthorizationToken}
   * with type {@code "Bearer"} and wrapped in a {@link
   * com.aliyun.odps.account.BearerTokenAccount}) is carried as the security token with no
   * access key id / secret. This distinguishes it from a normal AK/SK credential (with or
   * without an STS token), which always carries a non-blank access key id and secret.
   *
   * @param credentials the credentials to inspect
   * @return {@code true} if the credentials should be authenticated as a bearer token
   */
  public static boolean isBearerToken(ICredentials credentials) {
    return credentials != null
        && StringUtils.isBlank(credentials.getAccessKeyId())
        && StringUtils.isBlank(credentials.getAccessKeySecret())
        && StringUtils.isNotBlank(credentials.getSecurityToken());
  }

  public static String buildCanonicalString(String method, String resource,
                                            Map<String, String> params,
                                            Map<String, String> headers) {
    if (!resource.startsWith("/")) {
      resource = "/" + resource;
    }
    StringBuilder builder = new StringBuilder();
    builder.append(method + "\n");
    TreeMap<String, String> headersToSign = new TreeMap<String, String>();
    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        if (header.getKey() == null) {
          continue;
        }
        String lowerKey = header.getKey().toLowerCase();
        if (lowerKey.equals(CONTENT_MD5.toLowerCase())
            || lowerKey.equals(CONTENT_TYPE.toLowerCase())
            || lowerKey.equals(DATE.toLowerCase()) || lowerKey.startsWith(PREFIX)) {
          headersToSign.put(lowerKey, header.getValue());
        }
      }
    }
    if (!headersToSign.containsKey(CONTENT_TYPE.toLowerCase())) {
      headersToSign.put(CONTENT_TYPE.toLowerCase(), "");
    }
    if (!headersToSign.containsKey(CONTENT_MD5.toLowerCase())) {
      headersToSign.put(CONTENT_MD5.toLowerCase(), "");
    }
    // Add params that have the prefix "x-odps-"
    if (params != null) {
      for (Map.Entry<String, String> p : params.entrySet()) {
        if (p.getKey().startsWith(PREFIX)) {
          headersToSign.put(p.getKey(), p.getValue());
        }
      }
    }
    // Add all headers to sign to the builder
    for (Map.Entry<String, String> entry : headersToSign.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (key.startsWith(PREFIX)) {
        // null key will error in jdk.
        builder.append(key);
        builder.append(':');
        if (value != null) {
          builder.append(value);
        }
      } else {
        builder.append(value);
      }
      builder.append("\n");
    }
    // Add canonical resource
    builder.append(buildCanonicalResource(resource, params));
    return builder.toString();
  }

  private static String buildCanonicalResource(String resource, Map<String, String> params) {
    StringBuilder builder = new StringBuilder();
    builder.append(resource);

    if (params != null && params.size() > 0) {
      String[] names = params.keySet().toArray(new String[params.size()]);
      Arrays.sort(names);
      char separater = '?';
      for (String name : names) {

        builder.append(separater);
        builder.append(name);
        String paramValue = params.get(name);
        if (paramValue != null && paramValue.length() > 0) {
          builder.append("=").append(paramValue);
        }
        separater = '&';
      }
    }
    return builder.toString();
  }

  public static String getSignature(String strToSign, String accessKeyId, String accessKeySecret) {
    byte[] crypto;
    crypto = hmacsha1Signature(strToSign.getBytes(StandardCharsets.UTF_8),
                               accessKeySecret.getBytes());

    String signature = Base64.encodeBase64String(crypto).trim();
    return "ODPS " + accessKeyId + ":" + signature;
  }

  private static byte[] hmacsha1Signature(byte[] data, byte[] key) {
    try {
      SecretKeySpec signingKey = new SecretKeySpec(key, "HmacSHA1");
      Mac mac = Mac.getInstance("HmacSHA1");
      mac.init(signingKey);
      return mac.doFinal(data);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public static String toString(Object val) {
    if (val == null) {
      return "null";
    }
    return val.toString();
  }
}
