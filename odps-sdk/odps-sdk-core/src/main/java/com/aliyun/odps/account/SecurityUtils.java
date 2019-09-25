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

import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;

public class SecurityUtils {

  private static final String NEW_LINE = "\n";

  protected static void init() {
    //解决多线程并发问题
  }

  protected static String buildCanonicalString(String resource, Request request, String prefix) {
    StringBuilder builder = new StringBuilder();
    builder.append(request.getMethod() + NEW_LINE);

    Map<String, String> headers = request.getHeaders();
    TreeMap<String, String> headersToSign = new TreeMap<String, String>();

    if (headers != null) {
      for (Entry<String, String> header : headers.entrySet()) {
        if (header.getKey() == null) {
          continue;
        }

        String lowerKey = header.getKey().toLowerCase();

        if (lowerKey.equals(Headers.CONTENT_TYPE.toLowerCase())
            || lowerKey.equals(Headers.CONTENT_MD5.toLowerCase())
            || lowerKey.equals(Headers.DATE.toLowerCase()) || lowerKey.startsWith(prefix)) {
          headersToSign.put(lowerKey, header.getValue());
        }
      }
    }

    if (!headersToSign.containsKey(Headers.CONTENT_TYPE.toLowerCase())) {
      headersToSign.put(Headers.CONTENT_TYPE.toLowerCase(), "");
    }
    if (!headersToSign.containsKey(Headers.CONTENT_MD5.toLowerCase())) {
      headersToSign.put(Headers.CONTENT_MD5.toLowerCase(), "");
    }

    // Add params that have the prefix "x-oss-"
    if (request.getParameters() != null) {
      for (Map.Entry<String, String> p : request.getParameters().entrySet()) {
        if (p.getKey().startsWith(prefix)) {
          headersToSign.put(p.getKey(), p.getValue());
        }
      }
    }

    // Add all headers to sign to the builder
    for (Map.Entry<String, String> entry : headersToSign.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();


      if (key.startsWith(prefix)) {

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
    builder.append(buildCanonicalizedResource(resource, request.getParameters()));

    return builder.toString();
  }

  protected static String buildCanonicalizedResource(String resource, Map<String, String> params) {
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
    String str = builder.toString();
    return str;
  }

  protected static byte[] hmacsha1Signature(byte[] data, byte[] key) {
    try {
      SecretKeySpec signingKey = new SecretKeySpec(key, "HmacSHA1");
      Mac mac = Mac.getInstance("HmacSHA1");
      mac.init(signingKey);
      return mac.doFinal(data);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  protected static PrivateKey getPrivateKey(byte[] encodedkey) throws Exception {
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encodedkey);
    try {
      KeyFactory kf = KeyFactory.getInstance("RSA");
      PrivateKey privKey = kf.generatePrivate(keySpec);
      return privKey;
    } catch (Exception e) {
      throw e;
    }

  }

  protected static byte[] signature(byte[] message, PrivateKey privateKey) throws Exception {
    try {
      Signature instance = Signature.getInstance("SHA1withRSA");
      instance.initSign(privateKey);
      instance.update(message);
      byte[] signature = instance.sign();
      return signature;
    } catch (Exception e) {
      throw e;
    }
  }

  public static String md5Signature(String message) {

    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] array = md.digest(message.getBytes());
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < array.length; ++i) {
        sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
      }
      return sb.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
    }
    return null;

  }

  public static String getApplicationSignature(
      String accountProvider, String accessId, String signedString) {
    String signature = String.format(
        "account_provider:%s,signature_method:%s,access_id:%s,signature:%s",
        accountProvider,
        "hmac-sha1",
        accessId,
        signedString);
    return signature;
  }
}


