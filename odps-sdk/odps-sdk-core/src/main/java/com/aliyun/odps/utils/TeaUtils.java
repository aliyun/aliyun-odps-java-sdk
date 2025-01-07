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

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TeaUtils {

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
  static final String DATE = "Date";
  static final String PREFIX = "x-odps-";

  public static String buildCanonicalString(String method, String resource,
                                            Map<String, String> params,
                                            Map<String, String> headers) {
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
}
