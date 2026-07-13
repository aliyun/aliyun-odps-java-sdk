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

package com.aliyun.odps.storage.internal;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.utils.CredentialUtils;
import com.aliyun.odps.utils.StringUtils;

import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.Response;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SignatureInterceptor implements Interceptor {

  static final String DATE = "DATE";
  static final String AUTHORIZATION = "Authorization";
  static final String AUTHORIZATION_STS_TOKEN = "authorization-sts-token";
  static final String ODPS_BEARER_TOKEN = "x-odps-bearer-token";

  private static final Logger log = LoggerFactory.getLogger(SignatureInterceptor.class);

  private final ICredentialsProvider credentialProvider;

  public SignatureInterceptor(ICredentialsProvider credentialProvider) {
    this.credentialProvider = credentialProvider;
  }

  @Override
  public @NotNull Response intercept(Chain chain) throws IOException {
    okhttp3.Request originalRequest = chain.request();

    String date = CredentialUtils.getApiTimestamp();
    okhttp3.Request.Builder requestBuilder = originalRequest.newBuilder();

    HttpUrl url = originalRequest.url();
    String method = originalRequest.method();
    String resource = url.encodedPath();

    Map<String, String> params = new HashMap<>();
    Set<String> paramNames = url.queryParameterNames();
    for (String paramName : paramNames) {
      params.put(paramName, url.queryParameterValues(paramName).get(0));
    }

    Map<String, String> headers = new HashMap<>();
    headers.put(DATE, date);
    Map<String, List<String>> headersMultimap = originalRequest.headers().toMultimap();
    headersMultimap.forEach((key, values) -> {
      if (!values.isEmpty()) {
        headers.put(key, values.get(0));
      } else {
        headers.put(key, "");
      }
    });

    String canonicalString = CredentialUtils.buildCanonicalString(method, resource, params, headers);
    log.debug("CanonicalString: {}", canonicalString);

    ICredentials credentials = credentialProvider.getCredentials();
    if (CredentialUtils.isBearerToken(credentials)) {
      // Bearer token: authenticate via the x-odps-bearer-token header, no AK/SK signature.
      requestBuilder.header(DATE, date).header(ODPS_BEARER_TOKEN, credentials.getSecurityToken());
    } else {
      String signature =
        CredentialUtils.getSignature(canonicalString, credentials.getAccessKeyId(),
                              credentials.getAccessKeySecret());

      requestBuilder.header(DATE, date).header(AUTHORIZATION, signature);

      if (StringUtils.isNotBlank(credentials.getSecurityToken())) {
        requestBuilder.header(AUTHORIZATION_STS_TOKEN, credentials.getSecurityToken());
      }
    }

    return chain.proceed(requestBuilder.build());
  }
}
