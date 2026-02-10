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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.MaxStorageException;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.internal.retry.RetryHandler;
import com.aliyun.odps.storage.internal.utils.OkHttpProxyHelper;
import com.aliyun.odps.storage.internal.utils.SvnRevisionUtils;
import com.aliyun.odps.utils.CredentialUtils;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * HTTP client for making requests to the MaxCompute Storage API.
 *
 * <p>This class provides methods for making HTTP requests to the MaxCompute Storage API,
 * including regular requests, stream uploads, and stream downloads. It handles connection
 * pooling, timeouts, and authentication through a signature interceptor.
 *
 * <p>This is an internal class and is not intended for public use.
 */
public class HttpClient {

  private static final Logger log = LoggerFactory.getLogger(HttpClient.class);

  private String endpoint;

  private final OkHttpClient client;
  private final String region;
  private final String userAgent;
  private final ICredentialsProvider credentialsProvider;
  private final RetryHandler retryHandler;

  public static final MediaType JSON = MediaType.get("application/json");
  public static final MediaType HTML = MediaType.get("text/html");
  public static final MediaType OCTET_STREAM = MediaType.get("application/octet-stream");
  static final String ODPS_REQUEST_ID = "x-odps-request-id";

  private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

  /**
   * Builds the User-Agent string for HTTP requests.
   *
   * @return User-Agent string with format specific to this SDK
   */
  private String buildUserAgent() {
    String sdkVersion = SvnRevisionUtils.getMavenVersion();
    String javaVersion = SvnRevisionUtils.getJavaVersion();
    String revision = SvnRevisionUtils.getSvnRevision();
    String regionValue = this.region != null ? this.region : "unknown";
    String id = UUID.randomUUID().toString();

    String prefix = String.format("JavaSDK/%s (id:%s; revision:%s; java-version:%s; region:%s)",
                                  sdkVersion, id, revision, javaVersion, regionValue);
    if (userAgent == null) {
      return prefix;
    } else {
      return prefix + " " + userAgent;
    }
  }
  /**
   * Constructs a new HttpClient with the provided settings.
   *
   * @param settings The settings for configuring the HTTP client
   */
  public HttpClient(StubSettings settings) {
    this.credentialsProvider = settings.getCredentialsProvider();
    this.region = settings.getRegion(); // Save region for User-Agent
    this.userAgent = settings.getUserAgent();

    // 1. configure Connection Pool
    ConnectionPool connectionPool = new ConnectionPool(
      settings.getHttpSettings().getMaxIdleConnections(),
      settings.getHttpSettings().getKeepAliveDuration().toMillis(),
      TimeUnit.MILLISECONDS
    );

    OkHttpClient.Builder builder = new OkHttpClient.Builder();
    if (settings.getHttpSettings().getProxyConfig() != null) {
      builder.proxySelector(
        OkHttpProxyHelper.createProxySelector(settings.getHttpSettings().getProxyConfig()));
    }
    // 2. build OkHttpClient
    this.client = builder
      .connectionPool(connectionPool)
      .connectTimeout(settings.getHttpSettings().getConnectTimeout())
      .readTimeout(settings.getHttpSettings().getReadTimeout())
      .build();

    this.endpoint = settings.getTunnelEndpoint();
    if (StringUtils.isBlank(this.endpoint)) {
      this.endpoint = getTunnelEndpoint(settings);
    }
    this.retryHandler =
      settings.getRetryHandler() != null ? settings.getRetryHandler() : new RetryHandler();

    if (HttpUrl.parse(this.endpoint) == null) {
      throw new ClientException("Storage api endpoint is Illegal, current use: " + this.endpoint);
    }
  }

  private String getTunnelEndpoint(StubSettings settings) {
    Map<String, String> params = new HashMap<>();
    params.put("curr_project", settings.getProject());
    if (StringUtils.isNotBlank(settings.getQuota())) {
      params.put("quota", settings.getQuota());
    }
    Request request =
      buildRequest(settings.getEndpoint(),
                   String.format("projects/%s/tunnel", settings.getProject()), "GET", params, null,
                   null);

    HttpResponse response = requestWithoutRetry(request);
    String tunnelEndpoint = response.getBody();
    if(request.url().isHttps()) {
      tunnelEndpoint = "https://" + tunnelEndpoint;
    } else {
      tunnelEndpoint = "http://" + tunnelEndpoint;
    }
    log.info("Automatic discovery storage api endpoint: {}", tunnelEndpoint);
    return tunnelEndpoint;
  }

  /**
   * Makes a generic, one-time request (used for metadata operations).
   *
   * @param request    The request
   * @return The response body as a string
   * @throws MaxStorageException if the request fails
   */
  public HttpResponse request(Request request) {
    try {
      return retryHandler.executeWithRetry(() -> requestWithoutRetry(request));
    } catch (ServiceException e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof ClientException) {
        throw (ClientException) e;
      } else {
        throw new ClientException(e);
      }
    }
  }

  /**
   * Makes a generic, one-time request without automatic retry.
   * <p>
   * This method executes the request exactly once without any retry logic.
   * Use this when you need to handle retry logic yourself or when retry is not desired.
   *
   * @param request    The request
   * @return The response body as a string
   * @throws MaxStorageException if the request fails
   */
  public HttpResponse requestWithoutRetry(Request request) {
    try (Response response = client.newCall(request).execute()) {
      handleErrorResponse(response);
      ResponseBody responseBody = response.body();
      HttpResponse httpResponse = new HttpResponse();
      httpResponse.setStatusCode(response.code());
      httpResponse.setRequestId(response.header(ODPS_REQUEST_ID));
      httpResponse.setBody(
        responseBody != null ? new String(responseBody.bytes(), StandardCharsets.UTF_8) : null);
      return httpResponse;
    } catch (IOException e) {
      throw new ClientException(e);
    }
  }


  /**
   * Makes a generic, one-time request (used for metadata operations).
   *
   * @param path    The API path
   * @param method  The HTTP method
   * @param params  The query parameters
   * @param headers The HTTP headers
   * @param body    The request body
   * @return The response body as a string
   * @throws MaxStorageException if the request fails
   */
  public HttpResponse request(String path,
                              String method,
                              Map<String, String> params,
                              Map<String, String> headers,
                              String body) {

    Request request =
      buildRequest(endpoint, path, method, params, headers,
                   body == null ? null : RequestBody.create(JSON, body));
    return request(request);
  }

  /**
   * Uploads data using a stream.
   * <p>
   * Note: This method does not perform automatic retry. Retry logic should be implemented
   * at the StorageStub layer where the business logic and stream reset capability can be managed.
   *
   * @param path    The API path
   * @param method  The HTTP method
   * @param params  The query parameters
   * @param headers The HTTP headers
   * @return The response body as a byte array
   * @throws MaxStorageException if the request fails
   */
  public HttpResponse streamUpload(String path,
                                   String method,
                                   Map<String, String> params,
                                   Map<String, String> headers,
                                   RequestBody requestBody) {

    Request request = buildRequest(endpoint, path, method, params, headers, requestBody);

    try (Response response = client.newCall(request).execute()) {
      handleErrorResponse(response);
      ResponseBody responseBody = response.body();
      HttpResponse httpResponse = new HttpResponse();
      httpResponse.setStatusCode(response.code());
      httpResponse.setRequestId(response.header(ODPS_REQUEST_ID));
      httpResponse.setBody(responseBody != null ? new String(responseBody.bytes(), StandardCharsets.UTF_8) : null);
      return httpResponse;
    } catch (IOException e) {
      throw new ClientException(e);
    }
  }

  /**
   * Stream download request (replaces connect + getInputStream).
   *
   * @param path    The API path
   * @param method  The HTTP method
   * @param params  The query parameters
   * @param headers The HTTP headers
   * @param body    The request body
   * @return An InputStream containing the response data. The caller is responsible for closing it!
   * @throws MaxStorageException if the request fails
   */
  public HttpResponse streamDownload(String path,
                                     String method,
                                     Map<String, String> params,
                                     Map<String, String> headers,
                                     String body) {
    Request request = buildRequest(endpoint, path, method, params, headers,
                                   body == null ? null : RequestBody.create(HTML, body));
    try {
      return retryHandler.executeWithRetry(() -> {
        Response response = null;
        try {
          response = client.newCall(request).execute();
          handleErrorResponse(response);
          if (response.body() != null) {
            HttpResponse httpResponse = new HttpResponse();
            httpResponse.setStatusCode(response.code());
            httpResponse.setRequestId(response.header(ODPS_REQUEST_ID));
            httpResponse.setInputStream(response.body().byteStream());
            return httpResponse;
          } else {
            throw new ClientException("Response body is null");
          }
        } catch (IOException e) {
          if (response != null) {
            response.close();
          }
          throw new ClientException(e);
        }
      });
    } catch (ServiceException e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof ClientException) {
        throw (ClientException) e;
      } else {
        throw new ClientException(e);
      }
    }
  }

  private Request buildRequest(String endpoint,
                               String path,
                               String method,
                               Map<String, String> params,
                               Map<String, String> headers,
                               RequestBody body) {
    HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(endpoint))
      .newBuilder()
      .addPathSegments(path);
    if (params != null) {
      params.forEach(urlBuilder::addQueryParameter);
    }

    Request.Builder requestBuilder = new Request.Builder()
      .url(urlBuilder.build())
      .method(method, body);
    if (headers == null) {
      headers = new HashMap<>();
    }
    headers.put(CredentialUtils.DATE, CredentialUtils.getApiTimestamp());
    // Add User-Agent header
    headers.put("User-Agent", buildUserAgent());
    if (body != null) {
      headers.put("Content-Type", body.contentType().toString());
    }
    // calculate signature
    String canonicalString = CredentialUtils.buildCanonicalString(method, path, params, headers);
    log.debug("CanonicalString: {}", canonicalString);

    ICredentials credentials = credentialsProvider.getCredentials();
    String signature =
      CredentialUtils.getSignature(canonicalString, credentials.getAccessKeyId(),
                                   credentials.getAccessKeySecret());

    requestBuilder.header(CredentialUtils.AUTHORIZATION, signature);
    headers.forEach(requestBuilder::addHeader);
    if (StringUtils.isNotBlank(credentials.getSecurityToken())) {
      requestBuilder.header(CredentialUtils.AUTHORIZATION_STS_TOKEN, credentials.getSecurityToken());
    }
    return requestBuilder.build();
  }

  private void handleErrorResponse(Response response) {
    if (!response.isSuccessful()) {
      String errorBody = "";
      ResponseBody body = response.body();
      if (body != null) {
        try {
          errorBody = body.string();
        } catch (IOException e) {
          throw new ClientException(e);
        }
      }
      int statusCode = response.code();
      String requestId = response.header(ODPS_REQUEST_ID, "N/A");
      try {
        ErrorResponsePayload payload = gson.fromJson(errorBody, ErrorResponsePayload.class);
        throw new ServiceException(statusCode, payload.Code, payload.Message,
                                   requestId);
      } catch (JsonSyntaxException e) {
        throw new ServiceException(statusCode, "N/A", errorBody, requestId);
      }
    }
  }

  public void shutdown() {
    client.dispatcher().executorService().shutdown();
    client.connectionPool().evictAll();
  }

  private static class ErrorResponsePayload {
    public String Code;
    public String Message;
  }
}

