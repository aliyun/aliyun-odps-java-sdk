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

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.retry.RetryContext;
import com.aliyun.odps.retry.RetryHeaders;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.MaxStorageException;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.io.StreamingUpload;
import com.aliyun.odps.storage.internal.io.UploadSocketFactory;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.internal.retry.RetryHandler;
import com.aliyun.odps.storage.internal.utils.OkHttpProxyHelper;
import com.aliyun.odps.storage.internal.utils.SvnRevisionUtils;
import com.aliyun.odps.tracing.RequestTrace;
import com.aliyun.odps.utils.CredentialUtils;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final OkHttpClient uploadClient;
  private final String sharingQuotaToken;
  private final String tunnelTags;
  private final Consumer<Map<String, String>> additionalHeaderSigner;
  private final String region;
  private final String userAgent;
  private final RequestTrace requestTrace;
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
    this.requestTrace = settings.getRequestTrace();
    this.sharingQuotaToken = settings.getSharingQuotaToken();
    this.tunnelTags = settings.getTunnelTags();
    this.additionalHeaderSigner = settings.getAdditionalHeaderSigner();

    // 1. configure Connection Pool
    ConnectionPool connectionPool = new ConnectionPool(
      settings.getHttpSettings().getMaxIdleConnections(),
      settings.getHttpSettings().getKeepAliveDuration().toMillis(),
      TimeUnit.MILLISECONDS
    );

    OkHttpClient.Builder builder = new OkHttpClient.Builder();
    if (settings.getHttpSettings().getProxyConfig() != null) {
      ProxyConfig proxyConfig = settings.getHttpSettings().getProxyConfig();
      builder.proxySelector(OkHttpProxyHelper.createProxySelector(proxyConfig));
      // Only set when the proxy carries credentials: without them OkHttp keeps its default
      // Authenticator.NONE and a 407 is reported exactly as in previous SDK versions.
      okhttp3.Authenticator proxyAuthenticator =
        OkHttpProxyHelper.createProxyAuthenticator(proxyConfig);
      if (proxyAuthenticator != null) {
        builder.proxyAuthenticator(proxyAuthenticator);
      }
    }
    if (settings.getHttpSettings().isIgnoreCerts()) {
      configureInsecureTls(builder);
    }
    // 2. build OkHttpClient
    this.client =
        builder
            .connectionPool(connectionPool)
            .eventListenerFactory(UploadSocketFactory.listenerFactory())
            .connectTimeout(settings.getHttpSettings().getConnectTimeout())
            .readTimeout(settings.getHttpSettings().getReadTimeout())
            .writeTimeout(settings.getHttpSettings().getWriteTimeout())
            .build();

    // Keep bulk-upload sockets out of the control/read pool. SocketFactory is not part of
    // OkHttp's address reuse identity, so sharing the old pool could reuse unbuffered sockets.
    // HTTPS uses the original client: buffering raw TLS sockets changes handshake semantics.
    this.uploadClient =
        client
            .newBuilder()
            .connectionPool(
                new ConnectionPool(
                    settings.getHttpSettings().getMaxIdleConnections(),
                    settings.getHttpSettings().getKeepAliveDuration().toMillis(),
                    TimeUnit.MILLISECONDS))
            .socketFactory(new UploadSocketFactory())
            .protocols(java.util.Collections.singletonList(Protocol.HTTP_1_1))
            .build();

    this.endpoint = settings.getTunnelEndpoint();
    if (StringUtils.isBlank(this.endpoint)) {
      this.endpoint = getTunnelEndpoint(settings);
    }
    log.info("Use storage api endpoint: {}", this.endpoint);

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
                   null, RetryContext.create());

    HttpResponse response = requestWithoutRetry(request);
    String tunnelEndpoint = response.getBody();
    if(request.url().isHttps()) {
      tunnelEndpoint = "https://" + tunnelEndpoint;
    } else {
      tunnelEndpoint = "http://" + tunnelEndpoint;
    }
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
      return retryHandler.executeWithRetry(context ->
          requestWithoutRetry(withRetryContext(request, context)));
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
      httpResponse.setHeaders(response.headers().toMultimap());
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

    try {
      return retryHandler.executeWithRetry(context -> requestWithoutRetry(
          buildRequest(endpoint, path, method, params, headers,
                       body == null ? null : RequestBody.create(JSON, body), context)));
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
    return streamUpload(path, method, params, headers, requestBody, RetryContext.create());
  }

  HttpResponse streamUpload(String path,
                            String method,
                            Map<String, String> params,
                            Map<String, String> headers,
                            RequestBody requestBody,
                            RetryContext retryContext) {

    Request request =
        buildRequest(endpoint, path, method, params, headers, requestBody, retryContext);

    try (Response response = client.newCall(request).execute()) {
      handleErrorResponse(response);
      ResponseBody responseBody = response.body();
      HttpResponse httpResponse = new HttpResponse();
      httpResponse.setStatusCode(response.code());
      httpResponse.setRequestId(response.header(ODPS_REQUEST_ID));
      httpResponse.setBody(responseBody != null ? new String(responseBody.bytes(), StandardCharsets.UTF_8) : null);
      httpResponse.setHeaders(response.headers().toMultimap());

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
    try {
      return retryHandler.executeWithRetry(context -> {
        Request request = buildRequest(
            endpoint, path, method, params, headers,
            body == null ? null : RequestBody.create(HTML, body), context);
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
        } catch (RuntimeException e) {
          if (response != null) {
            response.close();
          }
          throw e;
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
                               RequestBody body,
                               RetryContext retryContext) {
    params = params == null ? new HashMap<>() : new HashMap<>(params);
    if (sharingQuotaToken != null) {
      params.put("sharingQuotaToken", sharingQuotaToken);
    }
    HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(endpoint))
      .newBuilder()
      .addPathSegments(path);
    params.forEach(urlBuilder::addQueryParameter);

    Request.Builder requestBuilder = new Request.Builder()
      .url(urlBuilder.build())
      .method(method, body);
    Map<String, String> requestHeaders =
        headers == null ? new HashMap<>() : new HashMap<>(headers);
    if (tunnelTags != null) {
      requestHeaders.put("odps-tunnel-tags", tunnelTags);
    }
    retryContext.injectHeaders(requestHeaders);
    requestHeaders.put(CredentialUtils.DATE, CredentialUtils.getApiTimestamp());
    // Add User-Agent header
    requestHeaders.put("User-Agent", buildUserAgent());
    requestTrace.apply(requestHeaders, null, headers);
    if (body != null) {
      requestHeaders.put("Content-Type", body.contentType().toString());
    }
    // calculate signature
    String canonicalString =
        CredentialUtils.buildCanonicalString(method, path, params, requestHeaders);

    ICredentials credentials = credentialsProvider.getCredentials();
    if (CredentialUtils.isBearerToken(credentials)) {
      // Bearer token: authenticate via the x-odps-bearer-token header, no AK/SK signature.
      requestHeaders.put(CredentialUtils.ODPS_BEARER_TOKEN, credentials.getSecurityToken());
    } else {
      String signature =
        CredentialUtils.getSignature(canonicalString, credentials.getAccessKeyId(),
                                     credentials.getAccessKeySecret());

      requestHeaders.put(CredentialUtils.AUTHORIZATION, signature);
      if (StringUtils.isNotBlank(credentials.getSecurityToken())) {
        requestHeaders.put(CredentialUtils.AUTHORIZATION_STS_TOKEN,
                              credentials.getSecurityToken());
      }
    }
    if (additionalHeaderSigner != null) {
      additionalHeaderSigner.accept(requestHeaders);
    }
    requestHeaders.forEach(requestBuilder::header);
    return requestBuilder.build();
  }

  public StreamingUpload openStreamingUpload(
      String path, Map<String, String> params, Map<String, String> headers, int chunkSize) {
    StreamingUpload upload =
        new StreamingUpload(chunkSize, client.writeTimeoutMillis());
    Request request = buildRequest(endpoint, path, "POST", params, headers, upload,
        RetryContext.create());
    OkHttpClient transport = request.url().isHttps() ? client : uploadClient;
    Call call = transport.newCall(request);
    upload.start(call, () -> {
      try (Response response = call.execute()) {
        handleErrorResponse(response);
        HttpResponse result = new HttpResponse();
        result.setStatusCode(response.code());
        result.setRequestId(response.header(ODPS_REQUEST_ID));
        result.setHeaders(response.headers().toMultimap());
        result.setBody(response.body() == null ? null : response.body().string());
        return result;
      }
    });
    return upload;
  }

  private Request withRetryContext(Request request, RetryContext retryContext) {
    // Retry headers do not use the x-odps prefix and therefore do not change the signature.
    return request.newBuilder()
        .header(RetryHeaders.TRACE_ID, retryContext.getTraceId())
        .header(RetryHeaders.RETRY_INDEX, String.valueOf(retryContext.getRetryIndex()))
        .build();
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
      // Parsing may fail as data, never as a crash. The status and the request id are already known
      // here, and when the peer answers with nothing but a status they are the whole story. They
      // used to be lost: reading fields off a payload that deserialized to null threw a
      // NullPointerException that callers could only report as 'java.lang.NullPointerException',
      // and being a bare RuntimeException it also bypassed the status-based retry policy.
      ErrorResponsePayload payload = null;
      boolean bodyIsJson = true;
      try {
        payload = gson.fromJson(errorBody, ErrorResponsePayload.class);
      } catch (RuntimeException ignored) {
        // A gateway's HTML page, plain text, a JSON array: not a payload, but not a client-side
        // failure either. Such a body is quoted verbatim below, exactly as before.
        bodyIsJson = false;
      }
      String errorCode = payload != null && StringUtils.isNotBlank(payload.Code)
                         ? payload.Code : "N/A";
      throw new ServiceException(statusCode, errorCode,
                                 errorMessageFor(statusCode, requestId, errorBody, payload,
                                                 bodyIsJson),
                                 requestId);
    }
  }

  /**
   * The most specific message available for a non-2xx response: never null, never blank.
   *
   * <p>Order of trust: the service's own {@code Message}; then the body verbatim when it says
   * something the payload model cannot represent (gateway HTML, or a document using key names
   * other than {@code Code}/{@code Message}); then a line built from the status and the request id,
   * which is what is left when the peer sent nothing but a status. When the body was not accepted
   * as JSON at all ({@code bodyIsJson} false), the raw body is the only description of the failure
   * and is therefore kept.
   */
  private static String errorMessageFor(int statusCode,
                                        String requestId,
                                        String errorBody,
                                        ErrorResponsePayload payload,
                                        boolean bodyIsJson) {
    if (payload != null && StringUtils.isNotBlank(payload.Message)) {
      return payload.Message;
    }
    if (StringUtils.isNotBlank(errorBody) && (payload != null || !bodyIsJson)) {
      return errorBody;
    }
    return "HTTP " + statusCode + " with an empty error response body (request id: " + requestId
        + ")";
  }

  private static void configureInsecureTls(OkHttpClient.Builder builder) {
    X509TrustManager trustManager = new X509TrustManager() {
      @Override
      public void checkClientTrusted(X509Certificate[] chain, String authType) {
      }

      @Override
      public void checkServerTrusted(X509Certificate[] chain, String authType) {
      }

      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
      }
    };
    try {
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, new TrustManager[]{trustManager}, null);
      builder.sslSocketFactory(context.getSocketFactory(), trustManager)
          .hostnameVerifier((hostname, session) -> true);
    } catch (GeneralSecurityException e) {
      throw new ClientException(e);
    }
  }

  public void shutdown() {
    client.dispatcher().cancelAll();
    client.dispatcher().executorService().shutdown();
    client.connectionPool().evictAll();
    uploadClient.connectionPool().evictAll();
  }

  private static class ErrorResponsePayload {
    public String Code;
    public String Message;
  }
}
