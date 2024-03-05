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

package com.aliyun.odps.rest;

import com.aliyun.odps.account.AppAccount;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLHandshakeException;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsDeprecatedLogger;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Survey;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AppStsAccount;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Request.Method;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.transport.Transport;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.commons.util.RetryExceedLimitException;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.commons.util.SvnRevisionUtils;
import com.aliyun.odps.commons.util.backoff.BackOffStrategy;
import com.aliyun.odps.commons.util.backoff.FixedBackOffStrategy;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.GsonBuilder;

/**
 * RESTful API客户端
 */
public class RestClient {

  static class RestRetryStrategy extends RetryStrategy {

    RestRetryStrategy(int limit, BackOffStrategy strategy) {
      super(limit, strategy);
    }

    @Override
    protected boolean needRetry(Exception e) {
      if (e instanceof OdpsException) {
        OdpsException err = (OdpsException) e;

        if (err.getStatus() != null && err.getStatus() / 100 == 4) {
          return false;
        }
      }

      return true;
    }
  }

  public static abstract class RetryLogger {

    /**
     * 当 RestClent 发生重试前的回调函数
     *
     * @param e
     *     错误异常
     * @param retryCount
     *     重试计数
     * @param retrySleepTime
     *     下次需要的重试时间(s)
     */
    public abstract void onRetryLog(Throwable e, long retryCount, long retrySleepTime);
  }

  /**
   * 底层网络建立超时时间,10秒
   * magic number
   * allow kernel retry 3 times.
   */
  public static final int DEFAULT_CONNECT_TIMEOUT = 10; // seconds

  /**
   * 底层网络重试次数, 3
   */
  public static final int DEFAULT_CONNECT_RETRYTIMES = 4;

  /**
   * 底层网络连接超时时间, 120秒。
   */
  public static final int DEFAULT_READ_TIMEOUT = 120;// seconds

  /**
   * 是否忽略HTTPS证书验证
   */
  public static final boolean DEFAULT_IGNORE_CERTS = false;

  /**
   * 上传数据时HTTP使用的块大小(单位字节)
   */
  public static final int DEFAULT_CHUNK_SIZE = 1500 - 4;

  private final Transport transport;

  private Account account;
  private AppAccount appAccount;
  private AppStsAccount appStsAccount;
  private String endpoint;
  private boolean ignoreCerts = DEFAULT_IGNORE_CERTS;

  private String defaultProject;
  private String currentSchema;

  private static final String
      USER_AGENT_PREFIX =
      "JavaSDK" + " Revision:" + SvnRevisionUtils.getSvnRevision()
      + " Version:" + SvnRevisionUtils.getMavenVersion() + " JavaVersion:" + SvnRevisionUtils
          .getJavaVersion();

  private String userAgent;

  public RetryLogger getRetryLogger() {
    return logger;
  }

  public void setRetryLogger(RetryLogger logger) {
    this.logger = logger;
  }

  private RetryLogger logger = null;

  /**
   * If true, send calling history of deprecated interface to ODPS.
   *
   * By default, deprecated logger is enabled
   */
  private boolean deprecatedLoggerEnabled = true;

  /**
   * 创建RestClient对象
   *
   * @param transport
   */
  @Survey
  public RestClient(Transport transport) {
    this.transport = transport;
  }

  /**
   * 请求RESTful API
   *
   * @param clazz
   *     返回结果绑定的Java类型
   * @param resource
   *     API资源标识
   * @param method
   *     访问方法
   * @return 与API返回结果绑定的clazz类型对象
   * @throws OdpsException
   */
  public <T> T request(Class<T> clazz, String resource, String method) throws OdpsException {
    return request(clazz, resource, method, null, null, null);
  }

  /**
   * 请求RESTful API
   *
   * @param clazz
   * @param resource
   * @param method
   * @param params
   * @return
   * @throws OdpsException
   */
  public <T> T request(Class<T> clazz, String resource, String method, Map<String, String> params)
      throws OdpsException {
    return request(clazz, resource, method, params, null, null);
  }

  private static final String CHARSET = "UTF-8";

  /**
   * 请求RESTful API
   *
   * @param clazz
   * @param resource
   * @param method
   * @param params
   * @param headers
   * @param body
   * @return
   * @throws OdpsException
   */
  public <T> T stringRequest(Class<T> clazz, String resource, String method,
                             Map<String, String> params,
                             Map<String, String> headers, String body) throws OdpsException {
    try {
      return request(clazz, resource, method, params, headers, body.getBytes(CHARSET));
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  /**
   * 请求RESTful API
   *
   * @param clazz
   * @param resource
   * @param method
   * @param params
   * @param headers
   * @param body
   * @return
   * @throws OdpsException
   */
  public <T> T request(Class<T> clazz, String resource, String method, Map<String, String> params,
                       Map<String, String> headers, byte[] body) throws OdpsException {

    T r = null;

    Response resp = request(resource, method, params, headers, body);
    try {
      r = SimpleXmlUtils.unmarshal(resp, clazz);
    } catch (Exception e) {
      throw new OdpsException("Can't bind xml to " + clazz.getName(), e);
    }

    return r;
  }

  /**
   * 请求RESTful API
   *
   * @param resource
   * @param method
   * @param params
   * @param headers
   * @param body
   * @return
   * @throws OdpsException
   */
  public Response stringRequest(String resource, String method, Map<String, String> params,
                                Map<String, String> headers, String body) throws OdpsException {
    try {
      return request(resource, method, params, headers, body.getBytes(CHARSET));
    } catch (UnsupportedEncodingException e) {
      throw new OdpsException(e.getMessage(), e);
    }

  }

  /**
   * 请求RESTful API
   *
   * @param resource
   * @param method
   * @param params
   * @param headers
   * @param body
   * @return
   * @throws OdpsException
   */
  public Response request(String resource, String method, Map<String, String> params,
                          Map<String, String> headers,
                          byte[] body) throws OdpsException {
    if (null == body) {
      return request(resource, method, params, headers, null, 0);
    } else {
      return request(resource, method, params, headers, new ByteArrayInputStream(body),
                     body.length);
    }
  }

  /**
   * 请求RESTful API
   *
   * @param resource
   * @param method
   * @param params
   * @param headers
   * @param body
   *     InputStream, 通常FileInputStream/ByteArrayInputStream
   * @param bodyLen
   *     InputStream的文件大小
   * @return
   * @throws OdpsException
   */
  public Response request(String resource, String method, Map<String, String> params,
                          Map<String, String> headers,
                          InputStream body, long bodyLen) throws OdpsException {
    int retryTimes = 0;
    if (method.equalsIgnoreCase(Method.GET.toString()) || method
        .equalsIgnoreCase(Method.HEAD.toString())) {
      retryTimes = getRetryTimes();
      if (body != null && body.markSupported()) {
        body.mark(0);
      }
    }

    long retryWaitTime = getConnectTimeout() + getReadTimeout();
    FixedBackOffStrategy backOffStrategy = new FixedBackOffStrategy(retryWaitTime);
    RetryStrategy retryStrategy = new RestRetryStrategy(retryTimes, backOffStrategy);

    while (true) {
      backOffStrategy.setStartTime(System.currentTimeMillis());

      try {
        Response resp = requestWithNoRetry(resource, method, params, headers, body, bodyLen);

        if (resp == null) {
          throw new OdpsException("Response is null.");
        }
        handleErrorResponse(resp);

        if (deprecatedLoggerEnabled) {
          uploadDeprecatedLog();
        }

        return resp;

      } catch (OdpsException e) {
        try {
          retryStrategy.onFailure(e, logger);
        } catch (RetryExceedLimitException ignore) {
          throw e;
        } catch (InterruptedException ignore) {
          throw e;
        }

        resetBody(body);
      }
    }
  }

  private void uploadDeprecatedLog() {
    try {
      ConcurrentHashMap<String, Long> deprecatedMaps = OdpsDeprecatedLogger.getDeprecatedCalls();
      if (deprecatedMaps.isEmpty()) {
        return;
      }
      String deprecatedLogs = new GsonBuilder().disableHtmlEscaping().create().toJson(deprecatedMaps);
      OdpsDeprecatedLogger.getDeprecatedCalls().clear();

      String project = getDefaultProject();
      if (project == null) {
        return;
      }
      String resource = ResourceBuilder.buildProjectResource(project);
      resource += "/logs";
      byte[] bytes = deprecatedLogs.getBytes(CHARSET);
      ByteArrayInputStream body = new ByteArrayInputStream(bytes);
      requestWithNoRetry(resource, "PUT", null, null, body, bytes.length);
    } catch (Throwable e) {
      //do nothing if error occured
    }
  }

  private void handleErrorResponse(Response resp) throws OdpsException {

    if (!resp.isOK()) {
      ErrorMessage error = ErrorMessage.from(resp.getBody());

      OdpsException e;
      if (resp.getStatus() == 404) {
        if (error != null) {
          e = new NoSuchObjectException(error.getMessage(), new RestException(error));
        } else {
          e = new NoSuchObjectException("No such object.");
        }
      } else {
        if (error != null) {
          e = new OdpsException(error.getMessage(), new RestException(error));
        } else {
          String errorMessage = resp.getBody() == null ? null : new String(resp.getBody());
          e = new OdpsException(errorMessage);
        }
      }

      e.setStatus(resp.getStatus());

      throw e;
    }
  }

  private void resetBody(InputStream body) {
    if (body != null && body.markSupported()) {
      try {
        body.reset();
      } catch (IOException e) {
        // DO NOTHING FOR SUPPORTED MARK STREAM WILL NOT FAILED
      }
    }
  }

  protected Response requestWithNoRetry(String resource, String method, Map<String, String> params,
                                        Map<String, String> headers, InputStream body, long bodyLen)
      throws OdpsException {

    Response resp = null;

    if (headers == null) {
      headers = new HashMap<String, String>();
    }

    try {
      // set Content-Length
      if (body != null) {
        headers.put(Headers.CONTENT_LENGTH, String.valueOf(bodyLen));
        if (!headers.containsKey(Headers.CONTENT_MD5) && (bodyLen > 0)) {
          String contentMd5 = org.apache.commons.codec.binary.Hex
              .encodeHexString(org.apache.commons.codec.digest.DigestUtils.md5(body));
          IOUtils.resetInputStream(body);
          headers.put(Headers.CONTENT_MD5, contentMd5);
        }
      } else {
        headers.put(Headers.CONTENT_LENGTH, "0");
      }

      Request req = buildRequest(resource, method, params, headers);
      req.setBody(body);
      req.setBodyLength(bodyLen);

      resp = transport.request(req);

      return resp;

    } catch (SSLHandshakeException e) {
      // FOR HTTPS CERTS CHECK FAILED
      // USE RuntimeException could avoid retry
      throw new RuntimeException(e.getMessage(), e);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (SocketTimeoutException | ConnectException e) {
      throw new OdpsException(e.getMessage()
                              + ", the possible reason is that the endpoint `" + endpoint
                              + "` is wrong, please check your endpoint",
                              e);
    } catch (SocketException e) {
      throw new OdpsException(e.getMessage()
                              + ", the possible reason is that read/write after socket closed, please check your socket",
                              e);
    } catch (IOException e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  /**
   * 获得HTTP连接
   *
   * @param resource
   * @param method
   * @param params
   * @param headers
   * @return
   * @throws OdpsException
   * @throws IOException
   */
  public Connection connect(String resource, String method, Map<String, String> params,
                            Map<String, String> headers)
      throws OdpsException, IOException {

    Request req = buildRequest(resource, method, params, headers);
    return transport.connect(req);
  }

  /**
   * 获得HTTP连接
   *
   * @param resource
   * @param method
   * @param params
   * @param headers
   * @param endpoint
   * @return
   * @throws OdpsException
   * @throws IOException
   */
  public Connection connect(String resource, String method, Map<String, String> params,
                            Map<String, String> headers, String endpoint)
          throws OdpsException, IOException {

    Request req = buildRequest(resource, method, params, headers, endpoint);
    return transport.connect(req);
  }

  /**
   * 请求RESTful API，如果返回错误码非2xx也不会抛出异常
   *
   * @param resource
   * @param method
   * @param params
   * @param headers
   * @param body
   * @param length
   * @return
   * @throws OdpsException
   * @throws IOException
   */
  @Survey
  public Response requestForRawResponse(String resource, String method, Map<String, String> params,
                                        Map<String, String> headers,
                                        InputStream body, int length)
      throws OdpsException, IOException {

    return requestWithNoRetry(resource, method, params, headers, body, length);
  }

  public void setAccount(Account account) {
    this.account = account;
  }

  public Account getAccount() {
    return account;
  }

  public void setAppAccount(AppAccount appAccount) {
    this.appAccount = appAccount;
  }

  public void setAppStsAccount(AppStsAccount appStsAccount) {
    this.appStsAccount = appStsAccount;
  }

  public AppAccount getAppAccount() {
    return appAccount;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getDefaultProject() {
    return defaultProject;
  }

  public void setDefaultProject(String defaultProject) {
    this.defaultProject = defaultProject;
  }

  public String getCurrentSchema() {
    return currentSchema;
  }

  public void setCurrentSchema(String schema) {
    this.currentSchema = schema;
  }

  public String getEndpoint() {
    return endpoint;
  }

  @Survey
  public Transport getTransport() {
    return transport;
  }

  public Request buildRequest(String resource, String method, Map<String, String> params,
                                 Map<String, String> headers) {
    return buildRequest(resource, method, params, headers, this.endpoint);
  }

  protected Request buildRequest(String resource, String method, Map<String, String> params,
                               Map<String, String> headers, String endpoint) {
    if (resource == null || !resource.startsWith("/")) {
      throw new IllegalArgumentException("Invalid resource: " + resource);
    }

    if (endpoint == null) {
      throw new IllegalArgumentException("Odps endpoint required.");
    }

    Request req = new Request(this);

    // build URL with parameters
    StringBuilder url = new StringBuilder();
    url.append(endpoint).append(resource);

    if (params == null) {
      params = new HashMap<>();
    }

    if (!params.containsKey("curr_project") && !StringUtils.isNullOrEmpty(defaultProject)) {
      params.put("curr_project", defaultProject);
    }

    if (params.size() != 0) {

      req.setParameters(params);

      url.append('?');
      boolean first = true;
      for (Entry<String, String> kv : params.entrySet()) {

        if (first) {
          first = false;
        } else {
          url.append('&');
        }

        String key = kv.getKey();
        String value = kv.getValue();
        url.append(key);
        if (value != null && value.length() > 0) {
          value = ResourceBuilder.encode(value);
          url.append('=').append(value);
        }
      }
    }

    try {

      req.setURI(new URI(url.toString()));
      req.setMethod(Method.valueOf(method));

      Map<String, String> reqHeaders =  req.getHeaders();

      if (!userDefinedHeaders.isEmpty()) {
        reqHeaders.putAll(userDefinedHeaders);
      }
      if (headers != null) {
        reqHeaders.putAll(headers);
      }

      req.setHeaders(reqHeaders);

      // set User-Agent
      if (req.getHeaders().get(Headers.USER_AGENT) == null && userAgent != null) {
        req.setHeader(Headers.USER_AGENT, userAgent);
        req.setHeader(Headers.ODPS_USER_AGENT, userAgent);
      }

      req.setHeader(Headers.DATE, DateUtils.formatRfc822Date(new Date()));

      // Sign the request
      account.getRequestSigner().sign(resource, req);
      if (appAccount != null) {
        appAccount.getRequestSigner().sign(resource, req);
      }
      if (appStsAccount != null) {
        appStsAccount.getRequestSigner().sign(resource, req);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }

    return req;
  }

  /**
   * 获取 User-Agent
   *
   * @return
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * 设置 User-Agent
   *
   * @param userAgent
   */
  public void setUserAgent(String userAgent) {
    this.userAgent = (USER_AGENT_PREFIX + " " + userAgent).trim();
  }

  int connectTimeout = DEFAULT_CONNECT_TIMEOUT;

  /**
   * 设置建立连接超时时间
   *
   * @param timeout
   *     超时时间，单位秒
   */
  public void setConnectTimeout(int timeout) {
    this.connectTimeout = timeout;
  }

  /**
   * 获取建立连接超时时间
   *
   * @return 超时时间，单位秒
   */
  public int getConnectTimeout() {
    return connectTimeout;
  }

  int readTimeout = DEFAULT_READ_TIMEOUT;

  /**
   * 设置网络超时时间
   *
   * @param timeout
   *     超时时间，单位秒
   */
  public void setReadTimeout(int timeout) {
    this.readTimeout = timeout;
  }

  /**
   * 获取建立网络超时时间
   *
   * @return 超时时间，单位秒
   */
  public int getReadTimeout() {
    return readTimeout;
  }

  int retryTimes = DEFAULT_CONNECT_RETRYTIMES;

  /**
   * 获取网络重试次数
   *
   * @return 重试次数
   */
  public int getRetryTimes() {
    return retryTimes;
  }


  /**
   * 设置网络重试次数
   *
   * @param retryTimes
   *     重试次数
   */
  public void setRetryTimes(int retryTimes) {
    this.retryTimes = retryTimes;
  }

  /**
   * 获取是否忽略 Https 验证
   *
   * @return
   */
  public boolean isIgnoreCerts() {
    return ignoreCerts;
  }

  /**
   * 设置是否忽略 Https 验证
   *
   * @param ignoreCerts
   */
  public void setIgnoreCerts(boolean ignoreCerts) {
    this.ignoreCerts = ignoreCerts;
  }

  int chunkSize = DEFAULT_CHUNK_SIZE;

  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  public int getChunkSize() {
    return chunkSize;
  }

  public void enableDeprecatedLogger() {
    this.deprecatedLoggerEnabled = true;
  }

  public void disableDeprecatedLogger() {
    this.deprecatedLoggerEnabled = false;
  }


  public Map<String, String> getUserDefinedHeaders() {
    return userDefinedHeaders;
  }

  public void addUserDefinedHeader(String key, String value) {
    this.userDefinedHeaders.put(key, value);
  }

  private Map<String, String> userDefinedHeaders = new HashMap<>();

}
