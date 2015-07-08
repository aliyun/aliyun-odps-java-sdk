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

package com.aliyun.odps.commons.transport;

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.rest.RestClient;

/**
 * Request表示一个HTTP请求
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class Request {

  /**
   * HTTP请求的Method
   *
   * @author shenggong.wang@alibaba-inc.com
   */
  public enum Method {
    GET, POST, PUT, DELETE, HEAD
  }

  private URI uri;
  private Method method = Method.GET;
  private Map<String, String> params = new HashMap<String, String>();
  private Map<String, String> headers = new HashMap<String, String>();
  ;
  private InputStream body;
  private long bodyLength;
  private RestClient restClient;

  public Request() {
    this(new RestClient(null));
  }

  public Request(RestClient client) {
    restClient = client;
  }

  public URI getURI() {
    return uri;
  }

  public void setURI(URI uri) {
    if (uri == null) {
      throw new IllegalArgumentException("Missing URI.");
    }
    this.uri = uri.normalize();
  }

  public Method getMethod() {
    return method;
  }

  public void setMethod(Method method) {
    this.method = method;
  }

  public Map<String, String> getParameters() {
    return params;
  }

  public void setParameter(String name, String value) {
    this.params.put(name, value);
  }

  public void setParameters(Map<String, String> params) {
    this.params = params;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeader(String name, String value) {
    this.headers.put(name, value);
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  public void setContentLength(long len) {
    this.headers.put(Headers.CONTENT_LENGTH, String.valueOf(len));
  }

  public InputStream getBody() {
    return body;
  }

  public void setBody(InputStream body) {
    this.body = body;
  }

  public long getBodyLength() {
    return bodyLength;
  }

  public void setBodyLength(long bodyLength) {
    this.bodyLength = bodyLength;
  }

  /**
   * 设置建立连接超时时间
   * 使用 {@link RestClient} 替代
   *
   * @param timeout
   *     超时时间，单位秒
   */
  @Deprecated
  public void setConnectTimeout(int timeout) {
    getRestClient().setConnectTimeout(timeout);
  }

  /**
   * 获取建立连接超时时间
   * 使用 {@link RestClient} 替代
   *
   * @return 超时时间，单位秒
   */
  @Deprecated
  public int getConnectTimeout() {
    return getRestClient().getConnectTimeout();
  }

  /**
   * 设置网络超时时间
   * 使用 {@link RestClient} 替代
   *
   * @param timeout
   *     超时时间，单位秒
   */
  @Deprecated
  public void setReadTimeout(int timeout) {
    getRestClient().setReadTimeout(timeout);
  }

  /**
   * 获取建立网络超时时间
   * 使用 {@link RestClient} 替代
   *
   * @return 超时时间，单位秒
   */
  @Deprecated
  public int getReadTimeout() {
    return getRestClient().getReadTimeout();
  }

  public RestClient getRestClient() {
    return restClient;
  }

  public void setRestClient(RestClient restClient) {
    this.restClient = restClient;
  }

}
