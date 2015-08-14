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

package com.aliyun.odps.tunnel;

import java.net.URI;
import java.net.URISyntaxException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.CompressOption;

/**
 * ODPS Tunnel 配置项
 *
 * <p>
 * 用于保存与ODPS Tunnel服务通讯过程需要的配置信息。
 * </p>
 *
 * @author <a href="mailto:chao.liu@alibaba-inc.com">chao.liu</a>
 */
class Configuration {

  /**
   * 上传数据的默认块大小(单位字节)。默认值按照MTU-4设置，MTU值1500。
   */
  // MTU - chunk length - \r\n
  public static int DEFAULT_CHUNK_SIZE = 1500 - 4;

  /**
   * 底层网络链接的默认超时时间,180秒
   */
  public static int DEFAULT_SOCKET_CONNECT_TIMEOUT = 180; // seconds

  /**
   * 底层网络默认超时时间, 300秒。
   */
  public static int DEFAULT_SOCKET_TIMEOUT = 300;// seconds

  private Odps odps;
  private TunnelServerRouter router;

  private int chunkSize = DEFAULT_CHUNK_SIZE;
  private int socketConnectTimeout = DEFAULT_SOCKET_CONNECT_TIMEOUT;
  private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;

  private CompressOption option = new CompressOption();

  // TunnelServer endpoint
  private URI endpoint;

  public Configuration(Odps odps) {
    this.odps = odps;
    router = new TunnelServerRouter(this.odps.getRestClient());
  }

  public Account getAccount() {
    return odps.getAccount();
  }

  public CompressOption getCompressOption() {
    return option;
  }

  public void setCompressOption(CompressOption option) {
    this.option = option;
  }

  /**
   * 取得指定Project的Tunnel服务入口地址
   *
   * @return ODPS Tunnel服务入口地址
   * @throws TunnelException
   */
  public URI getEndpoint(String projectName) throws TunnelException {

    if (endpoint != null) {
      return endpoint;
    }

    String odpsEndpoint = odps.getEndpoint();
    URI u = null;
    try {
      u = new URI(odpsEndpoint);
    } catch (URISyntaxException e) {
      throw new TunnelException(e.getMessage(), e);
    }
    return router.getTunnelServer(projectName, u.getScheme());
  }

  /**
   * Set endpoint
   *
   * @param endpoint
   */
  public void setEndpoint(URI endpoint) {
    if (endpoint == null) {
      throw new IllegalArgumentException("Missing endpoint.");
    }
    this.endpoint = endpoint;
  }

  /**
   * 取得URI
   *
   * @param projectName
   * @param tableName
   * @return
   * @throws TunnelException
   */
  public URI getURI(String projectName, String tableName) throws TunnelException {
    StringBuilder sb = new StringBuilder();
    String e = getEndpoint(projectName).toString();
    sb.append(e);
    if (!e.endsWith("/")) {
      sb.append("/");
    }
    sb.append("projects/");
    sb.append(projectName);
    sb.append("/tables/").append(tableName);
    try {
      return new URI(sb.toString());
    } catch (URISyntaxException e1) {
      throw new RuntimeException(e1);
    }
  }

  /**
   * 获得RESTful资源标识符
   *
   * @param projectName
   * @param tableName
   * @return
   */
  public String getResource(String projectName, String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append("/projects/").append(projectName).append("/tables/")
        .append(tableName);
    return sb.toString();
  }


  /**
   * 获得Stream upload数据的RESTful资源标识符
   *
   * @param projectName
   * @param tableName
   * @return
   */
  @Deprecated
  public String getStreamUploadResource(String projectName, String tableName, String shardId) {
    StringBuilder sb = new StringBuilder();
    sb.append("/projects/").append(projectName).append("/tables/")
        .append(tableName)
        .append("/shards/")
        .append(shardId);

    return sb.toString();
  }

  /**
   * 获得Stream upload数据的RESTful资源标识符
   *
   * @param projectName
   * @param tableName
   * @param shardId
   * @return
   */
  public String getStreamUploadResource(String projectName, String tableName, long shardId) {
    StringBuilder sb = new StringBuilder();
    sb.append("/projects/").append(projectName).append("/tables/")
        .append(tableName)
        .append("/shards/")
        .append(Long.toString(shardId));

    return sb.toString();
  }

  /**
   * 获得流式上传的RESTful资源标识符
   *
   * @param projectName
   * @param tableName
   * @return
   */
  public String getStreamUploadResource(String projectName, String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append("/projects/").append(projectName).append("/tables/")
        .append(tableName)
        .append("/shards");

    return sb.toString();
  }

  /**
   * 取得当前配置的数据传输块大小
   *
   * @return 当前配置的块大小
   * @see com.com.aliyun.odps.tunnel.Configuration#DEFAULT_CHUNK_SIZE
   */
  public int getChunkSize() {
    return chunkSize;
  }

  /**
   * 设置上传数据时HTTP使用的块大小(单位字节)。
   * <p>
   * 通过<code>ProtobufRecordStreamWriter</code>上传的数据会被以chunked方式编码在HTTP通道上传
   * 输。可以通过 <code>getChunkSize()</code>查询当前配置的块大小。
   * </p>
   *
   * <p>
   * 一般情况下不建议修改默认值。
   * </p>
   *
   * @param chunkSize
   *     块大小
   * @see com.com.aliyun.odps.tunnel.Configuration#DEFAULT_CHUNK_SIZE
   * @see com.com.aliyun.odps.tunnel.Configuration#getChunkSize()
   */
  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  /**
   * 取得当前配置的网络链接超时时间, 单位秒
   *
   * @return 网络链接超时时间
   */
  public int getSocketConnectTimeout() {
    return socketConnectTimeout;
  }

  /**
   * 设置网络链接超时时间, 单位秒
   *
   * @param timeout
   *     超时时间
   */
  public void setSocketConnectTimeout(int timeout) {
    this.socketConnectTimeout = timeout;
  }

  /**
   * 取得当前配置的网络IO超时时间, 秒
   *
   * @return 超时时间
   */
  public int getSocketTimeout() {
    return socketTimeout;
  }

  /**
   * 设置网络IO超时时间, 秒
   *
   * <p>
   * <font color="red">NOTICE: </font>此值等于HTTP请求的超时时间。对应不同的API请求，SDK会使用不同的 默认值,
   * 除非必要, 请不要修改默认值。
   * </p>
   *
   * @param timeout
   *     超时时间
   * @see java.net.URLConnection#setReadTimeout(int)
   */
  public void setSocketTimeout(int timeout) {
    this.socketTimeout = timeout;
  }

  /**
   * 获取User-Agent信息
   *
   * @return User-Agent信息
   */
  public String getUserAgent() {
    return odps.getUserAgent();
  }

  RestClient newRestClient(String projectName) throws TunnelException {

    RestClient odpsServiceClient = odps.clone().getRestClient();

    odpsServiceClient.setReadTimeout(getSocketTimeout());
    odpsServiceClient.setConnectTimeout(getSocketConnectTimeout());
    odpsServiceClient.setEndpoint(getEndpoint(projectName).toString());

    return odpsServiceClient;
  }


}
