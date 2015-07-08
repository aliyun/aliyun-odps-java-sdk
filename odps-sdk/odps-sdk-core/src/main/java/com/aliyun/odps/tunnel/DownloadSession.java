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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.DefaultTransport;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

/**
 * 此类表示一个向ODPS表中上传数据的会话
 *
 * <p>
 * 向ODPS表上传数据的流程如下: <br />
 * 1) 创建DownloadSession<br />
 * 2) 下载数据<br />
 * </p>
 *
 * 可以使用 {@link TableTunnel.DownloadSession} 替代
 */
@Deprecated
public class DownloadSession {

  /**
   * 下载会话的状态
   * <br> UNKNOWN 未知
   * <br> NORMAL 正常
   * <br> CLOSED 关闭
   * <br> EXPIRED 过期
   */
  public static enum Status {
    UNKNOWN, NORMAL, CLOSED, EXPIRED
  }

  private String id;
  private String projectName;
  private String tableName;
  private String partitionSpec;
  private long count;
  private TableSchema schema = new TableSchema();
  private Status status = Status.UNKNOWN;
  private Configuration conf;

  private RestClient tunnelServiceClient;

  /**
   * 构造一个新的下载会话
   *
   * @param projectName
   *     下载数据表所在{@link Project}名称
   * @param tableName
   *     下载{@link Table}名称
   * @param partitionSpec
   *     下载数据表的partition描述，格式如下: pt=xxx,dt=xxx
   */
  DownloadSession(Configuration conf, String projectName, String tableName,
                  String partitionSpec) throws TunnelException {
    this.conf = conf;
    this.projectName = projectName;
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;

    tunnelServiceClient = new RestClient(new DefaultTransport());
    tunnelServiceClient.setAccount(conf.getAccount());
    tunnelServiceClient.setEndpoint(conf.getEndpoint(projectName).toString());
    tunnelServiceClient.setUserAgent(conf.getUserAgent());

    tunnelServiceClient.setReadTimeout(conf.getSocketTimeout());
    tunnelServiceClient.setConnectTimeout(conf.getSocketConnectTimeout());

    initiate();
  }

  /**
   * 根据已有downloadId构造一个{@link DownloadSession}对象。
   *
   * @param projectName
   *     下载数据表所在{@link Project}名称
   * @param tableName
   *     下载{@link Table}名称
   * @param partitionSpec
   *     下载数据表的partition描述，格式如下: pt=xxx,dt=xxx
   * @param id
   *     Download的唯一标识符
   */
  DownloadSession(Configuration conf, String projectName, String tableName,
                  String partitionSpec, String downloadId) throws TunnelException {
    this.conf = conf;
    this.projectName = projectName;
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
    this.id = downloadId;

    tunnelServiceClient = new RestClient(new DefaultTransport());
    tunnelServiceClient.setAccount(conf.getAccount());
    tunnelServiceClient.setEndpoint(conf.getEndpoint(projectName).toString());
    tunnelServiceClient.setUserAgent(conf.getUserAgent());

    tunnelServiceClient.setReadTimeout(conf.getSocketTimeout());
    tunnelServiceClient.setConnectTimeout(conf.getSocketConnectTimeout());

    reload();
  }

  /**
   * 打开{@link RecordReader}用来读取记录
   *
   * @param start
   *     本次要读取记录的起始位置
   * @param count
   *     本次要读取记录的数量
   * @throws TunnelException
   * @throws IOException
   */
  public RecordReader openRecordReader(long start, long count)
      throws TunnelException, IOException {
    return openRecordReader(start, count, false);
  }

  /**
   * 打开{@link RecordReader}用来读取记录
   *
   * @param start
   *     本次要读取记录的起始位置
   * @param count
   *     本次要读取记录的数量
   * @param compress
   *     数据传输是否进行压缩；即使设置了压缩选项，如果server 不支持压缩，传输数据也不会被压缩
   * @throws TunnelException
   * @throws IOException
   */
  public RecordReader openRecordReader(long start, long count, boolean compress)
      throws TunnelException, IOException {

    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>();

    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION,
                String.valueOf(TunnelConstants.VERSION));

    if (compress) {
      if (conf.getCompressOption().algorithm
          .equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
        headers.put(Headers.ACCEPT_ENCODING, "deflate");
      } else {
        throw new TunnelException("invalid compression option.");
      }
    }

    params.put(TunnelConstants.DOWNLOADID, id);
    params.put("data", null);

    params.put(TunnelConstants.ROW_RANGE, "(" + start + "," + count + ")");

    if (partitionSpec != null && partitionSpec.length() > 0) {
      params.put(TunnelConstants.RES_PARTITION, partitionSpec);
    }

    TunnelRecordReader reader = null;
    Connection conn = null;
    try {
      conn = tunnelServiceClient.connect(getResource(), "GET", params, headers);
      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        TunnelException err = new TunnelException(conn.getInputStream());
        err.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw new IOException(err);
      }

      String content_encoding = resp.getHeader(Headers.CONTENT_ENCODING);
      if (content_encoding != null) {
        if (resp.getHeader(Headers.CONTENT_ENCODING).equals("deflate")) {
          conf.setCompressOption(new CompressOption(
              CompressOption.CompressAlgorithm.ODPS_ZLIB, -1, 0));
        } else {
          throw new TunnelException("invalid content encoding");
        }
        compress = true;
      } else {
        compress = false;
      }

      CompressOption option = compress ? conf.getCompressOption() : null;
      reader = new TunnelRecordReader(schema, conn, option);

    } catch (IOException e) {
      if (conn != null) {
        conn.disconnect();
      }
      throw new TunnelException(e.getMessage(), e);
    } catch (TunnelException e) {
      throw e;
    } catch (OdpsException e) {
      if (conn != null) {
        conn.disconnect();
      }
      throw new TunnelException(e.getMessage(), e);
    }

    return reader;
  }

  // initiate a new download session
  private void initiate() throws TunnelException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>();

    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

    params.put(TunnelConstants.DOWNLOADS, null);
    if (partitionSpec != null && partitionSpec.length() > 0) {
      params.put(TunnelConstants.RES_PARTITION, partitionSpec);
    }

    Connection conn = null;
    try {
      conn = tunnelServiceClient.connect(getResource(), "POST", params, headers);
      Response resp = conn.getResponse();

      if (resp.isOK()) {
        loadFromJson(conn.getInputStream());
      } else {
        TunnelException e = new TunnelException(conn.getInputStream());
        e.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw e;
      }
    } catch (IOException e) {
      throw new TunnelException(e.getMessage(), e);
    } catch (TunnelException e) {
      throw e;
    } catch (OdpsException e) {
      throw new TunnelException(e.getMessage(), e);
    } finally {
      if (conn != null) {
        try {
          conn.disconnect();
        } catch (IOException e) {
          // nothing
        }
      }
    }
  }

  // reload download session properties
  private void reload() throws TunnelException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>();

    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

    params.put(TunnelConstants.DOWNLOADID, id);

    if (partitionSpec != null && partitionSpec.length() > 0) {
      params.put(TunnelConstants.RES_PARTITION, partitionSpec);
    }

    Connection conn = null;
    try {
      conn = tunnelServiceClient.connect(getResource(), "GET", params, headers);
      Response resp = conn.getResponse();

      if (resp.isOK()) {
        loadFromJson(conn.getInputStream());
      } else {
        TunnelException e = new TunnelException(conn.getInputStream());
        e.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw e;
      }
    } catch (IOException e) {
      throw new TunnelException(e.getMessage(), e);
    } catch (TunnelException e) {
      throw e;
    } catch (OdpsException e) {
      throw new TunnelException(e.getMessage(), e);
    } finally {
      if (conn != null) {
        try {
          conn.disconnect();
        } catch (IOException e) {
          //
        }
      }
    }

  }

  /**
   * 获取数据对应的表结构
   *
   * @return {@link TableSchema}
   */
  public TableSchema getSchema() {
    return this.schema;
  }

  /**
   * 获取可下载的记录总数
   */
  public long getRecordCount() {
    return this.count;
  }

  /**
   * 获取会话ID
   */
  public String getId() {
    return this.id;
  }

  /**
   * 获取会话状态
   *
   * @return {@link Status}
   */
  public Status getStatus() throws TunnelException, IOException {
    reload();
    return status;
  }

  private String getResource() {
    return conf.getResource(projectName, tableName);
  }

  private void loadFromJson(InputStream is) throws TunnelException {
    try {
      ObjectMapper mapper = JacksonParser.getObjectMapper();

      JsonNode tree = mapper.readTree(is);

      // session id
      JsonNode node = tree.get("DownloadID");
      if (node != null && !node.isNull()) {
        id = node.asText();
      }

      // status
      node = tree.get("Status");
      if (node != null && !node.isNull()) {
        status = Status.valueOf(node.asText().toUpperCase());
      }

      // record count
      node = tree.get("RecordCount");
      if (node != null && !node.isNull()) {
        count = node.asLong();
      }

      // schema
      node = tree.get("Schema");
      if (node != null && !node.isNull()) {
        schema = new TunnelTableSchema(node);
      }
    } catch (Exception e) {
      throw new TunnelException("Invalid json content.", e);
    }
  }
}
