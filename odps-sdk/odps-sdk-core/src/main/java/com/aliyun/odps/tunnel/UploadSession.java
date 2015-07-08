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
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.DefaultTransport;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;

/**
 * UploadSession表示一个向ODPS表中上传数据的会话
 *
 * <p>
 * 向ODPS表上传数据的流程如下: <br />
 * 1) 创建UploadSession<br />
 * 2) 上传Block<br />
 * 3) 提交
 * </p>
 *
 * 可以使用 {@link TableTunnel.UploadSession} 替代
 */
@Deprecated
public class UploadSession {

  /**
   * Status表示当前Upload的状态
   * <br> UNKNOWN 未知
   * <br> NORMAL 正常
   * <br> CLOSING 关闭中
   * <br> CLOSED 已关闭
   * <br> CANCELED 已取消
   * <br> EXPIRED 已过期
   * <br> CRITICAL 严重错误
   */
  public static enum Status {
    UNKNOWN, NORMAL, CLOSING, CLOSED, CANCELED, EXPIRED, CRITICAL
  }

  private String id;
  private TableSchema schema = new TableSchema();
  private String projectName;
  private String tableName;
  private String partitionSpec;
  private List<Long> blocks = new ArrayList<Long>();
  private Status status = Status.UNKNOWN;

  private Configuration conf;

  private RestClient tunnelServiceClient;

  /**
   * 构造一个新的{@link UploadSession}对象。
   *
   * @param conf
   *     DataTunnel的配置信息{@link Configuration}。
   * @param projectName
   *     上传数据表所在的{@link Project}名称。
   * @param tableName
   *     上传{@link Table}名称。
   * @param partitionSpec
   *     上传数据表的partition描述，格式如下: pt=xxx,dt=xxx。
   */
  UploadSession(Configuration conf, String projectName, String tableName,
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
   * 根据已有的uploadId构造一个{@link UploadSession}对象
   *
   * <p>
   * 在多线程或者多进程模式下，推荐各个进程或者线程共享相同的uploadId， 使用此方法创建Upload对象来进行上传，可以提高性能。
   * </p>
   *
   * @param conf
   *     DataTunnel的配置信息{@link Configuration}。
   * @param projectName
   *     上传数据表所在的{@link Project}名称。
   * @param tableName
   *     上传{@link Table}名称。
   * @param partitionSpec
   *     上传数据表的partition描述，格式如下: pt=xxx,dt=xxx。
   * @param id
   *     Upload的唯一标识符
   */
  UploadSession(Configuration conf, String projectName, String tableName,
                String partitionSpec, String uploadId) throws TunnelException {
    this.conf = conf;
    this.projectName = projectName;
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
    this.id = uploadId;

    tunnelServiceClient = new RestClient(new DefaultTransport());
    tunnelServiceClient.setAccount(conf.getAccount());
    tunnelServiceClient.setEndpoint(conf.getEndpoint(projectName).toString());
    tunnelServiceClient.setUserAgent(conf.getUserAgent());

    tunnelServiceClient.setReadTimeout(conf.getSocketTimeout());
    tunnelServiceClient.setConnectTimeout(conf.getSocketConnectTimeout());

    reload();
  }

  /* Initiate upload session */
  private void initiate() throws TunnelException {

    HashMap<String, String> params = new HashMap<String, String>();
    params.put(TunnelConstants.UPLOADS, null);

    if (this.partitionSpec != null && this.partitionSpec.length() > 0) {
      params.put(TunnelConstants.RES_PARTITION, partitionSpec);
    }

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

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

  /**
   * 打开{@link RecordWriter}用来写入数据
   *
   * <p>
   * BlockId是由用户选取的0~19999之间的数值，标识本次上传数据块
   * </p>
   *
   * @param blockId
   *     块标识
   */
  public RecordWriter openRecordWriter(long blockId) throws TunnelException,
                                                            IOException {
    return openRecordWriter(blockId, false);
  }

  /**
   * 打开{@link RecordWriter}用来写入数据
   *
   * @param blockId
   *     块标识
   * @param compress
   *     数据传输是否进行压缩
   */
  public RecordWriter openRecordWriter(long blockId, boolean compress)
      throws TunnelException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>();

    headers.put(Headers.TRANSFER_ENCODING, Headers.CHUNKED);
    headers.put(Headers.CONTENT_TYPE, "application/octet-stream");
    // req.setHeader("Expect", "100-continue");

    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION,
                String.valueOf(TunnelConstants.VERSION));

    if (compress) {

      if (conf.getCompressOption().algorithm
          .equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
        headers.put(Headers.CONTENT_ENCODING, "deflate");
      } else {
        throw new TunnelException("invalid compression option.");
      }
    }

    params.put(TunnelConstants.UPLOADID, id);
    params.put(TunnelConstants.BLOCKID, Long.toString(blockId));

    if (partitionSpec != null && partitionSpec.length() > 0) {
      params.put(TunnelConstants.RES_PARTITION, partitionSpec);
    }

    TunnelRecordWriter writer = null;
    Connection conn = null;
    try {
      conn = tunnelServiceClient.connect(getResource(), "PUT", params, headers);
      CompressOption option = compress ? conf.getCompressOption() : null;
      writer = new TunnelRecordWriter(schema, conn, option);
    } catch (IOException e) {
      if (conn != null) {
        conn.disconnect();
      }
      throw new TunnelException(e.getMessage(), e.getCause());
    } catch (TunnelException e) {
      throw e;
    } catch (OdpsException e) {
      throw new TunnelException(e.getMessage(), e);
    }

    return writer;
  }

  private void reload() throws TunnelException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>();

    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

    params.put(TunnelConstants.UPLOADID, id);

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
   * 提交本次上传的所有数据块
   *
   * <p>blcoks表示用户记录的已经成功上传的数据块列表，用来与服务器端做完整性校验</p>
   *
   * @param blocks
   *     用户预期已经上传成功的数据块列表
   * @throws TunnelException
   *     如果提供的Block列表与Server端存在的Block不一致抛出异常
   * @throws IOException
   */
  public void commit(Long[] blocks) throws TunnelException, IOException {
    if (blocks == null) {
      throw new IllegalArgumentException("Invalid argument: blocks.");
    }

    HashMap<Long, Boolean> clientBlockMap = new HashMap<Long, Boolean>();
    for (Long blockId : blocks) {
      clientBlockMap.put(blockId, true);
    }

    Long[] serverBlocks = getBlockList();
    HashMap<Long, Boolean> serverBlockMap = new HashMap<Long, Boolean>();
    for (Long blockId : serverBlocks) {
      serverBlockMap.put(blockId, true);
    }

    if (serverBlockMap.size() != clientBlockMap.size()) {
      throw new TunnelException("Blocks not match, server: "
                                + serverBlockMap.size() + ", tunnelServiceClient: " + clientBlockMap
          .size());
    }

    for (Long blockId : blocks) {
      if (!serverBlockMap.containsKey(blockId)) {
        throw new TunnelException("Block not exsits on server, block id is "
                                  + blockId);
      }
    }

    completeUpload();
  }

  private void completeUpload() throws TunnelException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>();

    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

    params.put(TunnelConstants.UPLOADID, id);

    if (partitionSpec != null && partitionSpec.length() > 0) {
      params.put(TunnelConstants.RES_PARTITION, partitionSpec);
    }

    int count = 0;
    while (true) {
      count++;
      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(getResource(), "POST", params, headers);
        Response resp = conn.getResponse();

        if (resp.isOK()) {
          loadFromJson(conn.getInputStream());
          break;
        } else {
          if (resp.getStatus() == HttpURLConnection.HTTP_INTERNAL_ERROR
              && count < 3) {
            try {
              Thread.sleep(2 * count * 1000);
            } catch (InterruptedException e) {
              throw new TunnelException(e.getMessage(), e);
            }
            continue;
          }
          throw new TunnelException(conn.getInputStream());
        }
      } catch (IOException e) {
        throw new TunnelException(e.getMessage(), e);
      } catch (TunnelException e) {
        throw e;
      } catch (OdpsException e) {
        throw new TunnelException(e.getMessage(), e);
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
    }
  }

  /**
   * 获取会话ID
   */
  public String getId() {
    return this.id;
  }

  /**
   * 获取表结构
   *
   * @return {@link TableSchema}
   */
  public TableSchema getSchema() {
    return this.schema;
  }

  /**
   * 获取会话状态
   *
   * @return {@link Status}
   */
  public Status getStatus() throws TunnelException, IOException {
    reload();
    return this.status;
  }

  /**
   * 创建临时{@link Record}对象
   *
   * <p>创建一个临时的{@link Record}对象, 用于后续上传数据</p>
   *
   * @return {@link Record}
   */
  public Record newRecord() {
    return new ArrayRecord(getSchema().getColumns().toArray(new Column[0]));
  }

  /**
   * 获取当前会话已经上传成功的数据块列表
   */
  public Long[] getBlockList() throws TunnelException, IOException {
    reload();
    return blocks.toArray(new Long[0]);
  }

  private String getResource() {
    return conf.getResource(projectName, tableName);
  }

  /*
   * Parse session properties from json stream.
   */
  private void loadFromJson(InputStream is) throws TunnelException {
    try {
      ObjectMapper mapper = JacksonParser.getObjectMapper();

      JsonNode tree = mapper.readTree(is);

      // session id
      JsonNode node = tree.get("UploadID");
      if (node != null && !node.isNull()) {
        id = node.asText();
      }

      // status
      node = tree.get("Status");
      if (node != null && !node.isNull()) {
        status = Status.valueOf(node.asText().toUpperCase());
      }

      // blocks
      blocks.clear();
      node = tree.get("UploadedBlockList");
      if (node != null && !node.isNull() && node.isArray()) {
        Iterator<JsonNode> it = node.getElements();
        while (it.hasNext()) {
          blocks.add(it.next().get("BlockID").asLong());
        }
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
