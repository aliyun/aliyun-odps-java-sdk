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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.PackReader;
import com.aliyun.odps.tunnel.io.ReplicatorStatus;
import com.aliyun.odps.tunnel.io.StreamReader;
import com.aliyun.odps.tunnel.io.StreamWriter;
@Deprecated
public class StreamClient {

  private String projectName;
  private String tableName;

  private Configuration conf;
  private RestClient tunnelServiceClient;
  private HashMap<String, String> headers;
  private TableSchema schema = new TableSchema();
  private List<Long> shards = new ArrayList<Long>();

  final private Long MAX_WAITING_MILLISECOND = 120000L;

  /**
   * shard的状态
   */
  public static enum ShardState {
    UNLOADED,
    LOADED,
    LOADING
  }

  public StreamClient(Configuration conf, String projectName, String tableName)
      throws TunnelException {
    this.conf = conf;
    this.projectName = projectName;
    this.tableName = tableName;
    this.headers = new HashMap<String, String>();
    this.headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));
    this.headers.put(HttpHeaders.HEADER_STREAM_VERSION, "1");
    initiate();
  }

  public String getProjectName() {
    return projectName;
  }

  public String getTableName() {
    return tableName;
  }

  /**
   * 在ODPS hub服务上启用shard
   *
   * @param shardNumber
   *     需要启用的shard数量
   * @throws TunnelException
   */
  public void loadShard(long shardNumber) throws TunnelException {
    if (shardNumber < 0) {
      throw new TunnelException("invalid shard number");
    }

    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    // TODO use conf class
    String path = getResource() + "/shards";
    Connection conn = null;
    try {
      params.put(TunnelConstants.SHARD_NUMBER, Long.toString(shardNumber));
      conn = tunnelServiceClient.connect(path, "POST", params, headers);
      Response resp = conn.getResponse();

      if (!resp.isOK()) {
        TunnelException ex = new TunnelException(conn.getInputStream());
        ex.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
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
        } catch (IOException ignored) {
        }
      }
    }
  }

  @Deprecated
  public void loadShard(int shardNumber) throws TunnelException {
    loadShard(Long.valueOf(shardNumber));
  }

  /**
   * 同步等待 load shard 完成
   * 默认超时时间为 60s
   *
   * @return
   * @throws TunnelException
   */

  public void waitForShardLoad() throws TunnelException {
    waitForShardLoad(MAX_WAITING_MILLISECOND);
  }

  /**
   * 同步等待 load shard 完成
   * 最大超时时间为 60000ms
   *
   * @param timeout
   *     超时时间,单位是毫秒
   *     若该值超过 60000ms,将等待 60000ms
   * @return
   * @throws TunnelException
   */

  public void waitForShardLoad(long timeout) throws TunnelException {

    if (timeout <= 0) {
      throw new TunnelException("invalid waiting time");
    }

    long waitTime = timeout > MAX_WAITING_MILLISECOND ? MAX_WAITING_MILLISECOND : timeout;

    long now = System.currentTimeMillis();

    long end = now + waitTime;

    while (now < end) {
      try {
        if (isShardLoadCompleted()) {
          return;
        }
        Thread.sleep(10000L);
        now = System.currentTimeMillis();
      } catch (Exception e) {
        throw new TunnelException(e.getMessage(), e);
      }
    }

    if (isShardLoadCompleted() == false) {
      throw new TunnelException("load shard timeout");
    }
  }

  /**
   * 检查 StreamClinet 对应的table拥有的shard 是否全部 loaded
   *
   * @return true 为全部 load 完成, 否则返回 false
   * 注: 内部处理了 getShardStatus 可能引起的异常
   */

  private boolean isShardLoadCompleted() {
    try {
      HashMap<Long, ShardState> shardStatusMap = getShardStatus();
      Iterator iter = shardStatusMap.entrySet().iterator();

      while (iter.hasNext()) {
        Map.Entry entry = (Map.Entry) iter.next();
        ShardState status = (ShardState) entry.getValue();

        if (status != ShardState.LOADED) {
          return false;
        }
      }

      return true;
    } catch (Exception e) {

    }

    return false;
  }

  /**
   * 查询StreamClinet对应的table拥有的shard在服务端的状态
   *
   * @return 返回key为shardid, value是ShardState的HashMap
   * @throws TunnelException
   */
  public HashMap<Long, ShardState> getShardStatus() throws TunnelException, IOException {

    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> hdrs = new HashMap<String, String>(this.headers);

    try {
      String path = getResource() + "/shards";

      hdrs.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));

      params.put(TunnelConstants.SHARD_STATUS, null);

      Connection conn = tunnelServiceClient.connect(path, "GET", params, hdrs);
      Response resp = conn.getResponse();

      if (!resp.isOK()) {
        TunnelException ex = new TunnelException(conn.getInputStream());
        ex.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      } else {
        return loadShardStatusFromJson(conn.getInputStream());
      }

    } catch (Exception e) {
      throw new TunnelException(e.getMessage(), e);
    }
  }

  /**
   * 在ODPS hub查询partiton对应的拷贝到离线集群的状态
   *
   * @param shardId
   *     需要查询的shardId
   * @param partitionSpec
   *     查询的分区，分区表必选, 非分区表可以为null
   * @return ReplicatorStatus
   * @throws TunnelException
   */
  public ReplicatorStatus QueryReplicatorStatus(long shardId, PartitionSpec partitionSpec)
      throws TunnelException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    params.put("query", "replicator");
    if (partitionSpec != null && partitionSpec.toString().length() > 0) {
      params.put(TunnelConstants.RES_PARTITION, partitionSpec.toString().replaceAll("'", ""));
    }

    String path = getStreamResource(shardId);
    Connection conn = null;
    try {
      conn = tunnelServiceClient.connect(path, "GET", params, headers);
      Response resp = conn.getResponse();

      if (!resp.isOK()) {
        TunnelException ex = new TunnelException(conn.getInputStream());
        ex.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      }

      return new ReplicatorStatus(conn.getInputStream());

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
        } catch (IOException ignored) {
        }
      }
    }
  }

  /**
   * 在ODPS hub查询非分区表拷贝到离线集群的状态
   *
   * @param shardId
   *     需要查询的shardId
   * @return ReplicatorStatus
   * @throws TunnelException
   */
  public ReplicatorStatus QueryReplicatorStatus(long shardId) throws TunnelException {
    return QueryReplicatorStatus(shardId, null);
  }

  private void initiate() throws TunnelException {
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("query", "meta");
        /*for osps stream compatibility*/
    params.put("type", "stream");
    tunnelServiceClient = conf.newRestClient(projectName);
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
        } catch (IOException ignored) {
        }
      }
    }
  }

  public TableSchema getStreamSchema() {
    return this.schema;
  }

  public List<Long> getShardList() {
    return this.shards;
  }

  @Deprecated
  public StreamWriter openStreamWriter(String shardId) throws TunnelException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    return new StreamWriter(tunnelServiceClient, getStreamResource(shardId), params, headers);
  }

  public StreamWriter openStreamWriter(long shardId) throws TunnelException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    return new StreamWriter(tunnelServiceClient, getStreamResource(shardId), params, headers);
  }

  public StreamWriter openStreamWriter() throws TunnelException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));

    return new StreamWriter(tunnelServiceClient, getStreamResource(), params, headers);
  }

  public StreamReader openStreamReader(long shardId) throws TunnelException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    return new StreamReader(tunnelServiceClient, schema, getStreamResource(shardId), params,
                            headers);
  }

  public StreamReader openStreamReader(long shardId, String packId)
      throws TunnelException, IOException {
    if (packId == null) {
      throw new IllegalArgumentException("Invalid pack id.");
    }
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    return new StreamReader(tunnelServiceClient, schema, getStreamResource(shardId), params,
                            headers, packId);
  }

  public PackReader openPackReader(long shardId) throws TunnelException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    return new PackReader(tunnelServiceClient, schema, getStreamResource(shardId), params,
                          headers);
  }

  public PackReader openPackReader(long shardId, String packId)
      throws TunnelException, IOException {
    if (packId == null) {
      throw new IllegalArgumentException("Invalid pack id.");
    }
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    return new PackReader(tunnelServiceClient, schema, getStreamResource(shardId), params,
                          headers, packId);
  }

  private String getResource() {
    return conf.getResource(projectName, tableName);
  }

  private String getStreamResource(String shardId) {
    return conf.getStreamUploadResource(projectName, tableName, shardId);
  }

  private String getStreamResource(long shardId) {
    return conf.getStreamUploadResource(projectName, tableName, shardId);
  }

  private String getStreamResource() {
    return conf.getStreamUploadResource(projectName, tableName);
  }

  private void loadFromJson(InputStream is) throws TunnelException {
    try {
      String json = IOUtils.readStreamAsString(is);
      JSONObject tree = JSONObject.parseObject(json);
      JSONObject schemaNode = tree.getJSONObject("Schema");
      if (schemaNode != null) {
        schema = new TunnelTableSchema(schemaNode);
      } else {
        throw new TunnelException("get table schema fail");
      }
      JSONArray node = tree.getJSONArray("Shards");
      if (node != null) {
        for (int i = 0; i < node.size(); ++i) {
          long shardId = node.getLongValue(i);
          shards.add(shardId);
        }
      } else {
        throw new TunnelException("get shard fail");
      }
    } catch (Exception e) {
      throw new TunnelException("Invalid json content.", e);
    }
  }

  private HashMap<Long, ShardState> loadShardStatusFromJson(InputStream is) throws TunnelException {
    try {
      HashMap<Long, ShardState> shardStatus = new HashMap<Long, ShardState>();

      String json = IOUtils.readStreamAsString(is);
      JSONObject tree = JSONObject.parseObject(json);
      JSONArray node = tree.getJSONArray("ShardStatus");
      if (node != null) {
        for (int i = 0; i < node.size(); ++i) {
            JSONObject status = node.getJSONObject(i);
          ShardState state = ShardState.valueOf(status.getString("State").toUpperCase());
          shardStatus.put(Long.parseLong(status.getString("ShardId")), state);
        }
      }

      return shardStatus;
    } catch (Exception e) {
      throw new TunnelException("Invalid json content.", e);
    }
  }
}
