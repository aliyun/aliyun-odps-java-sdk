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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.ReplicatorStatus;
import com.aliyun.odps.tunnel.io.PackReader;
import com.aliyun.odps.tunnel.io.StreamReader;
import com.aliyun.odps.tunnel.io.StreamWriter;

public class StreamClient {

  private String projectName;
  private String tableName;

  private Configuration conf;
  private RestClient tunnelServiceClient;
  private HashMap<String, String> headers;
  private TableSchema schema = new TableSchema();
  private List<Long> shards = new ArrayList<Long>();


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

  /**
   * 在ODPS hub服务上启用shard
   *
   * @param shardNumber
   *     需要启用的shard数量
   * @throws TunnelException
   */
  public void loadShard(int shardNumber) throws TunnelException {
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
      params.put(TunnelConstants.SHARD_NUMBER, Integer.toString(shardNumber));
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

  /**
   * 查询StreamClinet对应的table拥有的shard在服务端的状态
   *
   * @return 返回key为shardid,value是ShardState的HashMap
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
   * @param partitionsSpec
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

  private void loadFromJson(InputStream is) throws TunnelException {
    try {
      ObjectMapper mapper = JacksonParser.getObjectMapper();
      JsonNode tree = mapper.readTree(is);
      JsonNode node = tree.get("Schema");
      if (node != null && !node.isNull()) {
        schema = new TunnelTableSchema(node);
      } else {
        throw new TunnelException("get table schema fail");
      }
      node = tree.get("Shards");
      if (node != null && !node.isNull()) {
        if (node.isArray()) {
          Iterator<JsonNode> it = node.getElements();
          while (it.hasNext()) {

            JsonNode shardId = it.next();
            shards.add(shardId.asLong());
          }
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

      ObjectMapper mapper = JacksonParser.getObjectMapper();
      JsonNode tree = mapper.readTree(is);
      JsonNode node = tree.get("ShardStatus");
      if (node != null && !node.isNull()) {
        if (node.isArray()) {
          Iterator<JsonNode> it = node.getElements();
          while (it.hasNext()) {

            JsonNode status = it.next();
            ShardState state = ShardState.valueOf(status.get("State").asText().toUpperCase());
            shardStatus.put(Long.parseLong(status.get("ShardId").asText()), state);
          }
        }
      }

      return shardStatus;
    } catch (Exception e) {
      throw new TunnelException("Invalid json content.", e);
    }
  }
}
