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

package com.aliyun.odps.datahub;

import com.aliyun.odps.*;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.rest.RestClient;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class DatahubClient {

  private String projectName;
  private String tableName;

  private DatahubConfiguration conf;
  private RestClient datahubServiceClient;
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

  /**
   * Datahub服务入口类
   *
   * @param odps
   *     odps对象
   * @param projectName
   *     对应project名称
   * @param tableName
   *     对应table名称
   * @param datahubEndpoint
   *     datahub服务地址，公网用户使用 http://dh.odps.aliyun.com,ecs或内网用户请使用 http://dh-ext.odps.aliyun-inc.com
   *
   * @throws OdpsException
   */
  public DatahubClient(Odps odps, String projectName, String tableName, String datahubEndpoint)
      throws OdpsException {
    this.conf = new DatahubConfiguration(odps);
    this.projectName = projectName;
    this.tableName = tableName;
    this.headers = new HashMap<String, String>();
    this.headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));
    this.headers.put(DatahubHttpHeaders.HEADER_STREAM_VERSION, "1");
    setEndpoint(datahubEndpoint);
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
   * @throws OdpsException
   */
  public void loadShard(long shardNumber) throws OdpsException {
    if (shardNumber < 0) {
      throw new DatahubException("invalid shard number");
    }

    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));
    // TODO use conf class
    String path = getResource() + "/shards";
    Connection conn = null;
    try {
      params.put(DatahubConstants.SHARD_NUMBER, Long.toString(shardNumber));
      conn = datahubServiceClient.connect(path, "POST", params, headers);
      Response resp = conn.getResponse();

      if (!resp.isOK()) {
        DatahubException ex = new DatahubException(conn.getInputStream());
        ex.setRequestId(resp.getHeader(DatahubHttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      }
    } catch (IOException e) {
      throw new DatahubException(e.getMessage(), e);
    } catch (DatahubException e) {
      throw e;
    } catch (OdpsException e) {
      throw new DatahubException(e.getMessage(), e);
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
   * 同步等待 load shard 完成
   * 默认超时时间为 120000ms
   *
   * @return
   * @throws OdpsException
   */

  public void waitForShardLoad() throws OdpsException {
    waitForShardLoad(MAX_WAITING_MILLISECOND);
  }

  /**
   * 同步等待 load shard 完成
   * 最大超时时间为 120000ms
   *
   * @param timeout
   *     超时时间,单位是毫秒
   *     若该值超过 120000ms,将等待 120000ms
   * @return
   * @throws OdpsException
   */

  public void waitForShardLoad(long timeout) throws OdpsException {

    if (timeout <= 0) {
      throw new DatahubException("invalid waiting time");
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
        throw new DatahubException(e.getMessage(), e);
      }
    }

    if (isShardLoadCompleted() == false) {
      throw new DatahubException("load shard timeout");
    }
  }

  /**
   * 检查 DatahubClinet 对应的table拥有的shard 是否全部 loaded
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
   * 查询DatahubClinet对应的table拥有的shard在服务端的状态
   *
   * @return 返回key为shardid, value是ShardState的HashMap
   * @throws OdpsException, IOException
   */
  public HashMap<Long, ShardState> getShardStatus() throws OdpsException, IOException {

    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> hdrs = new HashMap<String, String>(this.headers);

    try {
      String path = getResource() + "/shards";

      hdrs.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));

      params.put(DatahubConstants.SHARD_STATUS, null);

      Connection conn = datahubServiceClient.connect(path, "GET", params, hdrs);
      Response resp = conn.getResponse();

      if (!resp.isOK()) {
        DatahubException ex = new DatahubException(conn.getInputStream());
        ex.setRequestId(resp.getHeader(DatahubHttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      } else {
        return loadShardStatusFromJson(conn.getInputStream());
      }

    } catch (Exception e) {
      throw new DatahubException(e.getMessage(), e);
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
   * @throws OdpsException
   * @deprecated 返回的packid不一定是对应partition的, 只能通过比较大小来判断, 不要使用是否相等来判断
   */
  @Deprecated
  public ReplicatorStatus QueryReplicatorStatus(long shardId, PartitionSpec partitionSpec)
      throws OdpsException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));
    params.put("query", "replicator");
    if (partitionSpec != null && partitionSpec.toString().length() > 0) {
      params.put(DatahubConstants.RES_PARTITION, partitionSpec.toString().replaceAll("'", ""));
    }

    String path = getStreamResource(shardId);
    Connection conn = null;
    try {
      conn = datahubServiceClient.connect(path, "GET", params, headers);
      Response resp = conn.getResponse();

      if (!resp.isOK()) {
        DatahubException ex = new DatahubException(conn.getInputStream());
        ex.setRequestId(resp.getHeader(DatahubHttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      }

      return new ReplicatorStatus(conn.getInputStream());

    } catch (IOException e) {
      throw new DatahubException(e.getMessage(), e);
    } catch (DatahubException e) {
      throw e;
    } catch (OdpsException e) {
      throw new DatahubException(e.getMessage(), e);
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
   * 设置DatahubServer地址
   *
   * <p>
   * 没有设置DatahubServer地址的情况下, 自动选择
   * </p>
   *
   * @param endpoint
   */
  public void setEndpoint(String endpoint) throws OdpsException {
    try {
      URI u = new URI(endpoint);
      conf.setEndpoint(u);
      datahubServiceClient = conf.newRestClient(projectName);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid endpoint.");
    } catch (DatahubException e) {
      throw e;
    } catch (OdpsException e) {
      throw new DatahubException(e.getMessage(), e);
    }
  }

  /**
   * 在ODPS hub查询非分区表拷贝到离线集群的状态
   *
   * @param shardId
   *     需要查询的shardId
   * @return ReplicatorStatus
   * @throws OdpsException
   */
  public ReplicatorStatus QueryReplicatorStatus(long shardId) throws OdpsException {
    return QueryReplicatorStatus(shardId, null);
  }

  /**
   * 返回一个写入datahub的pack的时间戳，datahub确保小于等于这个时间戳的pack都已经进入离线ODPS
   *
   * @param
   * @return timestamp
   * @throws IOException, DatahubException
   */
  public Date getTableReplicatedTimeStamp() throws IOException, OdpsException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));
    params.put("query", "replicatedtimestamp");

    String path = getStreamResource();

    Connection conn = null;
    conn = datahubServiceClient.connect(path, "GET", params, headers);

    Response resp = conn.getResponse();
    if (!resp.isOK()) {
      DatahubException ex = new DatahubException(conn.getInputStream());
      ex.setRequestId(resp.getHeader(DatahubHttpHeaders.HEADER_ODPS_REQUEST_ID));
      throw ex;
    }

    ObjectMapper mapper = JacksonParser.getObjectMapper();
    JsonNode tree = mapper.readTree(conn.getInputStream());
    JsonNode node = null;
    node = tree.get(DatahubConstants.TABLE_REPLICATED_TIMESTAMP);
    if (node != null && !node.isNull()) {
      return new Date(node.getLongValue());
    } else {
      throw new DatahubException("get table replicated timestamp fail");
    }
  }

  /**
   * 返回一个时间戳，datahub确保小于等于这个时间戳的pack都已经进入离线ODPS
   *
   * @param
   * * @return timestamp
   * @throws IOException, DatahubException
   */
  public Date getTableTimestamp() throws IOException, OdpsException {
    HashMap<Long, ShardState> shardStatus = getShardStatus();
    final long currentTimestamp = System.currentTimeMillis();
    long timestamp = currentTimestamp;
    for (Map.Entry<Long, ShardState> entry : shardStatus.entrySet()) {
      long shardId = entry.getKey();
      PackReader reader = null;
      if (entry.getValue() == ShardState.LOADED) {
        final String loaderReplicatedPackid = QueryReplicatorStatus(shardId).GetLastReplicatedPackId();
        if (loaderReplicatedPackid.equals(PackType.FIRST_PACK_ID)) {
          continue;
        }
        reader = openPackReader(shardId);
        final String brokerLastPackid = reader.seek(currentTimestamp).getPackId();
        if (brokerLastPackid.equals(PackType.LAST_PACK_ID)) {
          continue;
        }
        if(!brokerLastPackid.equals(loaderReplicatedPackid)) {
          reader = openPackReader(shardId, loaderReplicatedPackid);
          ReadPackResult readPackResult = reader.readPackMeta();
          timestamp = Math.min(timestamp, reader.readPackMeta().getTimeStamp());
        }
      }
    }
    return new Date(timestamp);
  }

  private void initiate() throws OdpsException {
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("query", "meta");
    /*for osps stream compatibility*/
    params.put("type", "stream");
    Connection conn = null;
    try {
      datahubServiceClient = conf.newRestClient(projectName);
      conn = datahubServiceClient.connect(getResource(), "GET", params, headers);
      Response resp = conn.getResponse();

      if (resp.isOK()) {
        loadFromJson(conn.getInputStream());
      } else {
        DatahubException e = new DatahubException(conn.getInputStream());
        e.setRequestId(resp.getHeader(DatahubHttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw e;
      }
    } catch (IOException e) {
      throw new DatahubException(e.getMessage(), e);
    } catch (DatahubException e) {
      throw e;
    } catch (OdpsException e) {
      throw new DatahubException(e.getMessage(), e);
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

  public TableSchema getStreamSchemaFromServer() throws OdpsException {
    initiate();
    return this.schema;
  }

  public List<Long> getShardList() {
    return this.shards;
  }

  /**
   * 创建DatahubWriter写入指定shard
   * @param shardId
   *     需要写入数据的shardId
   *
   * @return DatahubWriter
   * @throws OdpsException, IOException
   */
  public DatahubWriter openDatahubWriter(long shardId) throws OdpsException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.CONTENT_TYPE, "application/octet-stream");
    headers.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));
    return new DatahubWriter(datahubServiceClient, getStreamResource(shardId), params, headers);
  }

  /**
   * 创建DatahubWriter
   *
   * @return DatahubWriter
   * @throws OdpsException, IOException
   */
  public DatahubWriter openDatahubWriter() throws OdpsException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.CONTENT_TYPE, "application/octet-stream");
    headers.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));

    return new DatahubWriter(datahubServiceClient, getStreamResource(), params, headers);
  }

  /**
   * 创建DatahubReader读取指定shard
   * @param shardId
   *     需要读取数据的shardId
   *
   * @return DatahubReader
   * @throws OdpsException, IOException
   */
  public DatahubReader openDatahubReader(long shardId) throws OdpsException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));
    return new DatahubReader(datahubServiceClient, schema, getStreamResource(shardId), params,
                            headers);
  }

  /**
   * 创建DatahubReader读取指定shard
   * @param shardId
   *     需要读取数据的shardId
   * @param packId
   *     指定读取的packId
   *
   * @return DatahubReader
   * @throws OdpsException, IOException
   */
  public DatahubReader openDatahubReader(long shardId, String packId)
      throws OdpsException, IOException {
    if (packId == null || packId.equals("")) {
      throw new IllegalArgumentException("Invalid pack id.");
    }
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));
    return new DatahubReader(datahubServiceClient, schema, getStreamResource(shardId), params,
                            headers, packId);
  }

  public PackReader openPackReader(long shardId) throws OdpsException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));
    return new PackReader(datahubServiceClient, schema, getStreamResource(shardId), params,
                          headers);
  }

  public PackReader openPackReader(long shardId, String packId)
      throws OdpsException, IOException {
    if (packId == null || packId.equals("")) {
      throw new IllegalArgumentException("Invalid pack id.");
    }
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(DatahubConstants.VERSION));
    return new PackReader(datahubServiceClient, schema, getStreamResource(shardId), params,
                          headers, packId);
  }

  private String getResource() {
    return conf.getResource(projectName, tableName);
  }

  private String getStreamResource(long shardId) {
    return conf.getStreamUploadResource(projectName, tableName, shardId);
  }

  private String getStreamResource() {
    return conf.getStreamUploadResource(projectName, tableName);
  }

  private void loadFromJson(InputStream is) throws OdpsException {
    try {
      ObjectMapper mapper = JacksonParser.getObjectMapper();
      JsonNode tree = mapper.readTree(is);
      JsonNode node = tree.get("Schema");
      if (node != null && !node.isNull()) {
        schema = new DatahubTableSchema(node);
      } else {
        throw new DatahubException("get table schema fail");
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
        throw new DatahubException("get shard fail");
      }
    } catch (Exception e) {
      throw new DatahubException("Invalid json content.", e);
    }
  }

  private HashMap<Long, ShardState> loadShardStatusFromJson(InputStream is) throws OdpsException {
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
      throw new DatahubException("Invalid json content.", e);
    }
  }
}
