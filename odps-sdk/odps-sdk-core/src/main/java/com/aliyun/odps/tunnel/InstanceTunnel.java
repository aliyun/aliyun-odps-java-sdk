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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.impl.ConfigurationImpl;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class InstanceTunnel {

  private ConfigurationImpl config;

  /**
   * 构造此类对象
   *
   * @param odps
   *     {@link Odps}
   */
  public InstanceTunnel(Odps odps) {
    this.config = new ConfigurationImpl(odps);
  }

  /**
   * 在 Instance 上创建下载会话
   *
   * 非法情况:
   * 1. 非 SQlTask
   * 2. 非 select sql
   * 3. Task 非 Success 状态
   *
   * @param projectName
   *     Project名
   * @param instanceID
   *     Instance ID
   * @return {@link InstanceTunnel.DownloadSession}
   * @throws TunnelException
   */
  public InstanceTunnel.DownloadSession createDownloadSession(String projectName, String instanceID)
      throws TunnelException {
    return new InstanceTunnel.DownloadSession(projectName, instanceID, null);
  }

  /**
   * 在 Instance 上创建下载会话
   *
   * 非法情况:
   * 1. 非 SQlTask
   * 2. 非 select sql
   * 3. Task 非 Success 状态
   *
   * @param projectName
   *     Project名
   * @param instanceID
   *     Instance ID
   * @return {@link InstanceTunnel.DownloadSession}
   * @throws TunnelException
   */
  public InstanceTunnel.DownloadSession createDownloadSession(String projectName, String instanceID, boolean limitEnabled)
      throws TunnelException {
    return new InstanceTunnel.DownloadSession(projectName, instanceID, null, limitEnabled);
  }

  /**
   * 在 Instance 上创建下载long polling会话
   *
   * 非法情况:
   * 1. 非 SQlTask
   * 2. 非 select sql
   * 3. Task 非 Success 状态
   *
   * @param projectName
   *     Project名
   * @param instanceID
   *     Instance ID
   * @param taskName
   *     SqlRtTask taskName
   * @param queryId
   *     SqlRtTask sub queryId
   * @param limitEnabled
   *     是否启用project设置READ_TABLE_MAX_ROW, 启用后从该session最多返回10000条数据, 不启用则没有限制, 但会进行select权限校验
   * @return {@link InstanceTunnel.DownloadSession}
   * @throws TunnelException
   */
  public InstanceTunnel.DownloadSession createDirectDownloadSession(String projectName, String instanceID, String taskName, int queryId, boolean limitEnabled)
      throws TunnelException {
    if (limitEnabled) {
      return new InstanceTunnel.DownloadSession(projectName, instanceID, true, taskName, queryId);
    }
    return new InstanceTunnel.DownloadSession(projectName, instanceID, false, taskName, queryId);
  }

  /**
   * 在 Instance 上创建下载long polling会话
   * 使用该接口将默认启用project的READ_TABLE_MAX_ROW限制 最多返回10000条数据
   *
   * 非法情况:
   * 1. 非 SQlTask
   * 2. 非 select sql
   * 3. Task 非 Success 状态
   *
   * @param projectName
   *     Project名
   * @param instanceID
   *     Instance ID
   * @param taskName
   *     SqlRtTask taskName
   * @param queryId
   *     SqlRtTask sub queryId
   * @return {@link InstanceTunnel.DownloadSession}
   * @throws TunnelException
   */
  @Deprecated
  public InstanceTunnel.DownloadSession createDirectDownloadSession(String projectName, String instanceID, String taskName, int queryId)
      throws TunnelException {
    return new InstanceTunnel.DownloadSession(projectName, instanceID, true, taskName, queryId);
  }

  private String getResource(String projectName, String instanceID) {
    return ResourceBuilder.buildInstanceResource(projectName, instanceID);
  }

  /**
   * 设置TunnelServer地址
   *
   * <p>
   * 没有设置TunnelServer地址的情况下, 自动选择
   * </p>
   *
   * @param endpoint
   */
  public void setEndpoint(String endpoint) {
    try {
      URI u = new URI(endpoint);
      config.setEndpoint(u);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid endpoint.");
    }
  }

  /**
   * 下载会话的状态 <br>
   * UNKNOWN 未知 <br>
   * NORMAL 正常 <br>
   * CLOSED 关闭 <br>
   * EXPIRED 过期
   */
  public static enum DownloadStatus {
    UNKNOWN, NORMAL, CLOSED, EXPIRED, INITIATING, FAILED
  }

  /**
   * <p>
   * DownloadSession 表示从 ODPS Instance 中下载数据的会话，一般通过{@link InstanceTunnel}来创建。<br />
   * Session ID 是 Session 的唯一标识符，可通过 {@link #getId()} 获取。<br />
   *
   * <br />
   * Instance 中Record总数可通过 {@link #getRecordCount()} 得到，用户可根据 Record 总数来启动并发下载。<br />
   * <br />
   * DownloadSession 通过创建 {@link RecordReader} 来完成数据的读取,需指定读取记录的起始位置和数量<br />
   * RecordReader 对应HTTP请求的超时时间为 300S，超时后 service 端会主动关闭。<br />
   * </p>
   */
  public class DownloadSession {

    private final String instanceID;
    private String id;
    private String projectName;
    private long count;
    private boolean limitEnabled;
    private TableSchema schema = new TableSchema();
    private DownloadStatus status = DownloadStatus.UNKNOWN;
    private ConfigurationImpl conf;
    private boolean shouldTransform = false;
    private RestClient tunnelServiceClient;

    private String taskName;
    private int queryId = -1;
    private boolean isLongPolling = false;
    /**
     * 根据已有downloadId构造一个{@link DownloadSession}对象。
     *
     * @param projectName
     *     下载数据表所在project名称
     * @param instanceID
     *     下载数据 instanceID
     * @param downloadId
     *     Download的唯一标识符
     */
    public DownloadSession(String projectName, String instanceID, String downloadId) throws TunnelException {
      this(projectName, instanceID, downloadId, false);
    }

    /**
     * 根据已有downloadId构造一个{@link DownloadSession}对象。
     *
     * @param projectName
     *     下载数据表所在project名称
     * @param instanceID
     *     下载数据 instanceID
     * @param downloadId
     *     Download的唯一标识符
     * @param limitEnabled
     *     limited to 1w results
     */
    private DownloadSession(String projectName, String instanceID, String downloadId, boolean limitEnabled)
        throws TunnelException {
      this.conf = InstanceTunnel.this.config;
      this.projectName = projectName;
      this.instanceID = instanceID;
      this.id = downloadId;
      this.limitEnabled = limitEnabled;
      tunnelServiceClient = conf.newRestClient(projectName);

      if (id == null) {
        initiate();
      } else {
        reload();
      }
    }

    /**
     * 创建一个long polling模式的session。
     *
     * @param projectName
     *     下载数据表所在project名称
     * @param instanceID
     *     下载数据 instanceID
     * @param limitEnabled
     *     limited to 1w results
     * @param taskName
     *     SqlRtTask模式的task名称
     * @param queryId
     *     SqlRtTask的SubqueryId, -1表示当前currentquery
     */
    private DownloadSession(String projectName, String instanceID, boolean limitEnabled, String taskName, int queryId)
        throws TunnelException {
      this.conf = InstanceTunnel.this.config;
      this.projectName = projectName;
      this.instanceID = instanceID;
      this.limitEnabled = limitEnabled;
      this.taskName = taskName;
      this.queryId = queryId;
      this.isLongPolling = true;
      tunnelServiceClient = conf.newRestClient(projectName);
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
    public TunnelRecordReader openRecordReader(long start, long count) throws TunnelException,
                                                                              IOException {
      return openRecordReader(start, count, false);
    }

    /**
     * 打开{@link RecordReader}用来读取记录
     *
     * @param start
     *     本次要读取记录的起始位置
     * @param count
     *     本次要读取记录的数量
     * @param sizeLimit
     *     本次要读取记录的大小(Bytes)
     * 如果超过count大小会截断
     * 如果超过sizeLimit会直接抛出异常
     * @throws TunnelException
     * @throws IOException
     */
    public TunnelRecordReader openRecordReader(long start, long count, long sizeLimit) throws TunnelException,
                                                                              IOException {
      return openRecordReader(start, count, sizeLimit, false);
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
    public TunnelRecordReader openRecordReader(long start, long count, boolean compress)
        throws TunnelException, IOException {
      return openRecordReader(start, count, compress, null);
    }

    /**
     * 打开{@link RecordReader}用来读取记录
     *
     * @param start
     *     本次要读取记录的起始位置
     * @param count
     *     本次要读取记录的数量
     * @param sizeLimit
     *     本次要读取记录的大小(Bytes)
     * 如果超过count大小会截断
     * 如果超过sizeLimit会直接抛出异常
     * @param compress
     *     数据传输是否进行压缩；即使设置了压缩选项，如果server 不支持压缩，传输数据也不会被压缩
     * @throws TunnelException
     * @throws IOException
     */
    public TunnelRecordReader openRecordReader(long start, long count, long sizeLimit, boolean compress)
        throws TunnelException, IOException {
      return openRecordReader(start, count, sizeLimit, compress, null);
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
    public TunnelRecordReader openRecordReader(long start, long count, CompressOption compress)
        throws TunnelException, IOException {
      return openRecordReader(start, count, compress, null);
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
     * @param columns
     *     本次需要下载的列
     * @throws TunnelException
     * @throws IOException
     */
    public TunnelRecordReader openRecordReader(long start, long count, boolean compress,
                                               List<Column> columns) throws TunnelException, IOException {
      CompressOption option = compress ? conf.getCompressOption() :
                              new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
      return openRecordReader(start, count, option, columns);
    }


    /**
     * 打开{@link RecordReader}用来读取记录
     *
     * @param start
     *     本次要读取记录的起始位置
     * @param count
     *     本次要读取记录的数量
     * @param sizeLimit
     *     本地要读取记录的大小(Bytes)
     * 如果超过count大小会截断
     * 如果超过sizeLimit会直接抛出异常
     * @param compress
     *     数据传输是否进行压缩；即使设置了压缩选项，如果server 不支持压缩，传输数据也不会被压缩
     * @param columns
     *     本次需要下载的列
     * @throws TunnelException
     * @throws IOException
     */
    public TunnelRecordReader openRecordReader(long start, long count, long sizeLimit, boolean compress,
                                               List<Column> columns) throws TunnelException, IOException {
      CompressOption option = compress ? conf.getCompressOption() :
                              new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
      return openRecordReader(start, count, sizeLimit, option, columns);
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
     * @param columns
     *     本次需要下载的列
     * @throws TunnelException
     * @throws IOException
     */
    public TunnelRecordReader openRecordReader(long start, long count, CompressOption compress,
                                               List<Column> columns)
        throws TunnelException, IOException {

      TunnelRecordReader reader =
          new TunnelRecordReader(start, count, columns, compress, tunnelServiceClient, this);
      reader.setTransform(shouldTransform);

      return reader;
    }

    /**
     * 打开{@link RecordReader}用来读取记录
     *
     * @param start
     *     本次要读取记录的起始位置
     * @param count
     *     本次要读取记录的数量
     * @param sizeLimit
     *     本次要读取记录的大小(Bytes)
     * 如果超过count大小会截断
     * 如果超过sizeLimit会直接抛出异常
     * @param compress
     *     数据传输是否进行压缩；即使设置了压缩选项，如果server 不支持压缩，传输数据也不会被压缩
     * @param columns
     *     本次需要下载的列
     * @throws TunnelException
     * @throws IOException
     */
    public TunnelRecordReader openRecordReader(long start, long count, long sizeLimit, CompressOption compress,
                                               List<Column> columns)
        throws TunnelException, IOException {

      TunnelRecordReader reader =
          new TunnelRecordReader(start, count, sizeLimit, columns, compress, tunnelServiceClient, this);
      reader.setTransform(shouldTransform);

      return reader;
    }

    /**
     * initiate a new download session
     * @throws TunnelException
     */
    private void initiate() throws TunnelException {
      HashMap<String, String> params = new HashMap<String, String>();
      HashMap<String, String> headers = TableTunnel.getCommonHeader();

      params.put(TunnelConstants.DOWNLOADS, null);
      
      if (limitEnabled) {
        params.put(TunnelConstants.INSTANCE_TUNNEL_LIMIT_ENABLED, null);
      }

      if (taskName != null) {
        params.put(TunnelConstants.CACHED, null);
        params.put(TunnelConstants.TASK_NAME, taskName);
        if (queryId != -1) {
          params.put(TunnelConstants.QUERY_ID, String.valueOf(queryId));
        }
      }

      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(getResource(), "POST", params, headers);
        Response resp = conn.getResponse();

        if (resp.isOK()) {
          loadFromJson(conn.getInputStream());
          shouldTransform =
              StringUtils.equals(resp.getHeader(HttpHeaders.HEADER_ODPS_DATE_TRANSFORM), "true");
        } else {
          TunnelException e = new TunnelException(conn.getInputStream());
          e.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
          throw e;
        }
      } catch (IOException e) {
        throw new TunnelException("Failed to create download session with tunnel endpoint "
                                  + tunnelServiceClient.getEndpoint(), e);
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
      HashMap<String, String> headers = TableTunnel.getCommonHeader();

      params.put(TunnelConstants.DOWNLOADID, id);

      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(getResource(), "GET", params, headers);
        Response resp = conn.getResponse();

        if (resp.isOK()) {
          loadFromJson(conn.getInputStream());
          shouldTransform =
              StringUtils.equals(resp.getHeader(HttpHeaders.HEADER_ODPS_DATE_TRANSFORM), "true");
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

    public void setSchema(TableSchema schema) {
      this.schema = schema;
    }

    /**
     * 获取可下载的记录总数
     */
    public long getRecordCount() {
      return this.count;
    }

    public void setRecordCount(long count) {
      this.count = count;
    }

    /**
     * 获取 project name
     */
    public String getProjectName() {
      return this.projectName;
    }

    /**
     * 获取 instanceID
     */
    public String getInstanceID() {
      return this.instanceID;
    }

    /**
     * 获取SQL_RT_TASK_NAME
     */
    public String getTaskName() {
      return this.taskName;
    }

    /**
     * 获取SQL_RT_TASK的subqueryId
     */
    public int getQueryId() {
      return this.queryId;
    }

    public boolean getIsLongPolling() {
      return this.isLongPolling;
    }

    /**
     * 获取会话ID
     */
    public String getId() {
      return this.id;
    }
    /**
     * 获取会话状态
     */
    public DownloadStatus getStatus() throws TunnelException, IOException {
      reload();
      return status;
    }

    public boolean getEnableLimit() {
      return limitEnabled;
    }

    private String getResource() {
      return InstanceTunnel.this.getResource(projectName, instanceID);
    }

    private void loadFromJson(InputStream is) throws TunnelException {
      try {
        String json = IOUtils.readStreamAsString(is);
        JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

        // session id
        if (tree.has("DownloadID")) {
          id = tree.get("DownloadID").getAsString();
        }

        // status
        if (tree.has("Status")) {
          String downloadStatus = tree.get("Status").getAsString().toUpperCase();
          status = DownloadStatus.valueOf(downloadStatus);
        }

        // record count
        if (tree.has("RecordCount")) {
          count = tree.get("RecordCount").getAsLong();
        }

        // schema
        if (tree.has("Schema")) {
          JsonObject tunnelTableSchema = tree.get("Schema").getAsJsonObject();
          schema = new TunnelTableSchema(tunnelTableSchema);
        }
      } catch (Exception e) {
        throw new TunnelException("Invalid json content.", e);
      }
    }
  }
}
