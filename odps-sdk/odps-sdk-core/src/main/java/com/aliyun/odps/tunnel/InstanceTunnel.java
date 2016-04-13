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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;


public class InstanceTunnel {

  private Configuration config;

  /**
   * 构造此类对象
   *
   * @param odps
   *     {@link Odps}
   */
  public InstanceTunnel(Odps odps) {
    this.config = new Configuration(odps);
  }

  /**
   * 在 Instance 上创建下载会话
   * 特殊情况
   * 1. sql 中的 limit operator 不会生效
   * 非法情况:
   * 1. select * from 多 partition 表
   * 2. select * from 空表
   * 3. 非 SQlTask
   * 4. 非 select sql
   * 5. Task 非 Success 状态
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
   * 获得在Instance 上获取的下载会话
   *
   * @param projectName
   *     Project名
   * @param id
   *     下载会话ID {@link InstanceTunnel.DownloadSession#getId()}
   * @return {@link InstanceTunnel.DownloadSession}
   * @throws TunnelException
   */
  public InstanceTunnel.DownloadSession getDownloadSession(String projectName, String id)
      throws TunnelException {
    return new InstanceTunnel.DownloadSession(projectName, null, id);
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
    UNKNOWN, NORMAL, CLOSED, EXPIRED
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
    private TableSchema schema = new TableSchema();
    private DownloadStatus status = DownloadStatus.UNKNOWN;
    private Configuration conf;

    private RestClient tunnelServiceClient;

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
    DownloadSession(String projectName, String instanceID, String downloadId) throws TunnelException {
      this.conf = InstanceTunnel.this.config;
      this.projectName = projectName;
      this.instanceID = instanceID;
      this.id = downloadId;

      tunnelServiceClient = conf.newRestClient(projectName);

      if (id == null) {
        initiate();
      } else {
        reload();
      }
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
     * @param compress
     *     数据传输是否进行压缩；即使设置了压缩选项，如果server 不支持压缩，传输数据也不会被压缩
     * @param columns
     *     本次需要下载的列
     * @throws TunnelException
     * @throws IOException
     */
    public TunnelRecordReader openRecordReader(long start, long count, CompressOption compress,
                                               List<Column> columns) throws TunnelException, IOException {
      return new TunnelRecordReader(start, count, columns, compress, tunnelServiceClient, this);
    }

    // initiate a new download session
    private void initiate() throws TunnelException {
      HashMap<String, String> params = new HashMap<String, String>();
      HashMap<String, String> headers = new HashMap<String, String>();

      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      params.put(TunnelConstants.DOWNLOADS, null);

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

    private String getResource() {
      return InstanceTunnel.this.getResource(projectName, instanceID);
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
          status = DownloadStatus.valueOf(node.asText().toUpperCase());
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


}
