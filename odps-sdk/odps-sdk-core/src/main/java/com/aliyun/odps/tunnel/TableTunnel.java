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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;

/**
 * Tunnel 是 ODPS 的数据通道，用户可以通过 Tunnel 向 ODPS 中上传或者下载数据。<br />
 * TableTunnel 是访问 ODPS Tunnel 服务的入口类，仅支持表数据（非视图）的上传和下载。<br />
 * <br />
 * 对一张表或 partition 上传下载的过程，称为一个session。session 由一或多个到 Tunnel RESTful API 的 HTTP Request 组成。<br />
 * session 用 session ID 来标识，session 的超时时间是24小时，如果大批量数据传输导致超过24小时，需要自行拆分成多个 session。<br />
 * 数据的上传和下载分别由 {@link UploadSession} 和 {@link DownloadSession} 这两个会话来负责。<br />
 * TableTunnel 提供创建 UploadSession 对象和 DownloadSession 对象的方法.<br />
 * <br />
 * <ul>
 *     <li>典型表数据上传流程： <br />
 *          1) 创建 TunnelTable<br />
 *          2) 创建 UploadSession<br />
 *          3) 创建 RecordWriter,写入 Record<br />
 *          4）提交上传操作<br />
 *     <br />
 *     <li>典型表数据下载流程：<br />
 *          1) 创建 TunnelTable<br />
 *          2) 创建 DownloadSession<br />
 *          3) 创建 RecordReader,读取 Record<br />
 * </ul>
 * <p>
 * 示例代码(将一张表的数据导入到另一张表)：
 * <pre>
 * public class Sample {
    private static String accessID = "<your access id>";
    private static String accessKey = "<your access key>";
    private static String odpsURL = "<your odps endpoint>";
    private static String tunnelURL = "<your tunnel endpoint>";
    private static String project = "<your project>";
    private static String table1 = "<your table1>";
    private static String table2 = "<your table2>";

    public static void main(String args[]){
        Account account = new AliyunAccount(accessID, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(odpsURL);
        odps.setDefaultProject(project);

        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(tunnelURL);

        try {
            DownloadSession downloadSession = tunnel.createDownloadSession(project, table1);
            long count = downloadSession.getRecordCount();
            RecordReader recordReader = downloadSession.openRecordReader(0, count);
            Record record;

            UploadSession uploadSession = tunnel.createUploadSession(project, table2);
            RecordWriter recordWriter = uploadSession.openRecordWriter(0);

            while((record = recordReader.read()) != null){
                recordWriter.write(record);
            }

            recordReader.close();

            recordWriter.close();
            uploadSession.commit(new Long[]{0L});
        }catch (TunnelException e){
            e.printStackTrace();
        }catch (IOException e1) {
            e1.printStackTrace();
        }
    }
}
 * </pre>
 *
 * </p>
 *
 * @see UploadSession
 * @see DownloadSession
 */

public class TableTunnel {

  private Configuration config;

  /**
   * 构造此类对象
   *
   * @param odps
   *     {@link Odps}
   */
  public TableTunnel(Odps odps) {
    this.config = new Configuration(odps);
  }

  /**
   * 在非分区表上创建上传会话
   *
   * @param projectName
   *     Project名称
   * @param tableName
   *     表名，非视图
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession createUploadSession(String projectName, String tableName)
      throws TunnelException {
    return new TableTunnel.UploadSession(projectName, tableName, null, null);
  }

  /**
   * 在分区表上创建上传会话
   *
   * <p>
   * 注: 分区必须为最末级分区,如表有两级分区pt,ds, 则必须全部指定值, 不支持只指定其中一个值
   * </p>
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     指定分区 {@link PartitionSpec}
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession createUploadSession(String projectName, String tableName,
                                                       PartitionSpec partitionSpec)
      throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    return new TableTunnel.UploadSession(projectName, tableName, partitionSpec.toString()
        .replaceAll("'", ""), null);
  }

  /**
   * 获得在非分区表上创建的上传会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param id
   *     上传会话的ID {@link TableTunnel.UploadSession#getId()}
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession getUploadSession(String projectName, String tableName, String id)
      throws TunnelException {
    return new TableTunnel.UploadSession(projectName, tableName, null, id);
  }

  /**
   * 获得在分区表上创建的上传会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     上传数据表的partition描述 {@link PartitionSpec}
   * @param id
   *     上传会话ID {@link TableTunnel.UploadSession#getId()}
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession getUploadSession(String projectName, String tableName,
                                                    PartitionSpec partitionSpec, String id)
      throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    return new TableTunnel.UploadSession(projectName, tableName, partitionSpec.toString()
        .replaceAll("'", ""), id);
  }

  /**
   * 在非分区表上创建下载会话
   *
   * @param projectName
   *     Project名称
   * @param tableName
   *     表名，非视图
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession createDownloadSession(String projectName, String tableName)
      throws TunnelException {
    return new TableTunnel.DownloadSession(projectName, tableName, null, null, null);
  }

  /**
   * 在分区表上创建下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     指定分区 {@link PartitionSpec}
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession createDownloadSession(String projectName, String tableName,
                                                           PartitionSpec partitionSpec)
      throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    return new TableTunnel.DownloadSession(projectName, tableName, partitionSpec.toString()
        .replaceAll("'", ""), null, null);
  }

  /**
   * 在shard表上创建下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param shardId
   *     指定shardId
   * @return {@link DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession createDownloadSession(String projectName, String tableName,
                                                           long shardId) throws TunnelException {
    if (shardId < 0) {
      throw new IllegalArgumentException("Invalid arguments, shard id required.");
    }
    return new TableTunnel.DownloadSession(projectName, tableName, null, shardId, null);
  }

  /**
   * 在shard表上创建下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     指定分区 {@link PartitionSpec}
   * @param shardId
   *     指定shardIs
   * @return {@link DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession createDownloadSession(String projectName, String tableName,
                                                           PartitionSpec partitionSpec,
                                                           long shardId) throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    if (shardId < 0) {
      throw new IllegalArgumentException("Invalid arguments, shard id required.");
    }
    return new TableTunnel.DownloadSession(projectName, tableName, partitionSpec.toString()
        .replaceAll("'", ""), shardId, null);
  }

  /**
   * 获得在非分区表上创建的下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param id
   *     下载会话ID {@link TableTunnel.DownloadSession#getId()}
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession getDownloadSession(String projectName, String tableName,
                                                        String id) throws TunnelException {
    return new TableTunnel.DownloadSession(projectName, tableName, null, null, id);
  }

  /**
   * 获得在非分区表上创建的下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param id
   *     下载会话ID {@link TableTunnel.DownloadSession#getId()}
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession getDownloadSession(String projectName, String tableName,
                                                        long shardId, String id)
      throws TunnelException {
    return new TableTunnel.DownloadSession(projectName, tableName, null, shardId, id);
  }

  /**
   * 获得在分区表上创建的下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     指定分区 {@link PartitionSpec}
   * @param id
   *     下载会话ID {@link TableTunnel.DownloadSession#getId()}
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession getDownloadSession(String projectName, String tableName,
                                                        PartitionSpec partitionSpec, String id)
      throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    return new TableTunnel.DownloadSession(projectName, tableName, partitionSpec.toString()
        .replaceAll("'", ""), null, id);
  }

  /**
   * 获得在shard表上创建的下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     指定分区 {@link PartitionSpec}
   * @param shardId
   *     指定shardId
   * @param id
   *     下载会话ID {@link DownloadSession#getId()}
   * @return {@link DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession getDownloadSession(String projectName, String tableName,
                                                        PartitionSpec partitionSpec, long shardId,
                                                        String id) throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    if (shardId < 0) {
      throw new IllegalArgumentException("Invalid arguments, shard id required.");
    }
    return new TableTunnel.DownloadSession(projectName, tableName, partitionSpec.toString()
        .replaceAll("'", ""), shardId, id);
  }

  /**
   * 创建Stream上传Client
   *
   * @param projectName
   *     Project名称
   * @param tableName
   *     table名称
   * @return {@link com.aliyun.odps.tunnel.StreamClient}
   * @throws TunnelException
   */
  public StreamClient createStreamClient(String projectName, String tableName)
      throws TunnelException {
    return new StreamClient(config, projectName, tableName);
  }

  /**
   * 创建流式上传Client
   *
   * @param projectName
   *    Project名称
   * @param tableName
   *    Table 名称
   * @return {@link com.aliyun.odps.tunnel.StreamUploadClient}
   * @throws com.aliyun.odps.tunnel.TunnelException
   */
  public StreamUploadClient createStreamUploadClient(String projectName, String tableName)
    throws TunnelException {
    return new StreamUploadClient(config, projectName, tableName);
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
   * UploadStatus表示当前Upload的状态 <br>
   * UNKNOWN 未知 <br>
   * NORMAL 正常 <br>
   * CLOSING 关闭中 <br>
   * CLOSED 已关闭 <br>
   * CANCELED 已取消 <br>
   * EXPIRED 已过期 <br>
   * CRITICAL 严重错误
   */
  public static enum UploadStatus {
    UNKNOWN, NORMAL, CLOSING, CLOSED, CANCELED, EXPIRED, CRITICAL
  }

  /**
   * <p>
   * UploadSession 表示向ODPS表中上传数据的会话，一般通过{@link TableTunnel}来创建。<br />
   * 上传 Session 是 INSERT INTO 语义，即对同一张表或 partition 的多个/多次上传 Session 互不影响。<br />
   * Session ID 是Session的唯一标识符，可通过 {@link #getId()} 获取。<br />
   * <br />
   * UploadSession 通过创建 {@link RecordWriter} 来完成数据的写入操作。<br />
   * 每个 RecordWriter 对应一个 HTTP Request，单个 UploadSession 可创建多个RecordWriter。<br />
   * <br />
   * 创建 RecordWriter 时需指定 block ID，block ID是 RecordWriter 的唯一标识符,取值范围 [0, 20000)，单个block上传的数据限制是 100G。<br />
   * 同一 UploadSession 中，使用同一 block ID 多次打开 RecordWriter 会导致覆盖行为，最后一个调用 close() 的 RecordWriter 所上传的数据会被保留。同一RecordWriter实例不能重复调用 close().<br />
   * RecordWriter 对应的 HTTP Request超时为 120s，若 120s 内没有数据传输，service 端会主动关闭连接。特别提醒，HTTP协议本身有8K buffer。<br />
   * </p>
   * <br/>
   * <p>
   * 最后调用 {@link #commit(Long[])} 来提交本次上传的所有数据块。<br />
   * commit 操作可以重试，除非遇到以下异常：
   * <ul>
   *     <li>CloseUploadSessionStatusConflictException
   *     <li>DataCollisionException
   *     <li>InternalServerError
   *     <li>LOCAL_ERROR_CODE
   * </ul>
   * </p>
   * tips：
   * <ul>
   * <li>不建议对每条数据打开一个 ReadWriter，容易影响后续使用性能；
   * <li>建议用户代码缓存至少64M的数据后，再使用一个 RecordWriter 一次性批量写入。
   * </ul>
   */
  public class UploadSession {

    private String id;
    private TableSchema schema = new TableSchema();
    private String projectName;
    private String tableName;
    private String partitionSpec;
    private List<Long> blocks = new ArrayList<Long>();
    private UploadStatus status = UploadStatus.UNKNOWN;

    private Configuration conf;

    private RestClient tunnelServiceClient;

    /**
     * 根据已有的uploadId构造一个{@link UploadSession}对象
     *
     * <p>
     * 在多线程或者多进程模式下，推荐各个进程或者线程共享相同的uploadId， 使用此方法创建Upload对象来进行上传，可以提高性能。
     * </p>
     *
     * @param projectName
     *     上传数据表所在的project名称。
     * @param tableName
     *     上传数据表名称。
     * @param partitionSpec
     *     上传数据表的partition描述，格式如下: pt=xxx,dt=xxx。
     * @param uploadId
     *     Upload的唯一标识符
     */
    UploadSession(String projectName, String tableName, String partitionSpec, String uploadId)
        throws TunnelException {
      this.conf = TableTunnel.this.config;
      this.projectName = projectName;
      this.tableName = tableName;
      this.partitionSpec = partitionSpec;
      this.id = uploadId;

      tunnelServiceClient = conf.newRestClient(projectName);
      if (id == null) {
        initiate();
      } else {
        reload();
      }
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
    public RecordWriter openRecordWriter(long blockId) throws TunnelException, IOException {
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
    public RecordWriter openRecordWriter(long blockId, boolean compress) throws TunnelException,
                                                                                IOException {
      CompressOption option = compress ? conf.getCompressOption() :
                              new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
      return openRecordWriter(blockId, option);
    }

    /**
     * 打开{@link RecordWriter}用来写入数据
     *
     * @param blockId
     *     块标识
     * @param compress
     *     数据传输是否进行压缩
     */
    public RecordWriter openRecordWriter(long blockId, CompressOption compress)
        throws TunnelException,
               IOException {
      HashMap<String, String> params = new HashMap<String, String>();
      HashMap<String, String> headers = new HashMap<String, String>();

      headers.put(Headers.TRANSFER_ENCODING, Headers.CHUNKED);
      headers.put(Headers.CONTENT_TYPE, "application/octet-stream");
      // req.setHeader("Expect", "100-continue");

      headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));

      switch (compress.algorithm) {
        case ODPS_RAW: {
          break;
        }
        case ODPS_ZLIB: {
          headers.put(Headers.CONTENT_ENCODING, "deflate");
          break;
        }
        case ODPS_SNAPPY: {
          headers.put(Headers.CONTENT_ENCODING, "x-snappy-framed");
          break;
        }
        default: {
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

        writer = new TunnelRecordWriter(schema, conn, compress);

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
     * <p>
     * blcoks表示用户记录的已经成功上传的数据块列表，用来与服务器端做完整性校验
     * </p>
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
        throw new TunnelException("Blocks not match, server: " + serverBlockMap.size()
                                  + ", tunnelServiceClient: " + clientBlockMap.size());
      }

      for (Long blockId : blocks) {
        if (!serverBlockMap.containsKey(blockId)) {
          throw new TunnelException("Block not exsits on server, block id is " + blockId);
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
            if (resp.getStatus() == HttpURLConnection.HTTP_INTERNAL_ERROR && count < 3) {
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
     */
    public TableSchema getSchema() {
      return this.schema;
    }

    /**
     * 获取会话状态
     */
    public UploadStatus getStatus() throws TunnelException, IOException {
      reload();
      return this.status;
    }

    /**
     * 创建临时{@link Record}对象
     *
     * <p>
     * 创建一个临时的{@link Record}对象, 用于后续上传数据
     * </p>
     *
     * @return
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
          status = UploadStatus.valueOf(node.asText().toUpperCase());
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
   * DownloadSession 表示从 ODPS 表中下载数据的会话，一般通过{@link TableTunnel}来创建。<br />
   * Session ID 是 Session 的唯一标识符，可通过 {@link #getId()} 获取。<br />
   *
   * <br />
   * 表中Record总数可通过 {@link #getRecordCount()} 得到，用户可根据 Record 总数来启动并发下载。<br />
   * <br />
   * DownloadSession 通过创建 {@link RecordReader} 来完成数据的读取,需指定读取记录的起始位置和数量<br />
   * RecordReader 对应HTTP请求的超时时间为 300S，超时后 service 端会主动关闭。<br />
   *</p>
   */
  public class DownloadSession {

    private String id;
    private String projectName;
    private String tableName;
    private String partitionSpec;
    private Long shardId;
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
     * @param tableName
     *     下载数据表名称
     * @param partitionSpec
     *     下载数据表的partition描述，格式如下: pt=xxx,dt=xxx
     * @param shardId
     *     下载数据表的shard标识
     * @param downloadId
     *     Download的唯一标识符
     */
    DownloadSession(String projectName, String tableName, String partitionSpec, Long shardId,
                    String downloadId) throws TunnelException {
      this.conf = TableTunnel.this.config;
      this.projectName = projectName;
      this.tableName = tableName;
      this.partitionSpec = partitionSpec;
      this.shardId = shardId;
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
      HashMap<String, String> params = new HashMap<String, String>();
      HashMap<String, String> headers = new HashMap<String, String>();

      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));

      switch (compress.algorithm) {
        case ODPS_RAW: {
          break;
        }
        case ODPS_ZLIB: {
          headers.put(Headers.ACCEPT_ENCODING, "deflate");
          break;
        }
        case ODPS_SNAPPY: {
          headers.put(Headers.ACCEPT_ENCODING, "x-snappy-framed");
          break;
        }
        default: {
          throw new TunnelException("invalid compression option.");
        }
      }

      if (columns != null && columns.size() != 0) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); ++i) {
          sb.append(columns.get(i).getName());
          if (i != columns.size() - 1) {
            sb.append(",");
          }
        }
        params.put(TunnelConstants.RES_COLUMNS, sb.toString());
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
          throw err;
        }

        boolean is_compress = false;
        String content_encoding = resp.getHeader(Headers.CONTENT_ENCODING);
        if (content_encoding != null) {
          if (content_encoding.equals("deflate")) {
            conf.setCompressOption(new CompressOption(CompressOption.CompressAlgorithm.ODPS_ZLIB,
                                                      -1, 0));
          } else if (content_encoding.equals("x-snappy-framed")) {
            conf.setCompressOption(new CompressOption(CompressOption.CompressAlgorithm.ODPS_SNAPPY,
                                                      -1, 0));
          } else {
            throw new TunnelException("invalid content encoding");
          }
          is_compress = true;
        }

        CompressOption option = is_compress ? conf.getCompressOption() : null;
        reader = new TunnelRecordReader(schema, columns, conn, option);

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

      if (this.shardId != null) {
        params.put(TunnelConstants.RES_SHARD, String.valueOf(this.shardId));
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

      if (shardId != null) {
        params.put(TunnelConstants.RES_SHARD, String.valueOf(shardId));
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
     */
    public DownloadStatus getStatus() throws TunnelException, IOException {
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
