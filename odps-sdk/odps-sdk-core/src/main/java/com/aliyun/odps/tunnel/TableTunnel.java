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

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;
import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_TUNNEL_METRICS;
import static com.aliyun.odps.tunnel.TunnelConstants.TUNNEL_DATE_TRANSFORM_VERSION;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.UncheckedOdpsException;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.ArrowUtils;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.data.ArrowRecordWriter;
import com.aliyun.odps.data.ArrowStreamRecordReader;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordPack;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.options.MaxStorageDownloadOption;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.tunnel.impl.StreamUploadSessionImpl;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl;
import com.aliyun.odps.tunnel.io.ArrowTunnelRecordReader;
import com.aliyun.odps.tunnel.io.ArrowTunnelRecordWriter;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.tunnel.io.TunnelBufferedReader;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.utils.ColumnUtils;
import com.aliyun.odps.utils.ConnectionWatcher;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

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
 * <li>典型表数据上传流程： <br />
 *  1) 创建 TableTunnel<br />
 *  2) 创建 UploadSession<br />
 *  3) 创建 RecordWriter,写入 Record<br />
 *  4）提交上传操作<br />
 * <br />
 * <li>典型表数据下载流程：<br />
 *  1) 创建 TableTunnel<br />
 *  2) 创建 DownloadSession<br />
 *  3) 创建 RecordReader,读取 Record<br />
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

    public static void main(String args[]) {
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

        while ((record = recordReader.read()) != null) {
          recordWriter.write(record);
        }

        recordReader.close();

        recordWriter.close();
        uploadSession.commit(new Long[]{0L});
      } catch (TunnelException e) {
        e.printStackTrace();
      } catch (IOException e1) {
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

  public interface BlockVersionProvider {
    long generateVersion(long blockId);
  }

  private final Configuration config;
  private final Random random = new Random();
  private final Odps odps;

  /**
   * 构造此类对象
   *
   * @param odps
   *     {@link Odps}
   */
  public TableTunnel(Odps odps) {
    this.odps = odps;
    this.config = new Configuration(odps);
  }

  public TableTunnel(Odps odps, Configuration config) {
    this.odps = odps;
    this.config = config;
  }

  public Odps getOdps() {
    return this.odps;
  }

  public Configuration getConfig() {
    return this.config;
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
    return createUploadSession(projectName, config.getOdps().getCurrentSchema(), tableName, false);
  }

  /**
   * 在非分区表上创建上传会话
   *
   * @param projectName
   *     Project名称
   * @param tableName
   *     表名，非视图
   * @param overwrite
   *     Overwrite模式
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession createUploadSession(String projectName, String tableName,
      boolean overwrite) throws TunnelException {
    return createUploadSession(projectName, config.getOdps().getCurrentSchema(), tableName, overwrite);
  }

  /**
   * Create an upload session of a non-partitioned table.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param overwrite Overwrite.
   * @return {@link TableTunnel.UploadSession}
   */
  public TableTunnel.UploadSession createUploadSession(
      String projectName,
      String schemaName,
      String tableName,
      boolean overwrite) throws TunnelException {
    return new TableTunnel.UploadSession(projectName, schemaName, tableName, null, null, overwrite);
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
  public TableTunnel.UploadSession createUploadSession(
      String projectName,
      String tableName,
      PartitionSpec partitionSpec) throws TunnelException {
    return createUploadSession(projectName, tableName, partitionSpec, false);
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
   * @param overwrite
   *     Overwrite模式
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession createUploadSession(
      String projectName,
      String tableName,
      PartitionSpec partitionSpec,
      boolean overwrite) throws TunnelException {
    return createUploadSession(projectName, config.getOdps().getCurrentSchema(), tableName, partitionSpec, overwrite);
  }

  /**
   * Create an upload session of a partitioned table.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param partitionSpec Partition spec.
   * @param overwrite Overwrite.
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession createUploadSession(
      String projectName,
      String schemaName,
      String tableName,
      PartitionSpec partitionSpec,
      boolean overwrite) throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    return new TableTunnel.UploadSession(
        projectName,
        schemaName,
        tableName,
        partitionSpec.toString().replaceAll("'", ""),
        null,
        overwrite);
  }

  /**
   * 获得在非分区表的上传会话，且该会话将要使用 {@link TunnelBufferedWriter} 进行数据上传。
   * 当有多个这样的会话实例（多进程或多线程）共享会话 ID 时，需要同时声明此会话实例的唯一标识（shareId）和共享的会话实例个数（shares）。
   *
   * <h3>代码实例</h3>
   *
   * 两个会话实例（分别在两个线程中）共享同一个会话 ID
   *
   * <pre>
   * final String sid = "<session_id>";
   *
   * Thread t1 = new Thread() {
   *   @Override
   *   public void run() {
   *     try {
   *       TableTunnel.UploadSession up = tunnel.getUploadSession(projectName, tableName, sid, 2, 0);
   *       Record r = up.newRecord();
   *       RecordWriter w = up.openBufferedWriter();
   *       r.setBigint(0, 1L);
   *       w.write(r);
   *       w.close();
   *       up.commit();
   *     } catch (TunnelException e) {
   *       throw new RuntimeException(e.getMessage(), e);
   *     }
   *   }
   * };
   *
   * Thread t2 = new Thread() {
   *   @Override
   *   public void run() {
   *     try {
   *       TableTunnel.UploadSession up = tunnel.getUploadSession(projectName, tableName, sid, 2, 1);
   *       Record r = up.newRecord();
   *       RecordWriter w = up.openBufferedWriter();
   *       r.setBigint(0, 2L);
   *       w.write(r);
   *       w.close();
   *       up.commit();
   *     } catch (TunnelException e) {
   *       throw new RuntimeException(e.getMessage(), e);
   *     }
   *   }
   * };
   *
   * t1.start();
   * t2.start();
   * </pre>
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param id
   *     上传会话的ID {@link TableTunnel.UploadSession#getId()}
   * @param shares
   *     有多少个 UploadSession 实例共享这个会话 ID
   * @param shareId
   *     此 UploadSession 的唯一标识，建议为 0 开始的正整数
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession getUploadSession(
      String projectName,
      String tableName,
      String id,
      long shares,
      long shareId) throws TunnelException {
    return getUploadSession(projectName, tableName, null, id, shares, shareId);
  }

  /**
   * 获得在分区表的上传会话，且该会话将要使用 {@link TunnelBufferedWriter} 进行数据上传。
   * 当有多个这样的会话实例（多进程或多线程）共享会话 ID 时，需要同时声明此会话实例的唯一标识（shareId）和共享的会话实例个数（shares）。
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     指定分区 {@link PartitionSpec}
   * @param id
   *     上传会话的ID {@link TableTunnel.UploadSession#getId()}
   * @param shares
   *     有多少个 UploadSession 实例共享这个会话 ID
   * @param shareId
   *     此 UploadSession 的唯一标识，建议为 0 开始的正整数
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession getUploadSession(
      String projectName,
      String tableName,
      PartitionSpec partitionSpec,
      String id,
      long shares,
      long shareId) throws TunnelException {
    return getUploadSession(projectName, config.getOdps().getCurrentSchema(), tableName, partitionSpec, id, shares, shareId);
  }

  /**
   * Get the upload session specified by the upload session ID. Make sure the upload session uses
   * {@link TunnelBufferedWriter} to upload data.
   *
   * When the upload session is shared by multiple threads or processes, the total number of threads
   * or processes should be passed as {@code shares}, and a unique {@code shareId} should be passed
   * for each thread or process.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param partitionSpec Partition spec.
   * @param id Upload session ID.
   * @param shares Number of clients that shares this upload session.
   * @param shareId Unique ID of this client.
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession getUploadSession(
      String projectName,
      String schemaName,
      String tableName,
      PartitionSpec partitionSpec,
      String id,
      long shares,
      long shareId) throws TunnelException {
    if (!(shares >= 1)) {
      throw new IllegalArgumentException("Invalid arguments, shares must >= 1");
    }
    if (!(shareId >= 0)) {
      throw new IllegalArgumentException("Invalid arguments, shareId must >= 0");
    }
    if (!(shares > shareId)) {
      throw new IllegalArgumentException("Invalid arguments, shares must > shareId");
    }
    TableTunnel.UploadSession session;
    if (partitionSpec != null) {
      session = getUploadSession(projectName, schemaName, tableName, partitionSpec, id);
    } else {
      session = getUploadSession(projectName, schemaName, tableName, id);
    }
    session.shares = shares;
    session.curBlockId = shareId;
    return session;
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
  public TableTunnel.UploadSession getUploadSession(
      String projectName,
      String tableName,
      String id) throws TunnelException {
    return getUploadSession(projectName, config.getOdps().getCurrentSchema(), tableName, id);
  }

  /**
   * Get the upload session specified by the upload session ID.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param id Upload session ID.
   * @return
   * @throws TunnelException
   */
  public TableTunnel.UploadSession getUploadSession(
      String projectName,
      String schemaName,
      String tableName,
      String id) throws TunnelException {
    return new TableTunnel.UploadSession(projectName, schemaName, tableName, null, id, false);
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
  public TableTunnel.UploadSession getUploadSession(
      String projectName,
      String tableName,
      PartitionSpec partitionSpec,
      String id) throws TunnelException {
    return getUploadSession(projectName, config.getOdps().getCurrentSchema(), tableName, partitionSpec, id);
  }

  /**
   * Get the upload session specified by the upload session ID.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param partitionSpec Partition spec.
   * @param id Upload session ID.
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession getUploadSession(
      String projectName,
      String schemaName,
      String tableName,
      PartitionSpec partitionSpec,
      String id) throws TunnelException {
    return getUploadSession(projectName, schemaName, tableName, partitionSpec, id, true);
  }

  /**
   * Get the upload session specified by the upload session ID.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param partitionSpec Partition spec.
   * @param id Upload session ID.
   * @param getBlockId Indicates whether to retrieve a list of block IDs. If set to false, the blockList will be empty,
   *                   resulting in reduced latency; however, you will not be able to use commit(Long[] blocks) and must
   *                   use commit() instead.
   * @return {@link TableTunnel.UploadSession}
   * @throws TunnelException
   */
  public TableTunnel.UploadSession getUploadSession(
      String projectName,
      String schemaName,
      String tableName,
      PartitionSpec partitionSpec,
      String id,
      boolean getBlockId) throws TunnelException {
    return new TableTunnel.UploadSession(
        projectName,
        schemaName,
        tableName,
        partitionSpec == null ? null : partitionSpec.toString().replace("'", ""),
        id,
        false,
        getBlockId);
  }

  public DownloadSessionBuilder buildDownloadSession(
          String projectName,
          String tableName) {
    return new DownloadSessionBuilder().setProjectName(projectName).setTableName(tableName);
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
  @Deprecated
  public TableTunnel.DownloadSession createDownloadSession(
      String projectName,
      String tableName) throws TunnelException {
      return createDownloadSession(projectName, tableName, false);
  }

  /**
   * 在非分区表上创建下载会话
   *
   * @param projectName
   *     Project名称
   * @param tableName
   *     表名，非视图
   * @param async
   *     异步创建session,小文件多的场景下可以避免连接超时的问题
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  @Deprecated
  public TableTunnel.DownloadSession createDownloadSession(
      String projectName,
      String tableName,
      boolean async) throws TunnelException {
    return createDownloadSession(projectName, config.getOdps().getCurrentSchema(), tableName, async);
  }

  /**
   * Create a download session of a non-partitioned table.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param async Create the session asynchronously. Enable this option to avoid the connection
   *              timeout error caused by the small file issue.
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  @Deprecated
  public TableTunnel.DownloadSession createDownloadSession(
      String projectName,
      String schemaName,
      String tableName,
      boolean async) throws TunnelException {
    return buildDownloadSession(projectName, tableName)
              .setSchemaName(schemaName)
              .setAsyncMode(async)
              .build();
  }

  /**
   * 在分区表上创建下载会话
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
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  @Deprecated
  public TableTunnel.DownloadSession createDownloadSession(
      String projectName,
      String tableName,
      PartitionSpec partitionSpec) throws TunnelException {
    return createDownloadSession(projectName, tableName, partitionSpec, false);
  }

  /**
   * 在分区表上创建下载会话
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
   * @param async
   *     异步创建session,小文件多的场景下可以避免连接超时的问题
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  @Deprecated
  public TableTunnel.DownloadSession createDownloadSession(
      String projectName,
      String tableName,
      PartitionSpec partitionSpec,
      boolean async) throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    return createDownloadSession(projectName, config.getOdps().getCurrentSchema(), tableName, partitionSpec, async);
  }

  /**
   * Create a download session of a partitioned table.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param partitionSpec Partition spec.
   * @param async Create the session asynchronously. Enable this option to avoid the connection
   *              timeout error caused by the small file issue.
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  @Deprecated
  public TableTunnel.DownloadSession createDownloadSession(
      String projectName,
      String schemaName,
      String tableName,
      PartitionSpec partitionSpec,
      boolean async) throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    return buildDownloadSession(projectName, tableName)
              .setSchemaName(schemaName)
              .setPartitionSpec(partitionSpec)
              .setAsyncMode(async)
              .build();
  }

  /**
   * This method is deprecated. Shard table is no longer supported.
   */
  @Deprecated
  public TableTunnel.DownloadSession createDownloadSession(String projectName, String tableName,
                                                           long shardId) throws TunnelException {
    if (shardId < 0) {
      throw new IllegalArgumentException("Invalid arguments, shard id required.");
    }
    return buildDownloadSession(projectName, tableName)
            .setShardId(shardId)
            .build();
  }

  /**
   * This method is deprecated. Shard table is no longer supported.
   */
  @Deprecated
  public TableTunnel.DownloadSession createDownloadSession(String projectName, String tableName,
                                                           PartitionSpec partitionSpec,
                                                           long shardId) throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    if (shardId < 0) {
      throw new IllegalArgumentException("Invalid arguments, shard id required.");
    }
    return buildDownloadSession(projectName, tableName)
            .setPartitionSpec(partitionSpec)
            .setShardId(shardId)
            .build();
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
  public TableTunnel.DownloadSession getDownloadSession(
      String projectName,
      String tableName,
      String id) throws TunnelException {
    return getDownloadSession(projectName, config.getOdps().getCurrentSchema(), tableName, id);
  }

  /**
   * Get the download session specified by the download session ID.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param id Download session ID.
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession getDownloadSession(
      String projectName,
      String schemaName,
      String tableName,
      String id) throws TunnelException {
    return buildDownloadSession(projectName, tableName)
            .setSchemaName(schemaName)
            .setDownloadId(id)
            .build();
  }

  /**
   * This method is deprecated. Shard table is no longer supported.
   */
  @Deprecated
  public TableTunnel.DownloadSession getDownloadSession(
      String projectName,
      String tableName,
      long shardId,
      String id) throws TunnelException {
    return buildDownloadSession(projectName, tableName)
            .setShardId(shardId)
            .setDownloadId(id)
            .build();
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
  public TableTunnel.DownloadSession getDownloadSession(
      String projectName,
      String tableName,
      PartitionSpec partitionSpec,
      String id) throws TunnelException {
    return getDownloadSession(projectName, config.getOdps().getCurrentSchema(), tableName, partitionSpec, id);
  }

  /**
   * Get the download session specified by the download session ID.
   *
   * @param projectName Project name.
   * @param schemaName Schema name.
   * @param tableName Table name.
   * @param partitionSpec Partition spec.
   * @param id Download session ID.
   * @return {@link TableTunnel.DownloadSession}
   * @throws TunnelException
   */
  public TableTunnel.DownloadSession getDownloadSession(
      String projectName,
      String schemaName,
      String tableName,
      PartitionSpec partitionSpec,
      String id) throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    return buildDownloadSession(projectName, tableName)
            .setSchemaName(schemaName)
            .setPartitionSpec(partitionSpec)
            .setDownloadId(id)
            .build();
  }

  /**
   * This method is deprecated. Shard table is no longer supported.
   */
  @Deprecated
  public TableTunnel.DownloadSession getDownloadSession(String projectName, String tableName,
                                                        PartitionSpec partitionSpec, long shardId,
                                                        String id) throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException("Invalid arguments, partition spec required.");
    }
    if (shardId < 0) {
      throw new IllegalArgumentException("Invalid arguments, shard id required.");
    }
    return buildDownloadSession(projectName, tableName)
            .setPartitionSpec(partitionSpec)
            .setShardId(shardId)
            .setDownloadId(id)
            .build();
  }

  private String getResource(String projectName, String tableName) {
    return config.getResource(projectName, tableName);
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

  public TableTunnel.StreamUploadSession.Builder buildStreamUploadSession(
          String projectName, String tableName) {
    return new StreamUploadSessionImpl.Builder().setConfig(this.config)
                                                .setProjectName(projectName)
                                                .setTableName(tableName).setSchemaName(config.getOdps().getCurrentSchema());
  }

  public TableTunnel.UpsertSession.Builder buildUpsertSession(
          String projectName, String tableName) {
    return new UpsertSessionImpl.Builder().setConfig(this.config)
                                          .setProjectName(projectName)
                                          .setTableName(tableName);
  }

  /**
   * 数据预览接口，通过tunnel下载指定行数的数据，最多返回5k行，超出部分将被截断。
   * 返回Arrow格式的数据流
   * @param projectName ODPS project name
   * @param schemaName ODPS schema name。如不指定分区可传入null。
   * @param tableName ODPS table name
   * @return 返回Arrow格式的数据流 {@link ArrowStreamReader}
   * 可以使用 {@link ArrowStreamRecordReader} 转换为RecordReader
   */
  public ArrowStreamReader preview(String projectName, String schemaName, String tableName) throws TunnelException {
    return preview(projectName, schemaName, tableName, null);
  }

  /**
   * 数据预览接口，通过tunnel下载指定行数的数据，最多返回5k行，超出部分将被截断。
   * 返回Arrow格式的数据流
   * @param projectName ODPS project name
   * @param schemaName ODPS schema name。如不指定分区可传入null。
   * @param tableName ODPS table name
   * @param partitionSpec 表的分区名{@link PartitionSpec}。如不指定分区可传入null。
   * @return 返回Arrow格式的数据流 {@link ArrowStreamReader}
   * 可以使用 {@link ArrowStreamRecordReader} 转换为RecordReader
   */
  public ArrowStreamReader preview(String projectName, String schemaName, String tableName,
                             String partitionSpec) throws TunnelException {
    return preview(projectName, schemaName, tableName, partitionSpec, -1L);
  }

  /**
   * @param projectName ODPS project name
   * @param schemaName ODPS schema name。如不指定分区可传入null。
   * @param tableName ODPS table name
   * @param partitionSpec 表的分区名{@link PartitionSpec}。如不指定分区可传入null。
   * @param limit     最多读取的记录行数，最大为5000。
   * 数据预览接口，通过tunnel下载指定行数的数据，最多返回5k行，超出部分将被截断。
   * @return 返回Arrow格式的数据流 {@link ArrowStreamReader}
   * 可以使用 {@link ArrowStreamRecordReader} 转换为RecordReader
   */
  public ArrowStreamReader preview(String projectName, String schemaName, String tableName,
                             String partitionSpec, Long limit) throws TunnelException {
    return preview(projectName, schemaName, tableName, partitionSpec, limit, null);
  }

  public ArrowStreamReader preview(String projectName, String schemaName, String tableName,
                                   String partitionSpec, Long limit, List<String> requiredColumns)
      throws TunnelException {
    return preview(projectName, schemaName, tableName, partitionSpec, limit, requiredColumns, null);
  }

  /**
   * 由于 arrow-memory-compression 依赖版本问题，先默认不压缩
   */
  private ArrowStreamReader preview(String projectName, String schemaName, String tableName,
                                    String partitionSpec, Long limit, List<String> requiredColumns,
                                    String acceptEncoding) throws TunnelException {
    if (limit != null && limit < 0) {
      limit = -1L;
    }
    HashMap<String, String> headers = getCommonHeader();
    if (acceptEncoding != null) {
      headers.put(Headers.ACCEPT_ENCODING, acceptEncoding);
    }

    Map<String, String> params = new HashMap<>();
    params.put("limit", String.valueOf(limit));
    if (partitionSpec != null) {
      params.put("partition", partitionSpec);
    }
    if (requiredColumns != null && !requiredColumns.isEmpty()) {
      TableSchema tableSchema = odps.tables().get(projectName, schemaName, tableName).getSchema();
      List<String> orderedColumns = ColumnUtils.orderColumns(tableSchema, requiredColumns);
      params.put("columns", String.join(",", orderedColumns));
    }

    String resource = ResourceBuilder.buildTableResource(projectName, schemaName, tableName);
    resource += "/preview";

    RestClient client = config.newRestClient(projectName);
    Connection conn;
    try {
      conn = client.connect(resource, "GET", params, headers);
      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        throw new TunnelException(conn.getInputStream());
      }
      String contentEncoding = resp.getHeader(Headers.CONTENT_ENCODING);
      CompressionCodec.Factory compressionFactory;
      switch (Optional.ofNullable(contentEncoding).orElse("").toUpperCase()) {
//        case "ZSTD":
//        case "LZ4_FRAME":
//          compressionFactory = CommonsCompressionFactory.INSTANCE;
//          break;
        default:
          compressionFactory = NoCompressionCodec.Factory.INSTANCE;
      }
      RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
      return new ArrowStreamReader(conn.getInputStream(), allocator, compressionFactory);
    } catch (IOException e) {
      throw new TunnelException("Failed to connect to the tunnel endpoint " + client.getEndpoint(),
                                e);
    } catch (TunnelException e) {
      // Do not delete here! TunnelException extends from OdpsException.
      throw e;
    } catch (Exception e) {
      throw new TunnelException(e.getMessage(), e);
    }
  }

  public interface FlushResult {
    public String getTraceId();
    public long getFlushSize();
    public long getRecordCount();
  }

  public interface StreamRecordPack {

    /**
     * append一条记录
     * @param record
     */
    public void append(Record record) throws IOException;

    /**
     * @return 返回当前pack存储的记录数
     */
    public long getRecordCount();

    /**
     * 注意：由于在写到内存缓冲区前，数据会经过多层缓冲区
     * 因此这个值的变化并不是连续的，有可能出现append数据后，getDataSize不变的场景
     * @return 返回当前pack存储数据的大小
     */
    public long getDataSize();

    /**
     * 数据发送到server端
     * pack对象在flush成功以后可以复用
     * @return traceId
     * @throws IOException
     */
    public String flush() throws IOException;

    /**
     * 数据发送到server端
     * pack对象在flush成功以后可以复用
     * @param flushOption 设置 write 参数 {@link FlushOption}
     * @return flush result
     * @throws IOException
     */
    public FlushResult flush(FlushOption flushOption) throws IOException;

    /**
     * 重置缓冲区内存，保证pack对象可以复用
     * pack对象在flush成功以后，会默认调用一次reset
     */
    public void reset() throws IOException;
  }

  /**
   * FlushOption 用于设置数据写入网络流时的一些配置。
   */
  public static class FlushOption {

    private long timeout = 0;

    /**
     * 设置写入操作的超时时间。
     * @param tm 单位毫秒 <= 0 代表无超时
     */
    public FlushOption timeout(long tm) {
      this.timeout = tm;
      return this;
    }

    public long getTimeout() {
      return timeout;
    }

    public void setTimeout(long timeout) {
      this.timeout = timeout;
    }
  }

  public interface StreamUploadSession {

    /**
     * 设置p2p mode
     * @param mode
     */
    public void setP2pMode(boolean mode);

    /**
     * 获取Session ID
     * @return Session ID
     */
    public String getId();

    /**
     * 获取表结构
     */
    public TableSchema getSchema();

    /**
     * 获取表结构Id
     */
    public String getSchemaVersion();


    /**
     * 获取当前 Quota
     */
    public String getQuotaName();

    /**
     * 创建一个无压缩{@Link StreamRecordPack}对象
     * @return StreamRecordPack对象
     */
    public StreamRecordPack newRecordPack() throws IOException;

    /**
     * 创建一个{@Link StreamRecordPack}对象
     * @param compressOption 数据传输压缩选项
     * @return StreamRecordPack对象
     */
    public StreamRecordPack newRecordPack(CompressOption compressOption) throws IOException, TunnelException;

    /**
     * 创建一个{@Link Record}对象
     * @return Record对象
     */
    public Record newRecord();

    default void close() throws IOException {
    }

    abstract class Builder {
      private String schemaName;
      private PartitionSpec partitionSpec;
      private long slotNum = 0;
      private boolean createPartition = false;
      private String schemaVersion;
      protected boolean allowSchemaMismatch = true;
      private boolean dynamicPartition = false;
      public String getSchemaName() {
        return schemaName;
      }

      public Builder setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
      }

      public String getPartitionSpec() {
        return this.partitionSpec == null ? null : partitionSpec.toString().replaceAll("'", "");
      }

      public Builder setPartitionSpec(PartitionSpec spec) {
        this.partitionSpec = spec;
        return this;
      }

      public Builder setPartitionSpec(String spec) {
        this.partitionSpec = spec == null ? null : new PartitionSpec(spec);
        return this;
      }

      public long getSlotNum() {
        return slotNum;
      }

      public Builder setSlotNum(long slotNum) {
        this.slotNum = slotNum;
        return this;
      }

      public Builder setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
        return this;
      }

      public String getSchemaVersion() {
        return schemaVersion;
      }

      public boolean isCreatePartition() {
        return createPartition;
      }

      public Builder setCreatePartition(boolean createPartition) {
        this.createPartition = createPartition;
        return this;
      }

      public Builder allowSchemaMismatch(boolean allowSchemaMismatch) {
        this.allowSchemaMismatch = allowSchemaMismatch;
        return this;
      }

      public Builder setDynamicPartition(boolean dynamicPartition) {
        this.dynamicPartition = dynamicPartition;
        return this;
      }

      abstract public StreamUploadSession build() throws TunnelException;
    }
  }

  public interface UpsertSession extends Closeable {

    /**
     * 获取Session ID
     * @return Session ID
     */
    String getId();

    /**
     * 获取当前 Quota
     */
    public String getQuotaName();

    /**
     * 获取Session状态
     * @return 状态码
     *  normal
     *  committing
     *  committed
     *  expired
     *  critical
     *  aborted
     */
    String getStatus() throws TunnelException;

    /**
     * 获取表结构
     */
    TableSchema getSchema();

    /**
     * 提交UpsertSession
     */
    void commit(boolean async) throws TunnelException;

    /**
     * 中止UpsertSession
     */
    void abort() throws TunnelException;

    /**
     * 清理客户端本地资源
     */
    void close();

    /**
     * 创建一个{@Link Record}对象
     * @return Record对象
     */
    Record newRecord();

    /**
     * 创建一个{@Link Record} 对象，默认在 SetByName 时非 caseSensitive
     * 可以配置为true，能够提高创建效率
     * @return Record对象
     */
    Record newRecord(boolean caseSensitive);

    UpsertStream.Builder buildUpsertStream();

    interface Builder {
      String getUpsertId();

      UpsertSession.Builder setUpsertId(String upsertId);

      String getSchemaName();

      UpsertSession.Builder setSchemaName(String schemaName);

      String getPartitionSpec();

      UpsertSession.Builder setPartitionSpec(PartitionSpec spec);

      UpsertSession.Builder setPartitionSpec(String spec);

      long getSlotNum();

      UpsertSession.Builder setSlotNum(long slotNum);

      long getCommitTimeout();

      UpsertSession.Builder setCommitTimeout(long commitTimeoutMs);

      /**
       * Netty 进行网络IO的线程池（EventLoop）的线程数，默认为 1
       */
      UpsertSession.Builder setNetworkThreadNum(int threadNum);

      /**
       * 最大并发数（允许同时存在的 Channel 数量），默认为 20，设为 <=0 为无限制
       */
      UpsertSession.Builder setConcurrentNum(int concurrentNum);

      /**
       * 建立链接的超时时间，单位 毫秒，默认为 180 * 1000
       */
      UpsertSession.Builder setConnectTimeout(long timeout);

      /**
       * 请求响应的超时时间，单位 毫秒，默认为 300 * 1000
       */
      UpsertSession.Builder setReadTimeout(long timeout);

      long getLifecycle();

      /**
       * Session生命周期，单位 小时，有效值域为 1 - 24，指定有效值域以外的值该参数会被忽略，使用服务端默认值
       */
      UpsertSession.Builder setLifecycle(long lifecycle);

      UpsertSession build() throws TunnelException, IOException;
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
    UNKNOWN, NORMAL, CLOSING, CLOSED, CANCELED, EXPIRED, CRITICAL, COMMITTING
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
   * 创建 RecordWriter 时需指定 block ID，block ID是 RecordWriter 的唯一标识符,取值范围 [0, 20000)，单个block上传的数据限制是
   * 100G。<br />
   * 同一 UploadSession 中，使用同一 block ID 多次打开 RecordWriter 会导致覆盖行为，最后一个调用 close() 的 RecordWriter
   * 所上传的数据会被保留。同一RecordWriter实例不能重复调用 close().<br />
   * RecordWriter 对应的 HTTP Request超时为 120s，若 120s 内没有数据传输，service 端会主动关闭连接。特别提醒，HTTP协议本身有8K
   * buffer。<br />
   * </p>
   * <br/>
   * <p>
   * 最后调用 {@link #commit(Long[])} 来提交本次上传的所有数据块。<br />
   * commit 操作可以重试，除非遇到以下异常：
   * <ul>
   * <li>CloseUploadSessionStatusConflictException
   * <li>DataCollisionException
   * <li>InternalServerError
   * <li>LOCAL_ERROR_CODE
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
    private String schemaName;
    private String tableName;
    private String partitionSpec;
    private Long fieldMaxSize;
    private List<Long> blocks = new ArrayList<Long>();
    private UploadStatus status = UploadStatus.UNKNOWN;
    private String quotaName;

    private Configuration conf;

    private RestClient tunnelServiceClient;

    // TunnelBufferedWriter 完成对 blockId 的自动管理，UploadSession 比原来多了以下状态
    // shares 表示有多少 TunnelBufferedWriter 在对这个表进行写
    // 通过 createUploadSession 创建的会话，shares 是 1。通过 getUploadSession 得到的会话，shares 大于 1
    private final Long totalBLocks = 20000L;
    private Long shares = 1L;
    private Long curBlockId = 0L;

    private static final int RETRY_SLEEP_SECONDS = 5;
    private boolean shouldTransform = false;
    private boolean overwrite = false;
    private boolean fetchBlockId = true;

    /**
     * 构造一个{@link UploadSession}对象
     *
     * <p>
     * 在多线程或者多进程模式下，推荐各个进程或者线程共享相同的uploadId， 使用此方法创建Upload对象来进行上传，可以提高性能。
     * </p>
     *
     * @param projectName
     *     上传数据表所在的project名称。
     * @param schemaName
     *     上传数据表所在的schema名称。
     * @param tableName
     *     上传数据表名称。
     * @param partitionSpec
     *     上传数据表的partition描述，格式如下: pt=xxx,dt=xxx。
     * @param uploadId
     *     Upload的唯一标识符。
     * @param overwrite
     *     是否覆盖数据。
     */
    UploadSession(
        String projectName,
        String schemaName,
        String tableName,
        String partitionSpec,
        String uploadId,
        boolean overwrite) throws TunnelException {
      this(projectName, schemaName, tableName, partitionSpec, uploadId, overwrite, true);
    }

    UploadSession(
        String projectName,
        String schemaName,
        String tableName,
        String partitionSpec,
        String uploadId,
        boolean overwrite,
        boolean fetchBlockId) throws TunnelException {
      this.conf = TableTunnel.this.config;
      this.projectName = projectName;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.partitionSpec = partitionSpec;
      this.id = uploadId;
      this.overwrite = overwrite;
      this.fetchBlockId = fetchBlockId;

      tunnelServiceClient = conf.newRestClient(projectName);

      initiateOrReload();
    }

    private void initiateOrReload() throws TunnelException {
      TunnelRetryHandler retryHandler = new TunnelRetryHandler(conf);
      try {
        retryHandler.executeWithRetry(
            () -> {
              if (this.id == null) {
                initiate();
              } else {
                reload();
              }
              return null;
            }
        );
      } catch (TunnelException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new TunnelException(e.getMessage(), e);
      }
    }

    /* Initiate upload session */
    private void initiate() throws TunnelException {
      HashMap<String, String> headers = getCommonHeader();

      List<String> tags = this.conf.getTags();
      if (tags != null) {
        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
      }

      HashMap<String, String> params = new HashMap<>();
      params.put(TunnelConstants.UPLOADS, null);
      if (this.partitionSpec != null && this.partitionSpec.length() > 0) {
        params.put(TunnelConstants.RES_PARTITION, partitionSpec);
      }
      if (this.overwrite) {
        params.put(TunnelConstants.OVERWRITE, "true");
      }
      if (this.conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, this.conf.getQuotaName());
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
          throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID), conn.getInputStream(), resp.getStatus());
        }
      } catch (IOException e) {
        throw new TunnelException("Failed to create upload session with tunnel endpoint "
                                  + tunnelServiceClient.getEndpoint(), e);
      } catch (TunnelException e) {
        // Do not delete here! TunnelException extends from OdpsException.
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


    public boolean isShouldTransform()
    {
      return shouldTransform;
    }

    /**
     * 多个线程中的 {@link TunnelBufferedWriter} 将通过这个接口获得写入的 blockId
     *
     * 为了防止 blockId 重复分配，对于 curBlockId 的访问必须加锁。
     *
     * @return 分配到的 blockId
     * @throws TunnelException
     */
    synchronized public Long getAvailBlockId() {
      if (curBlockId >= totalBLocks) {
        throw new RuntimeException("No more available blockId, already " + curBlockId);
      }
      Long old = curBlockId;
      curBlockId += shares;
      return old;
    }

    /**
     * 不进行校验的会话提交
     *
     * @throws TunnelException
     * @throws IOException
     */
    public void commit() throws TunnelException, IOException {
      completeUpload();
    }

    /**
     * 打开http链接，写入pack数据，然后关闭链接，多次向同一个block写入时会覆盖之前数据
     *
     * @param blockId
     *     块标识
     * @param pack
     *     pack数据
     */
    public void writeBlock(long blockId, RecordPack pack)
        throws IOException {
        writeBlock(blockId, pack, 0);
    }

    /**
     * 打开http链接，写入pack数据，然后关闭链接，多次向同一个block写入时会覆盖之前数据
     *
     * @param blockId
     *     块标识
     * @param pack
     *     pack数据
     * @param timeout
     *     超时时间 单位 ms 仅对 ProtobufRecordPack 有效 <=0 无超时
     */
    public void writeBlock(long blockId, RecordPack pack, long timeout)
            throws IOException {
        writeBlockInternal(blockId, pack, timeout, 0);
    }

    public void writeBlock(long blockId, RecordPack pack, long timeout, long blockVersion)
        throws IOException, TunnelException {
        checkBlockVersion(blockVersion);
        writeBlockInternal(blockId, pack, timeout, blockVersion);
    }

    private void writeBlockInternal(long blockId, RecordPack pack, long timeout, long blockVersion)
        throws IOException {
      TunnelRetryHandler retryHandler = new TunnelRetryHandler(conf);
      try {
        retryHandler.executeWithRetry(() -> {
          Connection conn = null;
          try {
            if (pack instanceof ProtobufRecordPack) {
              ProtobufRecordPack protoPack = (ProtobufRecordPack) pack;
              long startTime = System.currentTimeMillis();
              conn = getConnection(blockId, protoPack.getCompressOption(), blockVersion);
              protoPack.addNetworkWallTimeMs(System.currentTimeMillis() - startTime);
              protoPack.addLocalWallTimeMs(System.currentTimeMillis() - startTime);
              sendBlock(protoPack, conn, timeout);
            } else {
              RecordWriter writer = openRecordWriter(blockId);
              RecordReader reader = pack.getRecordReader();
              Record record;
              while ((record = reader.read()) != null) {
                writer.write(record);
              }
              writer.close();
            }
          } finally {
            if (conn != null) {
              try {
                conn.disconnect();
              } catch (IOException e) {
              }
            }
          }
          return null;
        });
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new IOException(e.getMessage(), e);
      }
    }

    private void sendBlock(ProtobufRecordPack pack, Connection conn) throws IOException {
        sendBlock(pack, conn, 0);
    }

    private void sendBlock(ProtobufRecordPack pack, Connection conn, long timeout) throws IOException {
      if (null == conn) {
        throw new IOException("Invalid connection");
      }
      long startTime = System.currentTimeMillis();
      pack.checkTransConsistency(shouldTransform);
      pack.complete();
      ByteArrayOutputStream baos = pack.getProtobufStream();
      if (timeout > 0) {
        ConnectionWatcher.getInstance().mark(conn, timeout);
      }
      Response response = null;
      try {
        baos.writeTo(conn.getOutputStream());
        conn.getOutputStream().close();
        baos.close();
        response = conn.getResponse();
      } catch (Throwable tr) {
        if (timeout > 0 && ConnectionWatcher.getInstance().checkTimedOut(conn)) {
          throw new SocketTimeoutException("Flush time exceeded timeout user set: " + timeout + "ms");
        }
        throw tr;
      } finally {
        if (timeout > 0) {
          ConnectionWatcher.getInstance().release(conn);
        }
      }
      if (!response.isOK()) {
        TunnelException exception =
            new TunnelException(response.getHeader(HEADER_ODPS_REQUEST_ID), conn.getInputStream(),
                                response.getStatus());
        throw new IOException(exception.getMessage(), exception);
      }
      // metrics
      String metricsStr = response.getHeader(HEADER_ODPS_TUNNEL_METRICS);
      TunnelMetrics batchMetrics =
          TunnelMetrics.parse(metricsStr, pack.getLocalWallTimeMs() + (System.currentTimeMillis() - startTime),
                              pack.getNetworkWallTimeMs() + (System.currentTimeMillis() - startTime));
      pack.addMetrics(batchMetrics);
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
      CompressOption option = compress ? new CompressOption() :
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
      return openRecordWriterInternal(blockId, compress, 0);
    }

    public RecordWriter openRecordWriter(long blockId, CompressOption compress, long blockVersion)
        throws TunnelException,
        IOException {
      checkBlockVersion(blockVersion);
      return openRecordWriterInternal(blockId, compress, blockVersion);
    }

    private RecordWriter openRecordWriterInternal(long blockId, CompressOption compress, long blockVersion)
        throws TunnelException {
      long startTime = System.currentTimeMillis();
      TunnelRetryHandler retryHandler = new TunnelRetryHandler(conf);
      try {
        return retryHandler.executeWithRetry(() -> {
          Connection conn = null;
          try {
            TunnelRecordWriter writer = null;
            conn = getConnection(blockId, compress, blockVersion);
            writer =
                new TunnelRecordWriter(schema, conn, compress);
            writer.setTransform(shouldTransform);
            writer.addWallTimeMs(System.currentTimeMillis() - startTime);
            return writer;
          } catch (IOException e) {
            if (conn != null) {
              try {
                conn.disconnect();
              } catch (IOException ignored) {
              }
            }
            throw e;
          }
        });
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new TunnelException(e.getMessage(), e);
      }
    }

    /**
     * 打开一个无压缩 {@link TunnelBufferedWriter} 用来写入数据
     */
    public RecordWriter openBufferedWriter() throws TunnelException {
      return openBufferedWriter(false);
    }

    /**
     * 打开 {@link TunnelBufferedWriter} 用来写入数据
     *
     * @param compress
     *     数据传输是否进行压缩
     */
    public RecordWriter openBufferedWriter(boolean compress) throws TunnelException {
      CompressOption compressOption = compress ? conf.getCompressOption() :
                              new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
      return openBufferedWriter(compressOption);
    }

    /**
     * 打开 {@link TunnelBufferedWriter} 用来写入数据
     *
     * @param compressOption
     *     数据传输压缩选项
     */
    public RecordWriter openBufferedWriter(CompressOption compressOption) throws TunnelException {
      return openBufferedWriter(compressOption, 0);
    }

    /**
     * 打开 {@link TunnelBufferedWriter} 用来写入数据
     *
     * @param compressOption
     *     数据传输压缩选项
     * @param timeout
     *     超时时间 单位 ms <=0 代表无超时. 推荐值: (BufferSizeInMB / UploadBandwidthInMB) * 1000 * 120%
     */
    public RecordWriter openBufferedWriter(CompressOption compressOption, long timeout) throws TunnelException {
      return openBufferedWriter(compressOption, timeout, null);
    }

    /**
     * 打开 {@link TunnelBufferedWriter} 用来写入数据
     *
     * @param compressOption
     *     数据传输压缩选项
     * @param timeout
     *     超时时间 单位 ms <=0 代表无超时. 推荐值: (BufferSizeInMB / UploadBandwidthInMB) * 1000 * 120%
     * @param versionProvider
     *     BlockVersion 提供者，为内部产生的 blockId 分别指定 block version, null 代表不使用此功能
     */
    public RecordWriter openBufferedWriter(CompressOption compressOption, long timeout, BlockVersionProvider versionProvider) throws TunnelException {
      try {
        return new TunnelBufferedWriter(this, compressOption, timeout, versionProvider);
      } catch (IOException e) {
        throw new TunnelException(e.getMessage(), e.getCause());
      }
    }

    private Schema arrowSchema;

    public Schema getArrowSchema() {
      if (this.arrowSchema == null){
        this.arrowSchema = ArrowUtils.tableSchemaToArrowSchema(this.schema);
      }
      return this.arrowSchema;
    }

    public ArrowRecordWriter openArrowRecordWriter(long blockId)
            throws TunnelException,
            IOException{
        return openArrowRecordWriter(blockId, new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0));
    }

    public ArrowRecordWriter openArrowRecordWriter(long blockId, CompressOption option)
        throws TunnelException,
        IOException{
        return openArrowRecordWriterInternal(blockId, option, 0);
    }

    public ArrowRecordWriter openArrowRecordWriter(long blockId, CompressOption option, long blockVersion)
        throws TunnelException,
        IOException{
      checkBlockVersion(blockVersion);
      return openArrowRecordWriterInternal(blockId, option, blockVersion);
    }

    private ArrowRecordWriter openArrowRecordWriterInternal(long blockId, CompressOption option, long blockVersion)
        throws TunnelException,
        IOException{
      ArrowTunnelRecordWriter arrowTunnelRecordWriter = null;
      Connection conn = null;
      try {
        conn = getConnection(blockId,true, option, blockVersion);
        arrowTunnelRecordWriter = new ArrowTunnelRecordWriter(this, conn, option);
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
      return arrowTunnelRecordWriter;
    }

    private Connection getConnection(long blockId, CompressOption compress, long blockVersion)
            throws OdpsException, IOException {
      return getConnection(blockId, false, compress, blockVersion);
    }

    private Connection getConnection(long blockId, boolean isArrow, CompressOption compress, long blockVersion)
        throws OdpsException, IOException {
      HashMap<String, String> headers = new HashMap<>();
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
        case ODPS_LZ4_FRAME: {
          headers.put(Headers.CONTENT_ENCODING, "x-lz4-frame");
          break;
        }
        case ODPS_ARROW_LZ4_FRAME: {
          headers.put(Headers.CONTENT_ENCODING, "x-odps-lz4-frame");
          break;
        }
        default: {
          throw new TunnelException("invalid compression option.");
        }
      }
      List<String> tags = this.conf.getTags();
      if (tags != null) {
        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
      }

      HashMap<String, String> params = new HashMap<>();
      if (blockVersion > 0) {
        params.put(TunnelConstants.PARAM_BLOCK_VERSION, Long.toString(blockVersion));
      }
      params.put(TunnelConstants.UPLOADID, id);
      params.put(TunnelConstants.BLOCKID, Long.toString(blockId));
      if (isArrow) {
        params.put(TunnelConstants.PARAM_ARROW,"");
      }
      if (partitionSpec != null && partitionSpec.length() > 0) {
        params.put(TunnelConstants.RES_PARTITION, partitionSpec);
      }
      if (conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, conf.getQuotaName());
      }

      return tunnelServiceClient.connect(getResource(), "PUT", params, headers);
    }

    private void reload() throws TunnelException {
      HashMap<String, String> headers = getCommonHeader();
      List<String> tags = this.conf.getTags();
      if (tags != null) {
        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
      }

      HashMap<String, String> params = new HashMap<>();
      params.put(TunnelConstants.UPLOADID, id);
      if (partitionSpec != null && partitionSpec.length() > 0) {
        params.put(TunnelConstants.RES_PARTITION, partitionSpec);
      }
      params.put(TunnelConstants.GET_BLOCK_ID, String.valueOf(fetchBlockId));
      if (this.conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, this.conf.getQuotaName());
      }

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
          throw e;
        }
      } catch (IOException e) {
        throw new TunnelException(e.getMessage(), e);
      } catch (TunnelException e) {
        // Do not delete here! TunnelException extends from OdpsException.
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
      HashMap<String, String> headers = getCommonHeader();
      List<String> tags = this.conf.getTags();
      if (tags != null) {
        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
      }

      HashMap<String, String> params = new HashMap<>();
      params.put(TunnelConstants.UPLOADID, id);
      if (partitionSpec != null && partitionSpec.length() > 0) {
        params.put(TunnelConstants.RES_PARTITION, partitionSpec);
      }
      if (this.conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, this.conf.getQuotaName());
      }

      TunnelRetryHandler retryHandler = new TunnelRetryHandler(conf);
      try {
        retryHandler.executeWithRetry(() -> {
          Connection conn = null;
          try {
            conn = tunnelServiceClient.connect(getResource(), "POST", params, headers);
            Response resp = conn.getResponse();

            if (resp.isOK()) {
              loadFromJson(conn.getInputStream());
              return null;
            } else {
              throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID),
                                        conn.getInputStream(),
                                        resp.getStatus());
            }
          } finally {
            if (conn != null) {
              try {
                conn.disconnect();
              } catch (IOException ignored) {
              }
            }
          }
        });
      } catch (TunnelException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new TunnelException(e.getMessage(), e);
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

    public String getQuotaName() { return quotaName; }

    /**
     * 获取会话状态
     */
    public UploadStatus getStatus() throws TunnelException, IOException {
      reload();
      return this.status;
    }

    public Configuration getConfig() {
      return conf;
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
      return new ArrayRecord(
          getSchema().getColumns().toArray(new Column[0]),
          true,
          fieldMaxSize,
          false);
    }

    public Record newRecord(boolean caseSensitive) {
      return new ArrayRecord(
          getSchema().getColumns().toArray(new Column[0]),
          true,
          fieldMaxSize,
          caseSensitive);
    }


    public RecordPack newRecordPack() throws IOException {
      return newRecordPack(null);
    }

    /**
     * 新建一个 ProtobufRecordPack，数据压缩方式 option
     *
     * @param option
     * @throws IOException
     */
    public RecordPack newRecordPack(CompressOption option) throws IOException {
      return newRecordPack(0, option);
    }

    /**
     * 新建一个 ProtobufRecordPack，预设流 buffer 大小为 capacity, 数据压缩方式 option
     *
     * @param capacity
     * @param option
     * @throws IOException
     */
    public RecordPack newRecordPack(int capacity, CompressOption option) throws IOException {
      ProtobufRecordPack pack = new ProtobufRecordPack(schema, new Checksum(), capacity, option);
      pack.setTransform(shouldTransform);
      return pack;
    }

    /**
     * 获取当前会话已经上传成功的数据块列表
     */
    public Long[] getBlockList() throws TunnelException, IOException {
      reload();
      return blocks.toArray(new Long[0]);
    }

    public String getResource() {
      return conf.getResource(projectName, schemaName, tableName);
    }

    /*
     * Parse session properties from json stream.
     */
    private void loadFromJson(InputStream is) throws TunnelException {
      try {
        String json = IOUtils.readStreamAsString(is);
        JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

        // session id
        if (tree.has("UploadID")) {
          id = tree.get("UploadID").getAsString();
        }

        // status
        if (tree.has("Status")) {
          String uploadStatus = tree.get("Status").getAsString().toUpperCase();
          status = UploadStatus.valueOf(uploadStatus);
        }

        // blocks
        blocks.clear();
        if (tree.has("UploadedBlockList")) {
          JsonArray blockList = tree.get("UploadedBlockList").getAsJsonArray();
          for (int i = 0; i < blockList.size(); ++i) {
            if (blockList.get(i).getAsJsonObject().has("BlockID")) {
              blocks.add(blockList.get(i).getAsJsonObject().get("BlockID").getAsLong());
            }
          }
        }

        // schema
        if (tree.has("Schema")) {
          JsonObject tunnelTableSchema = tree.get("Schema").getAsJsonObject();
          schema = new TunnelTableSchema(tunnelTableSchema);
        }

        // field max size
        if (tree.has("MaxFieldSize")) {
          fieldMaxSize = tree.get("MaxFieldSize").getAsLong();
        }

        if (tree.has("QuotaName")) {
          quotaName = tree.get("QuotaName").getAsString();
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
    UNKNOWN, NORMAL, CLOSED, EXPIRED, INITIATING
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
   * </p>
   */
  public class DownloadSession {

    private String id;
    private String projectName;
    private String schemaName;
    private String tableName;
    private String partitionSpec;
    private MaxStorageDownloadOption maxStorageOption;
    private Long shardId;
    private long count;
    private TableSchema schema = new TableSchema();
    private DownloadStatus status = DownloadStatus.UNKNOWN;
    private String quotaName;
    private Configuration conf;

    /**
     * tunnel下载行级权限表（RAP）会按照行级权限规则起sql进行过滤处理，
     * 此时SQL的InstanceId，非RAP时为null
     */
    private String RAPInstanceId;

    private RestClient tunnelServiceClient;
    private boolean shouldTransform = false;
    private final boolean enableMaxStorage;
    private boolean isDeltaTable;

    /**
     * 根据已有downloadId构造一个{@link DownloadSession}对象。
     *
     * @param projectName
     *     下载数据表所在project名称
     * @param schemaName
     *     下载数据表所在schema名称
     * @param tableName
     *     下载数据表名称
     * @param partitionSpec
     *     下载数据表的partition描述，格式如下: pt=xxx,dt=xxx
     * @param shardId
     *     下载数据表的shard标识
     * @param downloadId
     *     Download的唯一标识符
     * @param async
     *     异步创建session,小文件多的场景下可以避免连接超时的问题
     * @param waitAsyncBuild
     *     异步创建session时，是否等待session创建完成
     * @param maxStorageOption
     *     使用 MaxStorage 下载表时独有的配置项，目前仅 DeltaTable 使用 MaxStorage 下载
     */
    DownloadSession(
        String projectName,
        String schemaName,
        String tableName,
        String partitionSpec,
        Long shardId,
        String downloadId,
        boolean async,
        boolean waitAsyncBuild,
        boolean enableMaxStorage,
        MaxStorageDownloadOption maxStorageOption) throws TunnelException {
      this.conf = TableTunnel.this.config;
      this.projectName = projectName;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.partitionSpec = partitionSpec;
      this.shardId = shardId;
      this.id = downloadId;
      this.enableMaxStorage = enableMaxStorage;
      this.maxStorageOption = maxStorageOption;

      try {
        tunnelServiceClient = conf.newRestClient(projectName);
        Table table = conf.getOdps().tables().get(projectName, schemaName, tableName);
        if (enableMaxStorage && table.isDeltaTable()) {
          this.isDeltaTable = true;
          this.schema = table.getSchema();
          if (id == null) {
            initiateMaxStorageSession();
          } else {
            reloadMaxStorageSession();
          }
        } else {
          if (id == null) {
            initiate(async, waitAsyncBuild);
          } else {
            reload();
          }
        }
      } catch (UncheckedOdpsException e) {
        // when reload table, may throw UncheckedOdpsException (maybe table not exist or no privilege)
        TunnelException te = new TunnelException(e.getCause().getRequestId(), e.getCause().getMessage(), e.getCause());
        if (e.getCause().getMessage() != null && e.getCause().getMessage().contains("Access Denied")) {
          te.setErrorCode("NoPermission");
        }
        throw te;
      }
    }

    private void initiateMaxStorageSession() throws TunnelException {
      // plan Input Splits
      HashMap<String, String> headers = new HashMap<>();
      headers.put(Headers.CONTENT_TYPE, "application/json");
      List<String> tags = this.conf.getTags();

      if (tags != null) {
        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
      }
      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      HashMap<String, String> params = new HashMap<>();
      params.put(ConfigConstants.SESSION_TYPE, "batch_read");

      if (this.conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, this.conf.getQuotaName());
      }
      String request = generateReadSessionRequest();
      try {
        Response resp = tunnelServiceClient.stringRequest(
            ResourceBuilder.buildTableSessionResource(
                ConfigConstants.VERSION_1,
                projectName,
                schemaName,
                tableName,
                null),
            "POST", params, headers, request);
        String errorMsg;
        String response;
        if (resp.isOK()) {
          response = new String(resp.getBody());
          errorMsg = loadMaxStorageResultFromJson(response);
        } else {
          throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID),
                                    new ByteArrayInputStream(resp.getBody()), resp.getStatus());
        }
        if (this.status != DownloadStatus.NORMAL) {
          long asyncIntervalInMills = 3000; // hardcode now
          long asyncTimeoutInMills = 3600 * 1000L; // hardcode now
          long startTime = System.currentTimeMillis();

          while (this.status == DownloadStatus.INITIATING) {
            Thread.sleep(asyncIntervalInMills);
            errorMsg = reloadMaxStorageSession();
            if (System.currentTimeMillis() - startTime >= asyncTimeoutInMills) {
              break;
            }
          }
        }

        if (status != DownloadStatus.NORMAL) {
          throw new TunnelException(
              String.format(
                  "Create MaxStorage download session timeout.\n"
                  + "Table identifier: %s.\n"
                  + "Session status: %s.\n"
                  + "Session id: %s.\n"
                  + "Error message: %s.",
                  projectName + "." + schemaName + "." + tableName,
                  this.status.toString(),
                  id,
                  errorMsg));
        }
      } catch (OdpsException e) {
        throw new TunnelException(e.getErrorCode(), e.getMessage());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    private String reloadMaxStorageSession() throws TunnelException {
      // reload Input Splits
      HashMap<String, String> headers = new HashMap<>();
      List<String> tags = this.conf.getTags();
      if (tags != null) {
        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
      }
      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      HashMap<String, String> params = new HashMap<>();
      params.put(ConfigConstants.SESSION_TYPE, "batch_read");

      if (this.conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, this.conf.getQuotaName());
      }
      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(ResourceBuilder.buildTableSessionResource(
                                      ConfigConstants.VERSION_1,
                                      projectName,
                                      schemaName,
                                      tableName,
                                      id),
                                  "GET", params, headers);

        Response resp = conn.getResponse();

        if (resp.isOK()) {
          String response = IOUtils.readStreamAsString(conn.getInputStream());
          return loadMaxStorageResultFromJson(response);
        } else {
          throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID),
                                    conn.getInputStream(), resp.getStatus());
        }
      } catch (IOException e) {
        throw new TunnelException("Failed to reload table read session with endpoint: "
                              + tunnelServiceClient.getEndpoint(), e);
      } catch (OdpsException e) {
        throw new TunnelException(e.getRequestId(), e.getMessage(), e);
      } finally {
        if (conn != null) {
          try {
            conn.disconnect();
          } catch (IOException ignored) {
          }
        }
      }
    }

    private String generateReadSessionRequest() {
      JsonObject request = new JsonObject();
      JsonArray dataColumns = new JsonArray();
      if (maxStorageOption != null && maxStorageOption.getRequiredColumns() != null) {
        maxStorageOption.getRequiredColumns().stream().map(JsonPrimitive::new).forEach(dataColumns::add);
      }
      request.add("RequiredDataColumns", dataColumns);
      request.add("RequiredPartitionColumns", new JsonArray());

      JsonArray partitionFilters = new JsonArray();
      if (StringUtils.isNotBlank(this.partitionSpec)) {
        partitionFilters.add(
            new JsonPrimitive(new PartitionSpec(this.partitionSpec).toString(false, true)));
      }
      request.add("RequiredPartitions", partitionFilters);
      request.add("RequiredBucketIds", new JsonArray());

      JsonObject jsonSplitOptions = new JsonObject();
      jsonSplitOptions.addProperty("SplitMode", "Size");
      if (maxStorageOption != null && maxStorageOption.getSplitSize() != null) {
        jsonSplitOptions.addProperty("SplitNumber", maxStorageOption.getSplitSize());
      } else {
        jsonSplitOptions.addProperty("SplitNumber", 256 * 1024L * 1024L);
      }
      jsonSplitOptions.addProperty("CrossPartition", true);
      request.add("SplitOptions", jsonSplitOptions);

      request.add("SplitMaxFileNum", new JsonPrimitive(0));

      JsonObject arrowOptions = new JsonObject();
      arrowOptions.addProperty("TimestampUnit", "nano");
      arrowOptions.addProperty("DatetimeUnit", "milli");
      request.add("ArrowOptions", arrowOptions);

      request.add("FilterPredicate", new JsonPrimitive(""));

      Gson gson = new GsonBuilder().disableHtmlEscaping().create();
      return gson.toJson(request);
    }

    private String loadMaxStorageResultFromJson(String json) {
      JsonObject tree = new JsonParser().parse(json).getAsJsonObject();
      // session id
      if (tree.has("SessionId")) {
        this.id = tree.get("SessionId").getAsString();
      }
      // status
      if (tree.has("SessionStatus")) {
        String status = tree.get("SessionStatus").getAsString().toUpperCase();
        switch (status) {
          case "INIT":
            this.status = DownloadStatus.INITIATING;
            break;
          case "NORMAL":
          case "COMMITTING":
            this.status = DownloadStatus.NORMAL;
            break;
          case "COMMITTED":
            this.status = DownloadStatus.CLOSED;
            break;
          case "EXPIRED":
            this.status = DownloadStatus.EXPIRED;
            break;
          case "CRITICAL":
          case "UNKNOWN":
          default:
            this.status = DownloadStatus.UNKNOWN;
        }
      }
      // schema
      if (tree.has("DataSchema")) {
        JsonObject dataSchema = tree.get("DataSchema").getAsJsonObject();
        schema = new TunnelTableSchema(dataSchema, true);
      }
      // splits count
      if (tree.has("SplitsCount")) {
        count = tree.get("SplitsCount").getAsInt();
      }
      // error message
      String errorMessage = null;
      if (tree.has("Message")) {
        errorMessage = tree.get("Message").getAsString();
      }
      return errorMessage;
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
                                               List<Column> columns)
        throws TunnelException, IOException {
      CompressOption option = compress ? new CompressOption() :
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
                                               List<Column> columns)
        throws TunnelException, IOException {

      return openRecordReader(start, count, compress, columns, false);
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
     * @param disableModifiedCheck
     *     不检查下载的数据是否是表中最新数据
     * @throws TunnelException
     * @throws IOException
     */
    public TunnelRecordReader openRecordReader(long start, long count, CompressOption compress,
                                               List<Column> columns, boolean disableModifiedCheck)
        throws TunnelException, IOException {

      if (columns != null && columns.isEmpty()) {
        throw new TunnelException("Specified column list is empty.");
      }
      if (isDeltaTable) {
        throw new IllegalArgumentException(
            "Delta table does not support openRecordReader, use openArrowRecordReader instead.");
      }

      TunnelRecordReader reader = new TunnelRecordReader(start, count, columns, compress, tunnelServiceClient, this, disableModifiedCheck);
      reader.setTransform(shouldTransform);

      return reader;
    }

    public RecordReader openBufferedRecordReader(long start, long count, long batchSize, CompressOption compress,
                                               List<Column> columns, boolean disableModifiedCheck)
        throws TunnelException {
      if (columns != null && columns.isEmpty()) {
        throw new TunnelException("Specified column list is empty.");
      }
      TunnelBufferedReader reader = new TunnelBufferedReader(start, count, batchSize, columns, compress, this, disableModifiedCheck);
      reader.setTransform(shouldTransform);
      return reader;
    }

    private Schema arrowSchema;
    public Schema getArrowSchema() {
      if (this.arrowSchema == null){
        this.arrowSchema = ArrowUtils.tableSchemaToArrowSchema(this.schema);
      }
      return this.arrowSchema;
    }

    public ArrowRecordReader openArrowRecordReader(long splitIndex)
        throws TunnelException, IOException {
      return openArrowRecordReader(splitIndex, 1, null, null);
    }

    public ArrowRecordReader openArrowRecordReader(long start, long count)
            throws TunnelException, IOException {
      return openArrowRecordReader(start, count, null, null);
    }

    public ArrowRecordReader openArrowRecordReader(long start, long count, CompressOption compress)
        throws TunnelException, IOException {
      return openArrowRecordReader(start, count, null, null, compress);
    }

    public ArrowRecordReader openArrowRecordReader(long start, long count, BufferAllocator allocator)
            throws TunnelException, IOException {
      return openArrowRecordReader(start, count, null, allocator);
    }

    public ArrowRecordReader openArrowRecordReader(long start, long count, List<Column> columns)
            throws TunnelException, IOException {
      return openArrowRecordReader(start, count, columns, null);
    }

    public ArrowRecordReader openArrowRecordReader(long start, long count, List<Column> columns, BufferAllocator allocator)
        throws TunnelException, IOException {
      return openArrowRecordReader(start, count, columns, allocator, new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0));
    }

    public ArrowRecordReader openArrowRecordReader(long start, long count, List<Column> columns, BufferAllocator allocator, CompressOption compress)
            throws TunnelException, IOException {
      return openArrowRecordReader(start, count, columns, allocator, compress, false);
    }

    public ArrowRecordReader openArrowRecordReader(long start, long count, List<Column> columns, BufferAllocator allocator, CompressOption compress, boolean disableModifiedCheck)
        throws TunnelException, IOException {
      return new ArrowTunnelRecordReader(start, count, columns, this.tunnelServiceClient, this, allocator, compress, disableModifiedCheck);
    }

    // initiate a new download session
    private void initiate(boolean async, boolean wait) throws TunnelException {
      HashMap<String, String> headers = getCommonHeader();

      List<String> tags = this.conf.getTags();
      if (tags != null) {
        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
      }

      HashMap<String, String> params = new HashMap<>();
      params.put(TunnelConstants.DOWNLOADS, null);
      if (async) {
        params.put(TunnelConstants.ASYNC_MODE, "true");
      }
      if (partitionSpec != null && partitionSpec.length() > 0) {
        params.put(TunnelConstants.RES_PARTITION, partitionSpec);
      }
      if (this.shardId != null) {
        params.put(TunnelConstants.RES_SHARD, String.valueOf(this.shardId));
      }
      if (this.conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, this.conf.getQuotaName());
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
          throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID), conn.getInputStream(), resp.getStatus());
        }

        while (status == DownloadStatus.INITIATING && wait) {
          Thread.sleep(random.nextInt(30 * 1000) + 5 * 1000);
          reload();
        }
      } catch (IOException e) {
        throw new TunnelException("Failed to create download session with tunnel endpoint "
                                  + tunnelServiceClient.getEndpoint(), e);
      } catch (TunnelException e) {
        // Do not delete here! TunnelException extends from OdpsException.
        throw e;
      } catch (OdpsException e) {
        throw new TunnelException(e.getMessage(), e);
      } catch (InterruptedException e) {
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
      HashMap<String, String> headers = getCommonHeader();

      HashMap<String, String> params = new HashMap<>();
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
          shouldTransform =
              StringUtils.equals(resp.getHeader(HttpHeaders.HEADER_ODPS_DATE_TRANSFORM), "true");
        } else {
          TunnelException e = new TunnelException(conn.getInputStream());
          e.setRequestId(resp.getHeader(HEADER_ODPS_REQUEST_ID));
          throw e;
        }
      } catch (IOException e) {
        throw new TunnelException(e.getMessage(), e);
      } catch (TunnelException e) {
        // Do not delete here! TunnelException extends from OdpsException.
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
      if (isDeltaTable) {
        throw new UnsupportedOperationException("Delta table does not support getRecordCount, use getSplitCount instead.");
      }
      return this.count;
    }

    /**
     * 获取可下载的分片总数
     */
    public long getSplitCount() {
      return this.count;
    }

    public boolean isEnableMaxStorage() {
      return enableMaxStorage;
    }

    public boolean isUseMaxStorage() {
      return isDeltaTable;
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

    public Configuration getConfig() {
      return conf;
    }

    /**
     * 获取 partition
     */
    public String getPartitionSpec() {
      return partitionSpec;
    }

    public String getProjectName() {
      return projectName;
    }
    public String getSchemaName() {
      return schemaName;
    }

    public String getTableName() {
      return tableName;
    }

    public String getQuotaName() { return quotaName; }

    public String getRAPInstanceId() { return RAPInstanceId; }

    private String getResource() {
      return conf.getResource(projectName, schemaName, tableName);
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
          count =  tree.get("RecordCount").getAsLong();
        }

        // schema
        if (tree.has("Schema")) {
          JsonObject tunnelTableSchema = tree.get("Schema").getAsJsonObject();
          schema = new TunnelTableSchema(tunnelTableSchema);
        }

        if (tree.has("QuotaName")) {
          quotaName = tree.get("QuotaName").getAsString();
        }

        if (tree.has("RAPInstanceId")) {
          RAPInstanceId = tree.get("RAPInstanceId").getAsString();
        }
      } catch (Exception e) {
        throw new TunnelException("Invalid json content.", e);
      }
    }
  }

  public class DownloadSessionBuilder {
    private String projectName;
    private String schemaName;
    private String tableName;
    private List<String> requiredColumns;
    private PartitionSpec partitionSpec;
    private Long shardId;
    private String downloadId;
    private boolean asyncMode = false;
    private boolean waitAsyncBuild = true;
    private boolean enableMaxStorage = false;
    private MaxStorageDownloadOption maxStorageOption;

    public DownloadSessionBuilder setProjectName(String projectName) {
      this.projectName = projectName;
      return this;
    }

    public DownloadSessionBuilder setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public DownloadSessionBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public DownloadSessionBuilder setPartitionSpec(PartitionSpec partitionSpec) {
      this.partitionSpec = partitionSpec;
      return this;
    }

    public DownloadSessionBuilder setMaxStorageOption(MaxStorageDownloadOption maxStorageOption) {
      this.maxStorageOption = maxStorageOption;
      return this;
    }

    public DownloadSessionBuilder setDownloadId(String downloadId) {
      this.downloadId = downloadId;
      return this;
    }

    public DownloadSessionBuilder setShardId(Long shardId) {
      this.shardId = shardId;
      return this;
    }

    public DownloadSessionBuilder setAsyncMode(boolean asyncMode) {
      this.asyncMode = asyncMode;
      return this;
    }

    public DownloadSessionBuilder setWaitAsyncBuild(boolean waitAsyncBuild) {
      this.waitAsyncBuild = waitAsyncBuild;
      return this;
    }

    public DownloadSessionBuilder setEnableMaxStorage(boolean enableMaxStorage) {
      this.enableMaxStorage = enableMaxStorage;
      return this;
    }

    public DownloadSession build() throws TunnelException {
      return new TableTunnel.DownloadSession(projectName,
                                             schemaName,
                                             tableName,
                                             partitionSpec == null ? null : partitionSpec.toString().replaceAll("'", ""),
                                             shardId,
                                             downloadId,
                                             asyncMode,
                                             waitAsyncBuild,
                                             enableMaxStorage,
                                             maxStorageOption);
    }

    /**
     * 等待服务端异步创建{@link DownloadSession}请求成功
     *
     * @param download
     *     {@link DownloadSession}对象
     * @param interval
     *     客户端轮询间隔最大时间，单位：秒，最低1秒，最大30秒
     * @param timeout
     *     等待超时时间，单位：秒，最低0秒
     */
    public boolean wait(DownloadSession download, int interval, long timeout) throws TunnelException {
      if (download == null) {
        return false;
      }
      interval = max(interval, 1);
      interval = min(interval, 30);
      timeout = max(timeout, 0);
      int maxSleepIntervalMs = interval * 1000 + 1;
      long timeoutValueMs = timeout * 1000;
      while (download.status == DownloadStatus.INITIATING && timeoutValueMs > 0) {
        long sleepInterval = random.nextInt(maxSleepIntervalMs - 500) + 500;
        try {
          Thread.sleep(sleepInterval);
          download.reload();
          timeoutValueMs -= sleepInterval;
        } catch (InterruptedException e) {
          throw new TunnelException("Wait Async Create Download Session interrupted", e);
        }
      }
      if (download.status != DownloadStatus.INITIATING && download.status != DownloadStatus.NORMAL) {
        throw new TunnelException("Download session is " + download.status.toString());
      }
      return download.status == DownloadStatus.NORMAL;
    }
  }

  static HashMap<String, String> getCommonHeader() {
    HashMap<String, String> headers = new HashMap<>();

    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));
    headers.put(HttpHeaders.HEADER_ODPS_DATE_TRANSFORM, TUNNEL_DATE_TRANSFORM_VERSION);
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    return headers;
  }

  public static void checkBlockVersion(long blockVersion) throws TunnelException {
    if (blockVersion <= 0) {
      throw new TunnelException("Block version should be a positive integer.");
    }
  }
}
