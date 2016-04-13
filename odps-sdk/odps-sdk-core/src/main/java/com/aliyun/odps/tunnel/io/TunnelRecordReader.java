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

package com.aliyun.odps.tunnel.io;

import java.io.IOException;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamReader;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * TunnelReaderReader支持通过Tunnel服务读取ODPS表中的数据
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class TunnelRecordReader extends ProtobufRecordStreamReader {

  /**
   * 底层网络建立超时时间,30秒
   */
  public static final int DEFAULT_CONNECT_TIMEOUT = 30; // seconds

  /**
   * 底层网络重试次数, 4
   */
  public static final int DEFAULT_CONNECT_RETRYTIMES = 4;

  private int retryTimes = DEFAULT_CONNECT_RETRYTIMES;
  private int retryCount = 0;
  private long start = 0;
  private long count = 0;
  private long offset = 0;
  private long bytesReaded = 0;

  private boolean isClosed = false;

  private List<Column> columnList;
  private CompressOption option;
  private RestClient tunnelServiceClient;
  private TableTunnel.DownloadSession tableSession;
  private InstanceTunnel.DownloadSession instanceSession;
  private RawTunnelRecordReader reader;


  /**
   * 构造此类对象
   *
   * @param schema
   *     ODPS表的Schema  {@link TableSchema}
   * @param conn
   *     {@link Connection}
   * @param option
   *     {@link CompressOption}
   * @throws IOException
   */
  @Deprecated
  public TunnelRecordReader(TableSchema schema, Connection conn,
                            CompressOption option) throws IOException {

    super(schema, conn.getInputStream(), option);
  }

  /**
   * 构造此类对象
   *
   * @param schema
   *     ODPS表的Schema  {@link TableSchema}
   * @param columns
   *     需要读取的列 {@link Column}
   * @param conn
   *     {@link Connection}
   * @param option
   *     {@link CompressOption}
   * @throws IOException
   */
  @Deprecated
  public TunnelRecordReader(TableSchema schema, List<Column> columns, Connection conn,
                            CompressOption option) throws IOException {

    super(schema, columns, conn.getInputStream(), option);
  }

  /**
   * 构造此类对象
   *
   * @param columns
   *     需要读取的列 {@link Column}
   * @param option
   *     {@link CompressOption}
   * @param start
   *     本次要读取记录的起始位置
   * @param count
   *     本次要读取记录的数量
   * @param session
   *     本次读取所在 session
   * @throws IOException
   */
  public TunnelRecordReader(long start, long count, List<Column> columns,
                            CompressOption option, RestClient tunnelRestClient,
                            TableTunnel.DownloadSession session)
      throws TunnelException, IOException {
    this.start = start;
    this.count = count;
    this.offset = 0;
    this.option = option;
    this.columnList = columns;
    this.tableSession = session;
    this.reader = null;
    this.instanceSession = null;
    this.tunnelServiceClient = tunnelRestClient;

    createNewReader();
  }


  /**
   * 构造此类对象
   *
   * @param columns
   *     需要读取的列 {@link Column}
   * @param option
   *     {@link CompressOption}
   * @param start
   *     本次要读取记录的起始位置
   * @param count
   *     本次要读取记录的数量
   * @param session
   *     本次读取所在 session
   * @throws IOException
   */
  public TunnelRecordReader(long start, long count, List<Column> columns,
                            CompressOption option, RestClient tunnelRestClient,
                            InstanceTunnel.DownloadSession session)
      throws TunnelException, IOException {
    this.start = start;
    this.count = count;
    this.offset = 0;
    this.option = option;
    this.columnList = columns;
    this.tableSession = null;
    this.instanceSession = session;
    this.reader = null;
    this.tunnelServiceClient = tunnelRestClient;

    createNewReader();
  }

  @Override
  public void close() throws IOException {
    super.close();
    reader.close();
    isClosed = true;
  }

  @Override
  public Record read() throws IOException {
    return this.read(null);
  }

  @Override
  public Record read(Record reusedRecord) throws IOException {
    if (isClosed) {
      throw new IOException("Reader has been closed");
    }

    try {
      return readWithRetry(reusedRecord);
    } catch (TunnelException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getTotalBytes() {
    return bytesReaded + reader.getTotalBytes();
  }


  private Record readWithRetry(Record reusedRecord) throws TunnelException, IOException {
    try {
      Record record = reader.read(reusedRecord);
      offset += 1;
      return record;
    } catch (IOException e) {
      if (++retryCount > retryTimes || offset > count /* no more data */) {
        throw e;
      }

      createNewReader();

      return readWithRetry(reusedRecord);
    }
  }

  /**
   * 根据现有状态和已读数量,构建新的 TunnelRecordReader
   *
   * @throws TunnelException
   */
  private void createNewReader() throws TunnelException, IOException {
    while (retryCount <= retryTimes) {
      try {
        if (reader != null) {
          bytesReaded += reader.getTotalBytes();
          reader.close();
        }
        if (tableSession != null) {
          reader = RawTunnelRecordReader
                  .createTableTunnelReader(start + offset, count - offset, option, columnList,
                                           tunnelServiceClient, tableSession);
        }

        if (instanceSession != null) {
          reader = RawTunnelRecordReader
                  .createInstanceTunnelReader(start + offset, count - offset, option, columnList,
                                              tunnelServiceClient, instanceSession);
        }

        return;
      } catch (TunnelException e) {
        if (++retryCount == retryTimes) {
          throw e;
        }
        sleep(DEFAULT_CONNECT_TIMEOUT);
      } catch (IOException e) {
        if (++retryCount == retryTimes) {
          throw e;
        }
        sleep(DEFAULT_CONNECT_TIMEOUT);
      }
    }
  }

  private void sleep(long time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e1) {
      // ignore intentionally
    }
  }
}
