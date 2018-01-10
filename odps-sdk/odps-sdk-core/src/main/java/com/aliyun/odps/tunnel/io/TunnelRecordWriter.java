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

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

import java.io.IOException;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamWriter;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * TunnelRecordWriter支持通过Tunnel服务写入数据到ODPS表
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class TunnelRecordWriter extends ProtobufRecordStreamWriter {

  private Connection conn;
  private boolean isClosed;

  /**
   * 构造此类对象
   *
   * @param schema
   *     {@link TableSchema}
   * @param conn
   *     {@link Connection}
   * @throws IOException
   *     Signals that an I/O exception has occurred.
   */
  public TunnelRecordWriter(TableSchema schema, Connection conn)
      throws IOException {
    this(schema, conn, null);
  }

  public TunnelRecordWriter(TableSchema schema, Connection conn,
                            CompressOption option) throws IOException {

    super(schema, conn.getOutputStream(), option);
    this.conn = conn;
    this.isClosed = false;
  }

  @Override
  public void write(Record r) throws IOException{
    if (isClosed) {
      throw new IOException("Writer has been closed.");
    }

    try {
      super.write(r);
    } catch (IOException e) {
      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        TunnelException err = new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID), conn.getInputStream(), resp.getStatus());
        throw new IOException(err.getMessage(), err);
      }
    }
  }

  @Override
  public void close() throws IOException {
    super.close();

    // handle response
    try {
      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        TunnelException err = new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID), conn.getInputStream(), resp.getStatus());
        throw new IOException(err.getMessage(), err);
      }
    } finally {
      conn.disconnect();
      isClosed = true;
    }
  }
}
