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

/**
 * TunnelReaderReader支持通过Tunnel服务读取ODPS表中的数据
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class TunnelRecordReader extends ProtobufRecordStreamReader {

  private Connection conn;

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
  public TunnelRecordReader(TableSchema schema, Connection conn,
                            CompressOption option) throws IOException {

    super(schema, conn.getInputStream(), option);
    this.conn = conn;
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
  public TunnelRecordReader(TableSchema schema, List<Column> columns, Connection conn,
                            CompressOption option) throws IOException {

    super(schema, columns, conn.getInputStream(), option);
    this.conn = conn;
  }

  @Override
  public void close() throws IOException {
    super.close();
    conn.disconnect();
  }
}
