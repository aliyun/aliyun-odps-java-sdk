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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowMessage;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.ArrowUtils;
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;

public class ArrowTunnelRecordReader implements ArrowRecordReader {

  private RestClient tunnelServiceClient;
  private long start = 0L;
  private long count = 0L;
  private List<Column> columnList;
  private TableTunnel.DownloadSession tableSession;

  private boolean isClosed;
  private ArrowHttpInputStream inputStream;
  private Connection connection;
  private BufferAllocator allocator;
  private MessageChannelReader messageReader;
  private Schema arrowSchema;
  private CompressOption compression;
  private ArrowStreamReader arrowStreamReader;

  public ArrowTunnelRecordReader(long start,
                                 long count,
                                 List<Column> columns,
                                 RestClient tunnelRestClient,
                                 TableTunnel.DownloadSession session,
                                 BufferAllocator allocator,
                                 CompressOption compress)
      throws TunnelException, IOException {
    this(start, count, columns, tunnelRestClient, session, allocator, compress, false);
  }

  public ArrowTunnelRecordReader(long start,
                                 long count,
                                 List<Column> columns,
                                 RestClient tunnelRestClient,
                                 TableTunnel.DownloadSession session,
                                 BufferAllocator allocator,
                                 CompressOption compress,
                                 boolean disableModifiedCheck)
      throws TunnelException, IOException {
    this.start = start;
    this.count = count;
    this.columnList = columns;
    if (allocator == null) {
      this.allocator = new RootAllocator(Long.MAX_VALUE);
    } else {
      this.allocator = allocator;
    }
    this.tunnelServiceClient = tunnelRestClient;
    this.tableSession = session;
    this.isClosed = false;
    this.compression = compress;

    if (session.isUseMaxStorage()) {
      if (count != 1) {
        throw new IllegalArgumentException(
            "'count' must be 1 when download Delta Table, and 'start' means the splitId. ");
      }
      if (columns != null && !columns.isEmpty()) {
        throw new IllegalArgumentException(
            "Not support specific 'columns' when download Delta Table. ");
      }
      openMaxStorageReaderConnection(start);
      if (compress.algorithm == CompressOption.CompressAlgorithm.ODPS_RAW) {
        arrowStreamReader = new ArrowStreamReader(this.connection.getInputStream(), this.allocator);
      } else {
        arrowStreamReader =
            new ArrowStreamReader(this.connection.getInputStream(), this.allocator,
                                  CommonsCompressionFactory.INSTANCE);
      }
    } else {
      this.arrowSchema = ArrowUtils.tableSchemaToArrowSchema(session.getSchema(), columns);
      openReaderConnection(this.start, this.count, this.columnList, this.tunnelServiceClient,
                           this.tableSession, disableModifiedCheck);
    }
  }

  private ArrowRecordBatch readBatch() throws IOException {
    if (isClosed) {
      throw new IOException("Arrow reader is closed");
    }
    if (this.inputStream == null) {
      this.inputStream = new ArrowHttpInputStream(this.connection.getInputStream(), compression);
      this.messageReader = new MessageChannelReader(new ReadChannel(this.inputStream), allocator);
    }
    ArrowMessage deserializeMessageBatch = MessageSerializer.deserializeMessageBatch(messageReader);
    return deserializeMessageBatch == null ? null : (ArrowRecordBatch) deserializeMessageBatch;
  }

  @Override
  public VectorSchemaRoot read() throws IOException {
    if (tableSession.isUseMaxStorage()) {
      boolean hasNext = arrowStreamReader.loadNextBatch();
      if (hasNext) {
        return arrowStreamReader.getVectorSchemaRoot();
      } else {
        return null;
      }
    } else {
      List<FieldVector> vectors = new ArrayList<>();
      for (Field field : this.arrowSchema.getFields()) {
        vectors.add(field.createVector(allocator));
      }
      VectorSchemaRoot root = new VectorSchemaRoot(this.arrowSchema, vectors, 0);
      VectorLoader loader = new VectorLoader(root);
      ArrowRecordBatch recordBatch;
      if ((recordBatch = this.readBatch()) != null) {
        loader.load(recordBatch);
        recordBatch.close();
      } else {
        return null;
      }
      return root;
    }
  }

  @Override
  public long bytesRead() {
    if (tableSession.isUseMaxStorage()) {
      return arrowStreamReader.bytesRead();
    } else {
      if (messageReader != null) {
        return messageReader.bytesRead();
      }
    }
    return 0L;
  }

  @Override
  public void close() throws IOException {
    if (!isClosed) {
      if (inputStream != null) {
        inputStream.close();
      }
      connection.disconnect();
      if (arrowStreamReader != null) {
        arrowStreamReader.close();
      }
      isClosed = true;
    }
  }

  private void openReaderConnection(long start, long count, List<Column> columns,
                                    RestClient restClient,
                                    TableTunnel.DownloadSession session,
                                    boolean disableModifiedCheck)
      throws IOException, TunnelException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>();

    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));

    switch (compression.algorithm) {
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
      case ODPS_ARROW_LZ4_FRAME: {
        headers.put(Headers.ACCEPT_ENCODING, "x-odps-lz4-frame");
        break;
      }
      default: {
        throw new TunnelException("invalid compression option.");
      }
    }
    Configuration conf = tableSession.getConfig();
    List<String> tags = conf.getTags();
    if (tags != null) {
      headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
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

    params.put(TunnelConstants.DOWNLOADID, session.getId());
    params.put("data", null);

    params.put(TunnelConstants.ROW_RANGE, "(" + start + "," + count + ")");

    String partitionSpec = session.getPartitionSpec();
    if (partitionSpec != null && partitionSpec.length() > 0) {
      params.put(TunnelConstants.RES_PARTITION, partitionSpec);
    }

    params.put(TunnelConstants.PARAM_ARROW, "");

    if (!StringUtils.isNullOrEmpty(conf.getQuotaName())) {
      params.put(TunnelConstants.PARAM_QUOTA_NAME, conf.getQuotaName());
    }

    if (disableModifiedCheck) {
      params.put(TunnelConstants.PARAM_DISABLE_MODIFIED_CHECK, "true");
    }

    Connection conn = null;
    try {
      String resource = ResourceBuilder.buildTableResource(
          session.getProjectName(), session.getSchemaName(), session.getTableName());
      conn = restClient.connect(resource, "GET", params, headers);

      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        TunnelException err = new TunnelException(conn.getInputStream());
        err.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw err;
      }
      CompressOption reply_compression = null;
      String content_encoding = resp.getHeader(Headers.CONTENT_ENCODING);
      if (content_encoding != null) {
        if (content_encoding.equals("deflate")) {
          reply_compression = new CompressOption(CompressOption.CompressAlgorithm.ODPS_ZLIB,
                                                 -1, 0);
        } else if (content_encoding.equals("x-snappy-framed")) {
          reply_compression = new CompressOption(CompressOption.CompressAlgorithm.ODPS_SNAPPY,
                                                 -1, 0);
        } else if (content_encoding.equals("x-odps-lz4-frame")) {
          reply_compression =
              new CompressOption(CompressOption.CompressAlgorithm.ODPS_ARROW_LZ4_FRAME,
                                 -1, 0);
        } else {
          throw new TunnelException("invalid content encoding");
        }
      }
      // update to match server's selection of compress algorithm
      this.compression = reply_compression;
      this.connection = conn;
    } catch (IOException e) {
      if (connection != null) {
        connection.disconnect();
      }
      throw new TunnelException(e.getMessage(), e);
    } catch (TunnelException e) {
      throw e;
    } catch (OdpsException e) {
      if (connection != null) {
        connection.disconnect();
      }
      throw new TunnelException(e.getMessage(), e);
    }
  }

  private void openMaxStorageReaderConnection(long splitId) throws TunnelException {
    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));
    Configuration conf = tableSession.getConfig();
    List<String> tags = conf.getTags();
    if (tags != null) {
      headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
    }
    switch (compression.algorithm) {
      case ODPS_RAW: {
        break;
      }
      case ODPS_ARROW_ZSTD: {
        headers.put(Headers.ACCEPT_ENCODING, "ZSTD");
        break;
      }
      case ODPS_ARROW_LZ4_FRAME: {
        headers.put(Headers.ACCEPT_ENCODING, "LZ4_FRAME");
        break;
      }
      default: {
        throw new TunnelException("invalid compression option.");
      }
    }

    Map<String, String> params = new HashMap<>();
    if (StringUtils.isNotBlank(conf.getQuotaName())) {
      params.put(TunnelConstants.PARAM_QUOTA_NAME, conf.getQuotaName());
    }

    params.put(ConfigConstants.SESSION_ID, tableSession.getId());
    params.put(ConfigConstants.SPLIT_INDEX, String.valueOf(splitId));

    // hard code
    params.put(ConfigConstants.MAX_BATCH_ROWS, "4096");
    params.put(ConfigConstants.DATA_FORMAT_TYPE, "ARROW");
    params.put(ConfigConstants.DATA_FORMAT_VERSION, "V5");

    Connection conn;
    try {
      String resource = ResourceBuilder.buildTableDataResource(
          ConfigConstants.VERSION_1,
          tableSession.getProjectName(),
          tableSession.getSchemaName(),
          tableSession.getTableName());

      conn = tunnelServiceClient.connect(resource, "GET", params, headers);

      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        TunnelException err = new TunnelException(conn.getInputStream());
        err.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw err;
      }
      this.connection = conn;
    } catch (IOException | OdpsException e) {
      if (connection != null) {
        try {
          connection.disconnect();
        } catch (IOException ignored) {
        }
      }
      throw new TunnelException(e.getMessage(), e);
    }
  }
}
