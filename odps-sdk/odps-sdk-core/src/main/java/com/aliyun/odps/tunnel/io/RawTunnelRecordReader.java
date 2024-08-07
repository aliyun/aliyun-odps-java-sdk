package com.aliyun.odps.tunnel.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamReader;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.*;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Created by zhenhong.gzh on 16/1/5.
 */
public class RawTunnelRecordReader extends ProtobufRecordStreamReader {

  private Connection conn;

  public RawTunnelRecordReader(TableSchema schema, List<Column> columns, Connection conn,
                               CompressOption option) throws IOException {

    super(schema, columns, conn.getInputStream(), option);
    this.conn = conn;
  }

  public RawTunnelRecordReader(List<Column> columns, Connection conn,
                               CompressOption option) throws IOException {
    super(columns, conn.getInputStream(), option);
    this.conn = conn;
  }

  @Override
  public void close() throws IOException {
    super.close();
    conn.disconnect();
  }

  public static RawTunnelRecordReader createInstanceTunnelReader(long start, long count,
                                                                 CompressOption compress,
                                                                 List<Column> columns,
                                                                 RestClient restClient,
                                                                 InstanceTunnel.DownloadSession session,
                                                                 boolean longPolling)
      throws TunnelException, IOException {
    return createInstanceTunnelReader(start, count, 0L, compress, columns, restClient, session, longPolling);
  }

  public static RawTunnelRecordReader createInstanceTunnelReader(long start, long count, long sizeLimit,
                                                                 CompressOption compress,
                                                                 List<Column> columns,
                                                                 RestClient restClient,
                                                                 InstanceTunnel.DownloadSession session,
                                                                 boolean longPolling)
      throws TunnelException, IOException {
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
      case ODPS_LZ4_FRAME: {
        headers.put(Headers.ACCEPT_ENCODING, "x-lz4-frame");
        break;
      }
      default: {
        throw new TunnelException("invalid compression option.");
      }
    }

    Configuration conf = session.getConfig();
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

    if (session.getEnableLimit()) {
      params.put(TunnelConstants.INSTANCE_TUNNEL_LIMIT_ENABLED, null);
    }

    if (!StringUtils.isNullOrEmpty(conf.getQuotaName())) {
      params.put(TunnelConstants.PARAM_QUOTA_NAME, conf.getQuotaName());
    }

    params.put("data", null);

    if (longPolling) {
      // get schema from http stream
      params.put(TunnelConstants.SCHEMA_IN_STREAM, null);
      params.put(TunnelConstants.CACHED, null);
      params.put(TunnelConstants.TASK_NAME, session.getTaskName());
      if (session.getQueryId() != -1) {
        params.put(TunnelConstants.QUERY_ID, String.valueOf(session.getQueryId()));
      }
      if (start < 0) {
        TunnelException err = new TunnelException("The specified row range is not valid. start index is negative.");
        throw err;
      }
      // either count or start is set, ROW_RANGE should be set in HTTP parameters
      if (count > 0 || start > 0) {
        // only start is set
        if (start > 0 && count < 0) {
          params.put(TunnelConstants.ROW_RANGE, "(" + start + "," + Long.MAX_VALUE + ")");
        }
        else {
          // limit mode, otherwise unlimited
          params.put(TunnelConstants.ROW_RANGE, "(" + start + "," + count + ")");
        }
      }
      if(sizeLimit > 0){
        //limit result size if necessary
        params.put(TunnelConstants.SIZE_LIMIT, Long.toString(sizeLimit));
      }
    } else {
      params.put(TunnelConstants.DOWNLOADID, session.getId());
      params.put(TunnelConstants.ROW_RANGE, "(" + start + "," + count + ")");
    }
    Connection conn = null;
    try {
      conn =
          restClient.connect(ResourceBuilder.buildInstanceResource(session.getProjectName(),
              session.getInstanceID()), "GET",
              params, headers);
      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        TunnelException err = new TunnelException(conn.getInputStream());
        err.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw err;
      }

      CompressOption option = null;
      String content_encoding = resp.getHeader(Headers.CONTENT_ENCODING);
      if (content_encoding != null) {
        if (content_encoding.equals("deflate")) {
          option = new CompressOption(CompressOption.CompressAlgorithm.ODPS_ZLIB,
              -1, 0);
        } else if (content_encoding.equals("x-snappy-framed")) {
          option = new CompressOption(CompressOption.CompressAlgorithm.ODPS_SNAPPY,
              -1, 0);
        } else if (content_encoding.equals("x-lz4-frame")) {
          option = new CompressOption(CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME,
                  -1, 0);
        } else {
          throw new TunnelException("invalid content encoding");
        }
      }
      if (longPolling) {
        long recordCount = 0;
        if (resp.getHeaders().containsKey(Headers.TUNNEL_RECORD_COUNT)) {
          recordCount = Long.parseLong(resp.getHeader(Headers.TUNNEL_RECORD_COUNT));
        }
        session.setRecordCount(recordCount);
        // tunnel server do not support schema in stream
        if (resp.getHeaders().containsKey(Headers.TUNNEL_SCHEMA)) {
          String schemaStr = resp.getHeader(Headers.TUNNEL_SCHEMA);
          if (StringUtils.isNullOrEmpty(schemaStr)) {
            throw new TunnelException("Invalid response schema in header:" + schemaStr);
          }
          JsonObject tree = new JsonParser().parse(schemaStr).getAsJsonObject();
          TableSchema schema = new TunnelTableSchema(tree);
          // in direct mode, schema in session is null, we need to set it back
          session.setSchema(schema);
          return new RawTunnelRecordReader(session.getSchema(), columns, conn, option);
        } else {
          RawTunnelRecordReader reader =  new RawTunnelRecordReader(columns, conn, option);
          // in direct mode, schema in session is null, we need to set it back
          session.setSchema(reader.getTableSchema());
          return reader;
        }
      }
      return new RawTunnelRecordReader(session.getSchema(), columns, conn, option);

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
  }

  /**
   * 创建不可靠的 tunnel record reader
   *
   * @param start
   *     本次要读取记录的起始位置
   * @param count
   *     本次要读取记录的数量
   * @param columns
   *     需要读取的列 {@link Column}
   * @param compress
   *     {@link CompressOption}
   * @param session
   *     本次读取所在 session
   * @param restClient
   *     本次请求使用的 tunnelserverclient
   * @throws IOException
   */
  public static RawTunnelRecordReader createTableTunnelReader(long start, long count,
                                                              CompressOption compress,
                                                              List<Column> columns,
                                                              RestClient restClient,
                                                              TableTunnel.DownloadSession session)
      throws IOException, TunnelException {
      return createTableTunnelReader(start, count, compress, columns, restClient, session, false);
  }
  public static RawTunnelRecordReader createTableTunnelReader(long start, long count,
                                                              CompressOption compress,
                                                              List<Column> columns,
                                                              RestClient restClient,
                                                              TableTunnel.DownloadSession session,
                                                              boolean disableModifiedCheck)
      throws IOException, TunnelException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>();

    headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));

    if (compress != null) {
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
        case ODPS_LZ4_FRAME: {
          headers.put(Headers.ACCEPT_ENCODING, "x-lz4-frame");
          break;
        }
        default: {
          throw new TunnelException("invalid compression option.");
        }
      }
    }
    Configuration conf = session.getConfig();
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

      CompressOption option = null;

      String content_encoding = resp.getHeader(Headers.CONTENT_ENCODING);
      if (content_encoding != null) {
        if (content_encoding.equals("deflate")) {
          option = new CompressOption(CompressOption.CompressAlgorithm.ODPS_ZLIB, -1, 0);
        } else if (content_encoding.equals("x-snappy-framed")) {
          option = new CompressOption(CompressOption.CompressAlgorithm.ODPS_SNAPPY, -1, 0);
        } else if (content_encoding.equals("x-lz4-frame")) {
          option = new CompressOption(CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME, -1, 0);
        } else {
          throw new TunnelException("invalid content encoding");
        }
      }

      return new RawTunnelRecordReader(session.getSchema(), columns, conn, option);

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
  }
}
