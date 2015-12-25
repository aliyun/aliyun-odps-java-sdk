package com.aliyun.odps.tunnel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.commons.proto.XstreamPack.XStreamPack;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.datahub.DatahubRecordPack;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TunnelTableSchema;
import com.google.protobuf.ByteString;

/**
 * Created by yinyue on 15-6-10.
 */
public class StreamUploadWriter {
  private RestClient tunnelServiceClient;
  private String path;
  private MessageDigest messageDigest;
  private HashMap<String, String> params;
  private HashMap<String, String> headers;
  private TunnelTableSchema schema;

  public StreamUploadWriter(RestClient tunnelServiceClient, String path) throws TunnelException {
    this.path = path;
    this.tunnelServiceClient = tunnelServiceClient;
    try {
      messageDigest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage());
    }
    this.headers = new HashMap<String, String>();
    this.headers.put(Headers.CONTENT_LENGTH, "0");
    this.headers.put(HttpHeaders.HEADER_STREAM_VERSION, "1");
    this.params = new HashMap<String, String>();
    initiate();

    this.headers.put(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
    this.headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION,
                     String.valueOf(TunnelConstants.VERSION));
  }

  public StreamUploadWriter(
          RestClient tunnelServiceClient,
          String path,
          HashMap<String, String> params,
          HashMap<String, String> headers,
          TunnelTableSchema schema) {
    this.tunnelServiceClient = tunnelServiceClient;
    this.path = path;
    this.params = params;
    this.headers = headers;
    this.schema = schema;
    try {
      messageDigest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  private void initiate() throws TunnelException {
    HashMap<String, String> param = new HashMap<String, String>();
    param.put("query", "meta");

    Connection conn = null;
    try {
      conn = tunnelServiceClient.connect(path, "GET", param, headers);
      Response resp = conn.getResponse();

      if (resp.isOK()) {
        loadFromJson(conn.getInputStream());
      } else {
        TunnelException ex = new TunnelException(conn.getInputStream());
        ex.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      }
    } catch (TunnelException e) {
      throw e;
    } catch (OdpsException e) {
      throw new TunnelException(e.getMessage(), e);
    } catch (IOException e) {
      throw new TunnelException(e.getMessage(), e);
    } finally {
      if (null != conn) {
        try {
          conn.disconnect();
        } catch (IOException ignored) {
        }
      }
    }
  }

  private void loadFromJson(InputStream is)
    throws TunnelException {
    try {
      ObjectMapper mapper = JacksonParser.getObjectMapper();
      JsonNode tree = mapper.readTree(is);

      JsonNode node = tree.get("Schema");
      if (null != node && !node.isNull()) {
        schema = new TunnelTableSchema(node);
      } else {
        throw new TunnelException("Get table type failed");
      }

    } catch (Exception e) {
      throw new TunnelException("Invalid json content.", e);
    }
  }

  /**
   * 向ODPS非分区表写入数据,第一次写入会自动load shard
   *
   * @param recordPack
   *        {@link com.aliyun.odps.tunnel.io.StreamRecordPack} 对象
   * @throws TunnelException, IOException
   */
  public void write(DatahubRecordPack recordPack)
    throws TunnelException, IOException {
    write(null, recordPack);
  }
  
  /**
   * 向ODPS分区表写入数据,第一次写入会自动load shard
   *
   * @param partitionSpec
   *        {@link com.aliyun.odps.PartitionSpec} 对象
   * @param recordPack
   *        {@link com.aliyun.odps.tunnel.io.StreamRecordPack} 对象
   * @throws TunnelException, IOException
   */
  public void write(PartitionSpec partitionSpec, DatahubRecordPack recordPack)
    throws TunnelException, IOException {
    HashMap<String, String> params = new HashMap<String, String>(this.params);
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.CONTENT_ENCODING, "deflate");

    try {
      byte[] bytes = recordPack.getByteArray();

      XStreamPack.Builder pack = XStreamPack.newBuilder();
      pack.setPackData(ByteString.copyFrom(bytes));
      
      bytes = pack.build().toByteArray();
      
      if (null != partitionSpec && partitionSpec.toString().length() > 0) {
        params.put(TunnelConstants.RES_PARTITION, partitionSpec.toString().replace("'", ""));
      }
      params.put(TunnelConstants.RECORD_COUNT, String.valueOf(recordPack.getRecordCount()));
      params.put(TunnelConstants.MODE, TunnelConstants.STREAM_UPLOAD);
      headers.put(HttpHeaders.CONTENT_MD5, generateMD5(bytes));
      Response resp = tunnelServiceClient.requestForRawResponse(path, "PUT", params, headers,
                                          new ByteArrayInputStream(bytes), bytes.length);

      if (!resp.isOK()) {
        TunnelException ex = new TunnelException(new ByteArrayInputStream(resp.getBody()));
        ex.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      }
    } catch (TunnelException e) {
      throw e;
    } catch (Exception e) {
      throw new TunnelException(e.getMessage(), e);
    }
  }

  public TunnelTableSchema getSchema() {
    return schema;
  }

  private String generateMD5(byte[] bytes) {
    byte[] digest = messageDigest.digest(bytes);
    StringBuilder sb = new StringBuilder();
    for (byte b : digest) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }
}
