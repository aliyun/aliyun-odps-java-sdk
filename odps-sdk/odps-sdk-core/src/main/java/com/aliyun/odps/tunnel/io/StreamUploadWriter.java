package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.proto.XstreamPack.XStreamPack;
import com.google.protobuf.ByteString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

/**
 * Created by yinyue on 15-6-10.
 */
public class StreamUploadWriter {
  private RestClient tunnelServiceClient;
  private String path;
  private MessageDigest messageDigest;
  private HashMap<String, String> params;
  private HashMap<String, String> headers;

  public StreamUploadWriter(
          RestClient tunnelServiceClient,
          String path,
          HashMap<String, String> params,
          HashMap<String, String> headers) {
    this.tunnelServiceClient = tunnelServiceClient;
    this.path = path;
    this.params = params;
    this.headers = headers;
    try {
      messageDigest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * 向ODPS非分区表写入数据,第一次写入会自动load shard
   *
   * @param recordPack
   *        {@link com.aliyun.odps.tunnel.io.StreamRecordPack} 对象
   * @throws TunnelException, IOException
   */
  public void write(StreamRecordPack recordPack)
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
  public void write(PartitionSpec partitionSpec, StreamRecordPack recordPack)
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

  private String generateMD5(byte[] bytes) {
    byte[] digest = messageDigest.digest(bytes);
    StringBuilder sb = new StringBuilder();
    for (byte b : digest) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }
}
