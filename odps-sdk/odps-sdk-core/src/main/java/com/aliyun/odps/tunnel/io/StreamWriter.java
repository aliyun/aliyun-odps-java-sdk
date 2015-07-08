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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;


public class StreamWriter {

  private RestClient tunnelServiceClient;
  private String path;
  private MessageDigest messageDigest;
  private Map<String, String> params;
  private Map<String, String> headers;
  private WritePackResult lastPackResult;

  public StreamWriter(RestClient tunnelServiceClient, String path, HashMap<String, String> params,
                      Map<String, String> headers) {
    this.tunnelServiceClient = tunnelServiceClient;
    this.path = path;
    this.params = params;
    this.headers = headers;
    try {
      this.messageDigest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * 向ODPS hub服务的非分区表写入一个pack
   *
   * @param recordPack
   *     {@link StreamRecordPack} 对象
   * @throws TunnelException, IOexception
   *
   * @return {@link WritePackResult}
   *
   */
  public WritePackResult write(StreamRecordPack recordPack) throws TunnelException, IOException {
    return write(null, recordPack);
  }

  /**
   * 向ODPS hub服务的分区表写入一个pack
   *
   * @param PartitionSpec
   *     {@link PartitionSpec} 对象
   * @param recordPack
   *     {@link StreamRecordPack} 对象
   * @throws TunnelException, IOexception
   *
   * @return {@link WritePackResult}
   *
   */
  public WritePackResult write(PartitionSpec partitionSpec, StreamRecordPack recordPack)
      throws TunnelException, IOException {

    HashMap<String, String> params = new HashMap<String, String>(this.params);
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.CONTENT_ENCODING, "deflate");
    try {
      byte[] bytes = recordPack.getByteArray();
      if (partitionSpec != null && partitionSpec.toString().length() > 0) {
        params.put(TunnelConstants.RES_PARTITION, partitionSpec.toString().replaceAll("'", ""));
      }
      params.put(TunnelConstants.RECORD_COUNT, String.valueOf(recordPack.getRecordCount()));
      headers.put(Headers.CONTENT_MD5, generatorMD5(bytes));
      Response resp = tunnelServiceClient.requestForRawResponse(path, "PUT", params, headers,
                                                                new ByteArrayInputStream(bytes),
                                                                bytes.length);
      if (!resp.isOK()) {
        //TODO exception
        TunnelException ex = new TunnelException(new ByteArrayInputStream(resp.getBody()));
        ex.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      } else {
        ByteArrayInputStream in = new ByteArrayInputStream(resp.getBody());
        loadFromJson(in);
      }
    } catch (TunnelException e) {
      throw e;
    } catch (Exception e) {
      throw new TunnelException(e.getMessage(), e);
    }
    return this.lastPackResult;
  }

  private void loadFromJson(InputStream is) throws TunnelException {
    try {
      ObjectMapper mapper = JacksonParser.getObjectMapper();
      JsonNode tree = mapper.readTree(is);
      JsonNode node = tree.get("PackId");
      if (node != null && !node.isNull()) {
        this.lastPackResult = new WritePackResult(node.asText());
      } else {
        throw new TunnelException("get pack id fail");
      }
    } catch (Exception e) {
      throw new TunnelException("Invalid json content.", e);
    }
  }

  private String generatorMD5(byte[] bytes) {
    byte[] digest = messageDigest.digest(bytes);
    StringBuilder sb = new StringBuilder();
    for (byte b : digest) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }
}

