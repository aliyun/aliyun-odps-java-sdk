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

package com.aliyun.odps.datahub;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.commons.proto.XstreamPack.XStreamPack;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.rest.RestClient;
import com.google.protobuf.ByteString;


public class DatahubWriter {

  private RestClient datahubServiceClient;
  private String path;
  private MessageDigest messageDigest;
  private Map<String, String> params;
  private Map<String, String> headers;
  private WritePackResult lastPackResult;

  public DatahubWriter(RestClient datahubServiceClient, String path, HashMap<String, String> params,
                       Map<String, String> headers) {
    this.datahubServiceClient = datahubServiceClient;
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
   *     {@link DatahubRecordPack} 对象
   * @throws OdpsException, IOexception
   *
   * @return {@link WritePackResult}
   *
   */
  public WritePackResult write(DatahubRecordPack recordPack) throws OdpsException, IOException {
    return write(null, recordPack, null);
  }

  /**
   * 向ODPS hub服务的非分区表写入一个pack
   *
   * @param recordPack
   *     {@link DatahubRecordPack} 对象
   * @param meta
   *     pack 属性
   * @throws OdpsException, IOexception
   *
   * @return {@link WritePackResult}
   *
   */
  public WritePackResult write(DatahubRecordPack recordPack, byte [] meta) throws OdpsException, IOException {
    return write(null, recordPack, meta);
  }

  /**
   * 向ODPS hub服务的分区表写入一个pack
   *
   * @param partitionSpec
   *     {@link com.aliyun.odps.PartitionSpec} 对象
   * @param recordPack
   *     {@link DatahubRecordPack} 对象
   * @throws OdpsException, IOexception
   *
   * @return {@link WritePackResult}
   *
   */
  public WritePackResult write(PartitionSpec partitionSpec, DatahubRecordPack recordPack)
      throws OdpsException, IOException {
    return write(partitionSpec, recordPack, null);
  }

  /**
   * 向ODPS hub服务的分区表写入一个pack
   *
   * @param partitionSpec
   *     {@link com.aliyun.odps.PartitionSpec} 对象
   * @param recordPack
   *     {@link DatahubRecordPack} 对象
   * @param meta
   *     pack 属性
   * @throws OdpsException, IOexception
   *
   * @return {@link WritePackResult}
   *
   */
  public WritePackResult write(PartitionSpec partitionSpec, DatahubRecordPack recordPack, byte [] meta)
      throws OdpsException, IOException {

    HashMap<String, String> params = new HashMap<String, String>(this.params);
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(DatahubHttpHeaders.CONTENT_ENCODING, "deflate");
    try {
      byte[] bytes = recordPack.getByteArray();
      
      if ((null == bytes || 0 == bytes.length) && null == meta) {
        throw new DatahubException("both record pack and meta are empty.");
      }

      XStreamPack.Builder pack = XStreamPack.newBuilder();
      pack.setPackData(ByteString.copyFrom(bytes));
      if (null != meta) {
        pack.setPackMeta(ByteString.copyFrom(meta));
      }
      
      bytes = pack.build().toByteArray();
      
      if (partitionSpec != null && partitionSpec.toString().length() > 0) {
        params.put(DatahubConstants.RES_PARTITION, partitionSpec.toString().replaceAll("'", ""));
      }
      
      params.put(DatahubConstants.RECORD_COUNT, String.valueOf(recordPack.getRecordCount()));
      headers.put(Headers.CONTENT_MD5, generatorMD5(bytes));
      Response resp = datahubServiceClient.requestForRawResponse(path, "PUT", params, headers,
                                                                new ByteArrayInputStream(bytes),
                                                                bytes.length);
      if (!resp.isOK()) {
        //TODO exception
        DatahubException ex = new DatahubException(new ByteArrayInputStream(resp.getBody()));
        ex.setRequestId(resp.getHeader(DatahubHttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      } else {
        ByteArrayInputStream in = new ByteArrayInputStream(resp.getBody());
        loadFromJson(in);
      }
    } catch (DatahubException e) {
      throw e;
    } catch (Exception e) {
      throw new DatahubException(e.getMessage(), e);
    }
    return this.lastPackResult;
  }

  private void loadFromJson(InputStream is) throws OdpsException {
    try {
      ObjectMapper mapper = JacksonParser.getObjectMapper();
      JsonNode tree = mapper.readTree(is);
      JsonNode node = tree.get("PackId");
      if (node != null && !node.isNull()) {
        this.lastPackResult = new WritePackResult(node.asText());
      } else {
        throw new DatahubException("get pack id fail");
      }
    } catch (Exception e) {
      throw new DatahubException("Invalid json content.", e);
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

