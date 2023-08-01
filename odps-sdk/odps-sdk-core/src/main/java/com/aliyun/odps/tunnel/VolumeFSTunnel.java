/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aliyun.odps.tunnel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Volume;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request.Method;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.impl.ConfigurationImpl;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.VolumeInputStream;
import com.aliyun.odps.tunnel.io.VolumeOutputStream;

/**
 * Volume FileSystem Tunnel
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFSTunnel {

  private ConfigurationImpl conf;

  private static enum Encoding {
    deflate
  }

  private static final String STREAM_CONTENT_TYPE = "application/octet-stream";

  private static final String REPLICATION = "replication";

  public VolumeFSTunnel(Odps odps) {
    this.conf = new ConfigurationImpl(odps);
  }

  /**
   * Create an InputStream at the indicated Path
   * 
   * @param project
   * @param path
   * @param start The start offset of the target range of a file
   * @param end The end offset of a file
   * @return
   * @throws IOException
   */
  public InputStream openInputStream(String project, String path, Long start, Long end,
      CompressOption compressOption) throws TunnelException {

    checkVolume(path);

    Map<String, String> params = new HashMap<String, String>();
    Map<String, String> headers = new HashMap<String, String>();

    headers.put(HttpHeaders.RANGE, getRangeValue(start, end));
    headers.put(HttpHeaders.HEADER_ODPS_VOLUME_FS_PATH, path);

    if (compressOption != null) {
      if (CompressOption.CompressAlgorithm.ODPS_ZLIB.equals(compressOption.algorithm)) {
        headers.put(Headers.ACCEPT_ENCODING, Encoding.deflate.name());
      } else {
        throw new TunnelException("Not Support compress option:" + compressOption.algorithm.name());
      }
    }
    if (conf.availableQuotaName()) {
      params.put(TunnelConstants.PARAM_QUOTA_NAME, conf.getQuotaName());
    }

    VolumeInputStream in = null;
    Connection conn = null;
    try {
      String resource = ResourceBuilder.buildVolumeResource(project, getVolumeFromPath(path));
      conn = getRestClient(project).connect(resource, Method.GET.name(), params, headers);
      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        TunnelException exception = new TunnelException(conn.getInputStream());
        exception.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw exception;
      }
      boolean compress = false;
      String content_encoding = resp.getHeader(Headers.CONTENT_ENCODING);
      if (content_encoding != null) {
        if (!content_encoding.equals(Encoding.deflate.name())) {
          throw new TunnelException("Not Support compress option:" + content_encoding);
        }
        compress = true;
      }
      CompressOption option = compress ? new CompressOption() : null;
      in = new VolumeInputStream(conn, option);
    } catch (IOException e) {
      closeQuietly(conn);
      unifyException(e);
    } catch (OdpsException e) {
      closeQuietly(conn);
      unifyException(e);
    }
    return in;
  }



  /**
   * Create an OutputStream at the indicated Path
   * 
   * @param project
   * @param path
   * @param replication
   * @param compressOption
   * @return
   * @throws IOException
   */
  public OutputStream openOutputStream(String project, String path, Integer replication,
      CompressOption compressOption) throws TunnelException {

    checkVolume(path);

    Map<String, String> params = new HashMap<String, String>();
    Map<String, String> headers = new HashMap<String, String>();

    if (replication != null && replication > 0) {
      params.put(REPLICATION, replication.toString());
    }

    headers.put(Headers.CONTENT_TYPE, STREAM_CONTENT_TYPE);
    headers.put(Headers.TRANSFER_ENCODING, Headers.CHUNKED);
    headers.put(HttpHeaders.HEADER_ODPS_VOLUME_FS_PATH, path);

    boolean compress = false;
    if (compressOption != null) {
      compress = true;
    }

    if (compress) {
      if (CompressOption.CompressAlgorithm.ODPS_ZLIB.equals(compressOption.algorithm)) {
        headers.put(Headers.ACCEPT_ENCODING, Encoding.deflate.name());
      } else {
        throw new TunnelException("Not Support compress option:" + compressOption.algorithm.name());
      }
    }

    if (conf.availableQuotaName()) {
      params.put(TunnelConstants.PARAM_QUOTA_NAME, conf.getQuotaName());
    }

    VolumeOutputStream out = null;
    Connection conn = null;
    try {

      String resource = ResourceBuilder.buildVolumeResource(project, getVolumeFromPath(path));
      conn = getRestClient(project).connect(resource, Method.POST.name(), params, headers);

      CompressOption option = compress ? compressOption : null;
      out = new VolumeOutputStream(conn, option);

    } catch (IOException e) {
      closeQuietly(conn);
      unifyException(e);
    } catch (OdpsException e) {
      closeQuietly(conn);
      unifyException(e);
    }
    return out;
  }

  /**
   * Commit upload session
   * 
   * @param project
   * @param path
   * @param sessionId
   * @throws TunnelException
   */
  public void commit(String project, String path, String sessionId) throws TunnelException {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(HttpHeaders.HEADER_ODPS_VOLUME_FS_PATH, path);
    headers.put(HttpHeaders.HEADER_ODPS_VOLUME_SESSIONID, sessionId);
    String resource = null;
    try {
      resource = ResourceBuilder.buildVolumeResource(project, getVolumeFromPath(path));
    } catch (IOException e) {
      throw new TunnelException(e.getMessage(), e);
    }
    Connection conn = null;
    try {
      conn = getRestClient(project).connect(resource, Method.PUT.name(), null, headers);
      Response resp = conn.getResponse();
      if (!resp.isOK()) {
        TunnelException exception = new TunnelException(conn.getInputStream());
        exception.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw exception;
      }
    } catch (IOException e) {
      unifyException(e);
    } catch (OdpsException e) {
      unifyException(e);
    }
  }

  /**
   * Get upload sessionId from outputStream
   * <p>
   * You should call it after the outputStream has been closed , or this method will close
   * outputStream for you. So once you call this method , the outputStream will be closed.
   * <p>
   * 
   * @param outputStream
   * @return
   */
  public static String getUploadSessionId(VolumeOutputStream outputStream) throws TunnelException {
    if (outputStream == null) {
      throw new TunnelException("outputStream is null.");
    }
    if (!outputStream.isClosed()) {
      try {
        outputStream.close();
      } catch (IOException e) {
        if (e.getCause() instanceof TunnelException) {
          throw (TunnelException) e.getCause();
        } else {
          throw new TunnelException(e.getMessage(), e);
        }
      }
    }
    Response resp = outputStream.getResp();
    if (resp == null) {
      return null;
    }

    return resp.getHeader(HttpHeaders.HEADER_ODPS_VOLUME_SESSIONID);
  }

  private void unifyException(OdpsException e) throws TunnelException {
    if (e instanceof TunnelException)
      throw (TunnelException) e;
    TunnelException te = new TunnelException();
    OdpsException oe = (OdpsException) e;
    te.setRequestId(oe.getRequestId());
    te.setErrorCode(oe.getErrorCode());
    te.setErrorMsg(oe.getMessage());
    te.setStackTrace(oe.getStackTrace());
    throw te;
  }

  private void unifyException(IOException e) throws TunnelException {
    throw new TunnelException(e.getMessage(), e);
  }


  private void closeQuietly(Connection conn) {
    if (conn != null) {
      try {
        conn.disconnect();
      } catch (IOException ioe) {
      }
    }
  }

  private String getRangeValue(Long start, Long end) {
    return "bytes=" + (start == null ? "" : start) + "-" + (end == null ? "" : end);
  }

  private String getVolumeFromPath(String path) throws IOException {
    if (path == null || path.indexOf("/") == -1) {
      throw new IOException("No volume found!");
    }
    return path.split("/")[1];
  }

  private RestClient getRestClient(String project) {
    RestClient restClient = null;
    try {
      restClient = conf.newRestClient(project);
    } catch (TunnelException e) {
      throw new RuntimeException(e.getErrorMsg(), e);
    }
    return restClient;
  }



  public void setEndpoint(String endpoint) {
    try {
      conf.setEndpoint(new URI(endpoint));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Check path's volume
   * 
   * @param path
   */
  private static void checkVolume(String path) throws TunnelException {
    if (!Volume.checkPathHasVolume(path)) {
      TunnelException te = new TunnelException();
      te.setErrorCode(VolumeFSErrorCode.VolumeMissing);
      te.setErrorMsg("missing volume in path: " + path + "!");
      throw te;
    }
  }

}
