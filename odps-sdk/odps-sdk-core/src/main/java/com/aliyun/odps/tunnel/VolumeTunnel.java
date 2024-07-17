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

package com.aliyun.odps.tunnel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.VolumeInputStream;
import com.aliyun.odps.tunnel.io.VolumeOutputStream;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 访问ODPS Volume Tunnel服务的入口类
 * <b>暂未开放，仅限内部使用<b/>
 *
 * @author lu.lu@alibaba-inc.com
 */
public class VolumeTunnel {

  private Configuration config;

  /**
   * 构造此类对象
   *
   * @param odps
   *     {@link com.aliyun.odps.Odps}
   */
  public VolumeTunnel(Odps odps) {
    this.config = new Configuration(odps);
  }

  /**
   * 设置TunnelServer地址
   *
   * <p>没有设置TunnelServer地址的情况下, 自动选择</p>
   *
   * @param endpoint
   */
  public void setEndpoint(String endpoint) {
    try {
      URI u = new URI(endpoint);
      config.setEndpoint(u);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid endpoint.");
    }
  }

  /**
   * 构造一个新的{@link com.aliyun.odps.tunnel.VolumeTunnel.UploadSession}对象。
   *
   * @param projectName
   *     上传File所在的project名称
   * @param volumeName
   *     上传File所在的volume名称
   * @param partition
   *     上传File的partition描述，由字母,数字,下划线组成，3-32个字符，举例如下: my_pt_001
   */
  public UploadSession createUploadSession(String projectName, String volumeName, String partition)
      throws TunnelException {
    return new UploadSession(projectName, volumeName, partition);
  }

  /**
   * 根据已有uploadId获取一个{@link com.aliyun.odps.tunnel.VolumeTunnel.UploadSession}对象。
   *
   * @param projectName
   *     上传File所在的project名称
   * @param volumeName
   *     上传File所在的volume名称
   * @param partition
   *     上传File的partition描述，由字母,数字,下划线组成，3-32个字符，举例如下: my_pt_001
   * @param uploadId
   *     Upload的唯一标识符
   */
  public UploadSession getUploadSession(String projectName, String volumeName, String partition,
                                        String uploadId) throws TunnelException {
    return new UploadSession(projectName, volumeName, partition, uploadId);
  }

  /**
   * 构造一个新的{@link com.aliyun.odps.tunnel.VolumeTunnel.DownloadSession}对象。
   *
   * @param projectName
   *     下载File所在project名称
   * @param volumeName
   *     下载File所在volume名称
   * @param partition
   *     下载File的partition描述
   * @param fileName
   *     下载File的名称
   */
  public DownloadSession createDownloadSession(String projectName, String volumeName,
                                               String partition, String fileName)
      throws TunnelException {
    return new DownloadSession(projectName, volumeName, partition, fileName);
  }

  /**
   * 根据已有downloadId获取一个{@link com.aliyun.odps.tunnel.VolumeTunnel.DownloadSession}对象。
   *
   * @param projectName
   *     下载File所在project名称
   * @param volumeName
   *     下载File所在volume名称
   * @param partition
   *     下载File的partition描述
   * @param fileName
   *     下载File的名称
   * @param downloadId
   *     VolumeTunnel.DownloadSession的唯一标识符
   */
  public DownloadSession getDownloadSession(String projectName, String volumeName,
                                            String partition, String fileName, String downloadId)
      throws TunnelException {
    return new DownloadSession(projectName, volumeName, partition, fileName, downloadId);
  }

  /**
   * UploadStatus表示当前Upload的状态
   * <br> UNKNOWN 未知
   * <br> NORMAL 正常
   * <br> CLOSING 关闭中
   * <br> CLOSED 已关闭
   * <br> CANCELED 已取消
   * <br> EXPIRED 已过期
   * <br> CRITICAL 严重错误
   */
  public static enum UploadStatus {
    UNKNOWN, NORMAL, CLOSING, CLOSED, CANCELED, EXPIRED, CRITICAL
  }

  /**
   * UploadSession表示一个向ODPS表中上传数据的会话
   *
   * <p>
   * 向ODPS表上传数据的流程如下: <br />
   * 1) 创建UploadSession<br />
   * 2) 上传数据<br />
   * 3) 提交
   * </p>
   */
  public class UploadSession {

    private String id;
    private String projectName;
    private String volumeName;
    private String partitionSpec;
    private HashMap<String, Long> fileLists = new HashMap<String, Long>();
    private UploadStatus status = UploadStatus.UNKNOWN;
    private String quotaName = "";

    private Configuration conf;

    private RestClient tunnelServiceClient;

    /**
     * 构造一个新的{@link UploadSession}对象。
     *
     * @param projectName
     *     上传File所在的project名称。
     * @param volumeName
     *     上传File所在Volume名称。
     * @param partitionSpec
     *     上传File的partition描述，格式如下: pt=xxx,dt=xxx。
     */
    public UploadSession(String projectName, String volumeName, String partitionSpec)
        throws TunnelException {
      this.conf = VolumeTunnel.this.config;
      this.projectName = projectName;
      this.volumeName = volumeName;
      this.partitionSpec = partitionSpec;

      tunnelServiceClient = conf.newRestClient(projectName);

      initiate();
    }

    /**
     * 根据已有的uploadId构造一个{@link UploadSession}对象。
     *
     * @param projectName
     *     上传File所在的project名称。
     * @param volumeName
     *     上传File所在Volume名称。
     * @param partitionSpec
     *     上传File的partition描述，格式如下: pt=xxx,dt=xxx。
     * @param uploadId
     *     UploadSession的唯一标识符
     */
    public UploadSession(String projectName, String volumeName, String partitionSpec,
                         String uploadId)
        throws TunnelException {
      this.conf = VolumeTunnel.this.config;
      this.projectName = projectName;
      this.volumeName = volumeName;
      this.partitionSpec = partitionSpec;
      this.id = uploadId;

      tunnelServiceClient = conf.newRestClient(projectName);

      reload();
    }

    private void initiate() throws TunnelException {

      HashMap<String, String> headers = new HashMap<String, String>();
      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      HashMap<String, String> params = new HashMap<String, String>();
      params.put(TunnelConstants.TYPE, "volumefile");
      params
          .put(TunnelConstants.TARGET, projectName + "/" + volumeName + "/" + partitionSpec + "/");
      if (this.conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, this.conf.getQuotaName());
      }

      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(getResource(), "POST", params, headers);
        Response resp = conn.getResponse();

        if (resp.isOK()) {
          loadFromJson(conn.getInputStream());
        } else {
          TunnelException e = new TunnelException(conn.getInputStream());
          e.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
          throw e;
        }
      } catch (IOException e) {
        throw new TunnelException("Failed to create upload session with tunnel endpoint "
                                  + tunnelServiceClient.getEndpoint(), e);
      } catch (TunnelException e) {
        throw e;
      } catch (OdpsException e) {
        throw new TunnelException(e.getMessage(), e);
      } finally {
        if (conn != null) {
          try {
            conn.disconnect();
          } catch (IOException e) {
            // nothing
          }
        }
      }
    }

    /**
     * 创建{@link java.io.OutputStream}用来将数据流入到指定File。
     *
     * @param fileName
     *     指定File的名称。
     */
    public OutputStream openOutputStream(String fileName) throws TunnelException, IOException {
      return openOutputStream(fileName, false, false);
    }

    /**
     * 创建{@link java.io.OutputStream}用来将数据流入到指定File。
     *
     * @param fileName
     *     指定File的名称。
     * @param compress
     *     设置压缩参数对传输数据进行压缩
     */
    public OutputStream openOutputStream(String fileName, boolean compress)
        throws TunnelException, IOException {
      return openOutputStream(fileName, compress, false);
    }

    /**
     * 创建{@link java.io.OutputStream}用来将数据流入到指定File。
     *
     * @param fileName
     *     指定File的名称。
     * @param compress
     *     数据传输是否进行压缩
     * @param append
     *     是否采用断点续传
     */
    public OutputStream openOutputStream(String fileName, boolean compress, boolean append)
        throws TunnelException, IOException {
      HashMap<String, String> params = new HashMap<String, String>();
      HashMap<String, String> headers = new HashMap<String, String>();

      headers.put(Headers.CONTENT_TYPE, "text/plain");
      headers.put(Headers.TRANSFER_ENCODING, Headers.CHUNKED);
      headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION,
                  String.valueOf(TunnelConstants.VERSION));

      if (compress) {
        if (conf.getCompressOption().algorithm.equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
          headers.put(HttpHeaders.CONTENT_ENCODING, "deflate");
        } else {
          throw new TunnelException("invalid compression option.");
        }
      }

      fileName = formatFileName(fileName);
      params.put(TunnelConstants.BLOCKID, fileName);
      if (append) {
        params.put(TunnelConstants.RESUME_MODE, null);
      }

      if (conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, conf.getQuotaName());
      }

      VolumeOutputStream vout = null;
      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(getResource() + "/" + id, "POST", params, headers);

        CompressOption option = compress ? conf.getCompressOption() : null;
        vout = new VolumeOutputStream(conn, option);

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

      return vout;
    }

    private void reload() throws TunnelException {
      HashMap<String, String> params = new HashMap<String, String>();
      HashMap<String, String> headers = new HashMap<String, String>();

      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(getResource() + "/" + id, "GET", params, headers);
        Response resp = conn.getResponse();
        if (resp.isOK()) {
          loadFromJson(conn.getInputStream());
        } else {
          TunnelException e = new TunnelException(conn.getInputStream());
          e.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
          throw e;
        }
      } catch (IOException e) {
        throw new TunnelException(e.getMessage(), e);
      } catch (TunnelException e) {
        throw e;
      } catch (OdpsException e) {
        throw new TunnelException(e.getMessage(), e);
      } finally {
        if (conn != null) {
          try {
            conn.disconnect();
          } catch (IOException e) {
            //
          }
        }
      }
    }

    /**
     * 提交本次上传的所有file
     *
     * @param files
     *     已经成功上传的Files列表
     * @throws com.aliyun.odps.tunnel.TunnelException
     *     如果提供的file列表与Server端存在的file不一致抛出异常
     */
    public void commit(String[] files) throws TunnelException, IOException {
      if (files == null) {
        throw new IllegalArgumentException("Invalid argument: files.");
      }

      List<String> formatFiles = new ArrayList<String>();
      for (String fileId : files) {
        formatFiles.add(formatFileName(fileId));
      }

      files = formatFiles.toArray(new String[0]);

      HashMap<String, Boolean> clientFileMap = new HashMap<String, Boolean>();
      for (String fileId : files) {
        clientFileMap.put(fileId, true);
      }

      this.getStatus();

      if (fileLists.size() != clientFileMap.size()) {
        throw new TunnelException(
            "File number not match, server: " + fileLists.size() + ", client: "
            + clientFileMap.size());
      }

      for (String fileId : files) {
        if (!fileLists.containsKey(fileId)) {
          throw new TunnelException("File not exits on server, file name is " + fileId);
        }
      }

      completeUpload();
    }

    private String formatFileName(String fileName) throws TunnelException {
      StringBuilder sb = new StringBuilder();

      if (fileName.length() > 0 && fileName.charAt(0) == '/') {
        throw new TunnelException("FileName cann't start with '/', file name is " + fileName);
      }
      int pos = 0;
      boolean preSlash = false;
      while (pos < fileName.length()) {
        if (fileName.charAt(pos) == '/') {
          if (!preSlash) {
            sb.append(fileName.charAt(pos));
          }
          preSlash = true;
        } else {
          sb.append(fileName.charAt(pos));
          preSlash = false;
        }
        pos++;
      }
      return sb.toString();
    }

    private void completeUpload() throws TunnelException, IOException {

      HashMap<String, String> params = new HashMap<String, String>();
      HashMap<String, String> headers = new HashMap<String, String>();
      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      int count = 0;
      while (true) {
        count++;
        Connection conn = null;
        try {
          conn = tunnelServiceClient.connect(getResource() + "/" + id, "PUT", params, headers);
          Response resp = conn.getResponse();

          if (resp.isOK()) {
            loadFromJson(conn.getInputStream());
            break;
          } else {
            if (resp.getStatus() == HttpURLConnection.HTTP_INTERNAL_ERROR
                && count < 3) {
              try {
                Thread.sleep(2 * count * 1000);
              } catch (InterruptedException e) {
                throw new TunnelException(e.getMessage(), e);
              }
              continue;
            }
            throw new TunnelException(conn.getInputStream());
          }
        } catch (IOException e) {
          throw new TunnelException(e.getMessage(), e);
        } catch (TunnelException e) {
          throw e;
        } catch (OdpsException e) {
          throw new TunnelException(e.getMessage(), e);
        } finally {
          if (conn != null) {
            conn.disconnect();
          }
        }
      }
    }

    /**
     * 获取当前{@link UploadSession}的唯一标识符
     */
    public String getId() {
      return this.id;
    }

    /**
     * 获取当前{@link UploadSession}的{@link Status}
     */
    public UploadStatus getStatus() throws TunnelException {
      reload();
      return this.status;
    }

    /**
     * 获取当前{@link UploadSession}已成功上传的files
     */
    public String[] getFileList() throws TunnelException, IOException {
      reload();
      return fileLists.keySet().toArray(new String[0]);
    }

    /*
     * 获取(@param fileName)在当前{@link UploadSession}已成功上传的长度
     *
     * @param fileName
     *      指定File的名称。
     *
     * @throws TunnelException
     *
     */
    public Long getFileLength(String fileName) throws TunnelException, IOException {
      fileName = formatFileName(fileName);
      reload();
      if (fileLists.containsKey(fileName)) {
        return fileLists.get(fileName);
      } else {
        throw new TunnelException(fileName + " not exists in tunnel server");
      }
    }

    private String getResource() {
      StringBuilder sb = new StringBuilder();
      sb.append("/projects/").append(projectName).append("/tunnel").append("/uploads");
      return sb.toString();
    }

    /*
     * Parse session properties from json stream.
     */
    private void loadFromJson(InputStream is) throws TunnelException {
      try {
        String json = IOUtils.readStreamAsString(is);
        JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

        // session id
        if (tree.has("UploadID")) {
          id = tree.get("UploadID").getAsString();
        }

        // status
        if (tree.has("Status")) {
          status = UploadStatus.valueOf(tree.get("Status").getAsString().toUpperCase());
        }

        // fileList
        fileLists.clear();
        if (tree.has("FileList")) {
          JsonArray node2 = tree.get("FileList").getAsJsonArray();
          for (int i = 0; i < node2.size(); ++i) {
            JsonObject fileNode = node2.get(i).getAsJsonObject();
            String fileName = fileNode.has("FileName") ? fileNode.get("FileName").getAsString() : null;
            Long fileLength = fileNode.has("FileLength") ? fileNode.get("FileLength").getAsLong() : 0L;
            fileLists.put(fileName, fileLength);
          }
        }

        if (tree.has("QuotaName")) {
          quotaName = tree.get("QuotaName").getAsString();
        }
      } catch (Exception e) {
        throw new TunnelException("Invalid json content.", e);
      }
    }

    public String getQuotaName() {
      return quotaName;
    }
  }

  /**
   * 下载会话的状态
   * <br> UNKNOWN 未知
   * <br> NORMAL 正常
   * <br> CLOSED 关闭
   * <br> EXPIRED 过期
   */
  public static enum DownloadStatus {
    UNKNOWN, NORMAL, CLOSED, EXPIRED
  }

  /**
   * 此类表示一个向ODPS中下载Volume的会话
   *
   * <p>
   * 向ODPS下载Volume的流程如下: <br />
   * 1) 创建DownloadSession<br />
   * 2) 下载数据<br />
   * </p>
   */
  public class DownloadSession {

    private String id;
    private String projectName;
    private String volumeName;
    private String partitionSpec;
    private String fileName;
    private long fileLength = (long) -1;
    private DownloadStatus status = DownloadStatus.UNKNOWN;
    private String quotaName;
    private Configuration conf;

    private RestClient tunnelServiceClient;

    /**
     * 构造一个新的{@link DownloadSession}。
     *
     * @param projectName
     *     下载File所在project名称
     * @param volumeName
     *     下载File所在volume名称
     * @param partitionSpec
     *     下载File的partition描述，格式如下: pt=xxx,dt=xxx
     * @param fileName
     *     下载File的名称
     */
    public DownloadSession(String projectName, String volumeName, String partitionSpec,
                           String fileName) throws TunnelException {
      this.conf = VolumeTunnel.this.config;
      this.projectName = projectName;
      this.volumeName = volumeName;
      this.partitionSpec = partitionSpec;
      this.fileName = formatFileName(fileName);

      tunnelServiceClient = conf.newRestClient(projectName);

      initiate();
    }

    /**
     * 根据已有downloadId构造一个{@link DownloadSession}对象。
     *
     * @param projectName
     *     下载File所在project名称
     * @param volumeName
     *     下载File所在volume名称
     * @param partitionSpec
     *     下载File的partition描述，格式如下: pt=xxx,dt=xxx
     * @param fileName
     *     下载File的名称
     * @param downloadId
     *     DownloadSession的唯一标识符
     */
    public DownloadSession(String projectName, String volumeName, String partitionSpec,
                           String fileName, String downloadId)
        throws TunnelException {
      this.conf = VolumeTunnel.this.config;
      this.projectName = projectName;
      this.volumeName = volumeName;
      this.partitionSpec = partitionSpec;
      this.fileName = formatFileName(fileName);
      this.id = downloadId;

      tunnelServiceClient = conf.newRestClient(projectName);

      reload();

      //check if volumeName, partitionName and fileName consistent with downloadId.
      if (!this.volumeName.equalsIgnoreCase(volumeName) ||
          !this.partitionSpec.equalsIgnoreCase(partitionSpec) ||
          !this.fileName.equalsIgnoreCase(fileName)) {
        throw new TunnelException(
            "volumeName, partitionName or fileName doesn't match whith downloadId.");
      }
    }

    // initiate a new volume download session
    private void initiate() throws TunnelException {

      HashMap<String, String> headers = new HashMap<String, String>();
      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      HashMap<String, String> params = new HashMap<String, String>();
      params.put(TunnelConstants.TYPE, "volumefile");
      params.put(TunnelConstants.TARGET,
                 projectName + "/" + volumeName + "/" + partitionSpec + "/" + fileName);
      if (this.conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, this.conf.getQuotaName());
      }

      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(getResource(), "POST", params, headers);
        Response resp = conn.getResponse();
        if (resp.isOK()) {
          loadFromJson(conn.getInputStream());
        } else {
          TunnelException e = new TunnelException(conn.getInputStream());
          e.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
          throw e;
        }
      } catch (IOException e) {
        throw new TunnelException("Failed to create download session with tunnel endpoint "
                                  + tunnelServiceClient.getEndpoint(), e);
      } catch (TunnelException e) {
        throw e;
      } catch (OdpsException e) {
        throw new TunnelException(e.getMessage(), e);
      } finally {
        if (conn != null) {
          try {
            conn.disconnect();
          } catch (IOException e) {
            // nothing
          }
        }
      }
    }

    // reload volume download session properties
    private void reload() throws TunnelException {
      HashMap<String, String> params = new HashMap<String, String>();
      HashMap<String, String> headers = new HashMap<String, String>();

      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(getResource() + "/" + id, "GET", params, headers);
        Response resp = conn.getResponse();

        if (resp.isOK()) {
          loadFromJson(conn.getInputStream());
        } else {
          TunnelException e = new TunnelException(conn.getInputStream());
          e.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
          throw e;
        }
      } catch (IOException e) {
        throw new TunnelException(e.getMessage(), e);
      } catch (TunnelException e) {
        throw e;
      } catch (OdpsException e) {
        throw new TunnelException(e.getMessage(), e);
      } finally {
        if (conn != null) {
          try {
            conn.disconnect();
          } catch (IOException e) {
            //
          }
        }
      }
    }

    /**
     * 创建{@link java.io.InputStream}用来将指定File以数据流的形式输出。
     */
    public InputStream openInputStream() throws TunnelException, IOException {
      return this.openInputStream(0, Long.MAX_VALUE, false);
    }

    /**
     * 创建{@link java.io.InputStream}用来将指定File以数据流的形式输出。
     *
     * @param compress
     *     压缩选项,即使设置了压缩选项，如果server
     *     不支持压缩，传输数据也不会被压缩
     */
    public InputStream openInputStream(boolean compress) throws TunnelException, IOException {
      return this.openInputStream(0, Long.MAX_VALUE, compress);
    }

    /**
     * 创建{@link java.io.InputStream}用来将指定File以数据流的形式输出。
     *
     * @param start
     *     本次要读的起始位置。
     * @param length
     *     本次要读的字节数量。
     */
    public InputStream openInputStream(long start, long length)
        throws TunnelException, IOException {
      return openInputStream(start, length, false);
    }

    /**
     * 创建{@link java.io.InputStream}用来将指定File以数据流的形式输出。
     *
     * @param start
     *     本次要读的起始位置。
     * @param length
     *     本次要读的字节数量。
     * @param compress
     *     压缩选项,即使设置了压缩选项，如果server
     *     不支持压缩，传输数据也不会被压缩
     */
    public InputStream openInputStream(long start, long length, boolean compress)
        throws TunnelException, IOException {

      HashMap<String, String> params = new HashMap<String, String>();
      HashMap<String, String> headers = new HashMap<String, String>();

      headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));

      headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION,
                  String.valueOf(TunnelConstants.VERSION));

      if (compress) {
        if (conf.getCompressOption().algorithm
            .equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
          headers.put(Headers.ACCEPT_ENCODING, "deflate");
        } else {
          throw new TunnelException("invalid compression option.");
        }
      }

      params.put("data", null);

      params.put(TunnelConstants.RANGE, "(" + start + "," + length + ")");

      if (conf.availableQuotaName()) {
        params.put(TunnelConstants.PARAM_QUOTA_NAME, conf.getQuotaName());
      }

      VolumeInputStream vin = null;
      Connection conn = null;
      try {
        conn = tunnelServiceClient.connect(getResource() + "/" + id, "GET", params, headers);
        Response resp = conn.getResponse();
        if (!resp.isOK()) {
          TunnelException err = new TunnelException(conn.getInputStream());
          err.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
          throw new IOException(err);
        }

        String content_encoding = resp.getHeader(Headers.CONTENT_ENCODING);
        if (content_encoding != null) {
          if (resp.getHeader(Headers.CONTENT_ENCODING).equals("deflate")) {
            conf.setCompressOption(new CompressOption(
                CompressOption.CompressAlgorithm.ODPS_ZLIB, -1, 0));
          } else {
            throw new TunnelException("invalid content encoding");
          }
          compress = true;
        } else {
          compress = false;
        }

        CompressOption option = compress ? conf.getCompressOption() : null;
        vin = new VolumeInputStream(conn, option);

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

      return vin;
    }


    /**
     * 获取当前{@link DownloadSession}操作的File的长度。
     */
    public long getFileLength() {
      return this.fileLength;
    }

    /**
     * 获取当前{@link DownloadSession}的唯一标识符
     */
    public String getId() {
      return this.id;
    }

    /**
     * 获取当前{@link DownloadSession}的{@link Status}
     */
    public DownloadStatus getStatus() throws TunnelException, IOException {
      reload();
      return status;
    }

    private String getResource() {
      StringBuilder sb = new StringBuilder();
      sb.append("/projects/").append(this.projectName).append("/tunnel").append("/downloads");
      return sb.toString();
    }

    private void loadFromJson(InputStream is) throws TunnelException {
      try {
        String json = IOUtils.readStreamAsString(is);
        JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

        // session id
        if (tree.has("DownloadID")) {
          id = tree.get("DownloadID").getAsString();
        }

        // status
        if (tree.has("Status")) {
          status = DownloadStatus.valueOf(tree.get("Status").getAsString().toUpperCase());
        }

        // file
        if (tree.has("File")) {
          JsonObject jsonObject = tree.get("File").getAsJsonObject();
          if (jsonObject.has("FileName")) {
            fileName = jsonObject.get("FileName").getAsString();
          }
          if (jsonObject.has("FileLength")) {
            fileLength = jsonObject.get("FileLength").getAsLong();
          }
        }

        // partition
        if (tree.has("Partition")) {
          JsonObject jsonObject = tree.get("Partition").getAsJsonObject();
          if (jsonObject.has("Volume")) {
            volumeName = jsonObject.get("Volume").getAsString();
          }
          if (jsonObject.has("Partition")) {
            partitionSpec = jsonObject.get("Partition").getAsString();
          }
        }

        if (tree.has("QuotaName")) {
          quotaName = tree.get("QuotaName").getAsString();
        }
      } catch (Exception e) {
        throw new TunnelException("Invalid json content.", e);
      }
    }

    private String formatFileName(String fileName) throws TunnelException {
      StringBuilder sb = new StringBuilder();

      if (fileName.length() > 0 && fileName.charAt(0) == '/') {
        throw new TunnelException("FileName cann't start with '/', file name is " + fileName);
      }
      int pos = 0;
      boolean preSlash = false;
      while (pos < fileName.length()) {
        if (fileName.charAt(pos) == '/') {
          if (!preSlash) {
            sb.append(fileName.charAt(pos));
          }
          preSlash = true;
        } else {
          sb.append(fileName.charAt(pos));
          preSlash = false;
        }
        pos++;
      }
      return sb.toString();
    }

    public String getQuotaName() {
      return quotaName;
    }
  }
}
