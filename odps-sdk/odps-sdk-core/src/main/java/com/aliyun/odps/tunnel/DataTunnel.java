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

import java.net.URI;
import java.net.URISyntaxException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;

/**
 * 访问ODPS Tunnel服务的入口类
 *
 * @author chao.liu
 * @author shenggong.wang@alibaba-inc.com
 *         可以使用 {@link TableTunnel} 替代
 */
@Deprecated
public class DataTunnel {

  private Configuration config;

  /**
   * 构造此类对象
   *
   * @param odps
   *     {@link Odps}
   */
  public DataTunnel(Odps odps) {
    this.config = new Configuration(odps);
  }

  /**
   * 在非分区表上创建上传会话
   *
   * @param projectName
   *     Project名称
   * @param tableName
   *     表名，非视图
   * @return {@link UploadSession}
   * @throws TunnelException
   */
  public UploadSession createUploadSession(String projectName, String tableName)
      throws TunnelException {
    return new UploadSession(config, projectName, tableName, null);
  }

  /**
   * 在分区表上创建上传会话
   *
   * <p>
   * 注: 分区必须为最末级分区,如表有两级分区pt,ds, 则必须全部指定值, 不支持只指定其中一个值
   * </p>
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     指定分区 {@link PartitionSpec}
   * @return {@link UploadSession}
   * @throws TunnelException
   */
  public UploadSession createUploadSession(String projectName,
                                           String tableName, PartitionSpec partitionSpec)
      throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException(
          "Invalid arguments, partition spec required.");
    }
    return new UploadSession(config, projectName, tableName,
                             partitionSpec.toString().replaceAll("'", ""));
  }

  /**
   * 获得在非分区表上创建的上传会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param id
   *     上传会话的ID {@link UploadSession#getId()}
   * @return {@link UploadSession}
   * @throws TunnelException
   */
  public UploadSession getUploadSession(String projectName, String tableName,
                                        String id) throws TunnelException {
    return new UploadSession(config, projectName, tableName, null, id);
  }

  /**
   * 获得在分区表上创建的上传会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     上传数据表的partition描述 {@link PartitionSpec}
   * @param id
   *     上传会话ID  {@link UploadSession#getId()}
   * @return {@link UploadSession}
   * @throws TunnelException
   */
  public UploadSession getUploadSession(String projectName, String tableName,
                                        PartitionSpec partitionSpec, String id)
      throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException(
          "Invalid arguments, partition spec required.");
    }
    return new UploadSession(config, projectName, tableName,
                             partitionSpec.toString().replaceAll("'", ""), id);
  }

  /**
   * 在非分区表上创建下载会话
   *
   * @param projectName
   *     Project名称
   * @param tableName
   *     表名，非视图
   * @return {@link DownloadSession}
   * @throws TunnelException
   */
  public DownloadSession createDownloadSession(String projectName,
                                               String tableName) throws TunnelException {
    return new DownloadSession(config, projectName, tableName, null);
  }

  /**
   * 在分区表上创建下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     指定分区 {@link PartitionSpec}
   * @return {@link DownloadSession}
   * @throws TunnelException
   */
  public DownloadSession createDownloadSession(String projectName,
                                               String tableName, PartitionSpec partitionSpec)
      throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException(
          "Invalid arguments, partition spec required.");
    }
    return new DownloadSession(config, projectName, tableName,
                               partitionSpec.toString().replaceAll("'", ""));
  }

  /**
   * 获得在非分区表上创建的下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param id
   *     下载会话ID  {@link DownloadSession#getId()}
   * @return {@link DownloadSession}
   * @throws TunnelException
   */
  public DownloadSession getDownloadSession(String projectName,
                                            String tableName, String id) throws TunnelException {
    return new DownloadSession(config, projectName, tableName, null, id);
  }

  /**
   * 获得在分区表上创建的下载会话
   *
   * @param projectName
   *     Project名
   * @param tableName
   *     表名，非视图
   * @param partitionSpec
   *     指定分区 {@link PartitionSpec}
   * @param id
   *     下载会话ID  {@link DownloadSession#getId()}
   * @return {@link DownloadSession}
   * @throws TunnelException
   */
  public DownloadSession getDownloadSession(String projectName,
                                            String tableName, PartitionSpec partitionSpec,
                                            String id)
      throws TunnelException {
    if (partitionSpec == null || partitionSpec.keys().size() == 0) {
      throw new IllegalArgumentException(
          "Invalid arguments, partition spec required.");
    }
    return new DownloadSession(config, projectName, tableName,
                               partitionSpec.toString().replaceAll("'", ""), id);
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
}
