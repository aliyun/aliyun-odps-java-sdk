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
package com.aliyun.odps.fs;

import org.apache.hadoop.fs.CommonConfigurationKeys;

/**
 * This class contains constants for configuration keys used in the Volume file system
 *
 *
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFileSystemConfigKeys extends CommonConfigurationKeys {

  public static final String VOLUME_URI_SCHEME = "odps";
  public static final String ODPS_SERVICE_ENDPOINT = "odps.service.endpoint";
  public static final String ODPS_TUNNEL_ENDPOINT = "odps.tunnel.endpoint";
  public static final String ODPS_ACCESS_ID = "odps.access.id";
  public static final String ODPS_ACCESS_KEY = "odps.access.key";
  public static final String ODPS_HOME_VOLMUE = "odps.home.volume";
  public static final String ODPS_VOLUME_BLOCK_SIZE = "odps.volume.block.size";
  public static final String ODPS_VOLUME_BLOCK_BUFFER_DIR = "odps.volume.block.buffer.dir";
  public static final String ODPS_VOLUME_TRANSFER_COMPRESS_ENABLED =
      "odps.volume.transfer.compress.enabled";
  public static final String ODPS_VOLUME_TRANSFER_COMPRESS_ALGORITHM =
      "odps.volume.transfer.compress.algorithm";
  public static final String ODPS_VOLUME_SEEK_OPTIMIZATION_ENABLED =
      "odps.volume.seek.optimization.enabled";
  public static final String DFS_REPLICATION_KEY = "dfs.replication";
}
