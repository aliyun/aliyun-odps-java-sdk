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
package com.aliyun.odps.volume.protocol;

/**
 * Volume Constants
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFSConstants {

  protected VolumeFSConstants() {}

  /*
   * Constants
   */
  public static final String SEPARATOR = "/";
  public static final String ROOT_PATH = "/";
  public static final String SCHEME_SEPARATOR = "://";
  public static final String PARAM_META = "meta";
  public static final String PARAM_PATH = "path";
  public static final int EOF = -1;
  public static final String ODPS_ZLIB = "ODPS_ZLIB";
  public static final String CREATE_VOLUME_COMMENT =
      "This volume created by VolumeClient automatically";
  public static final String VOLUME_FS_CONFIG_FILE = "volumefs-site.xml";

  /*
   * Default value
   */
  public static final long DEFAULT_VOLUME_BLOCK_SIZE = 512 * 1024;
  public static final String DEFAULT_HOME_VOLUME = "user";
  public static final String DEFAULT_VOLUME_BLOCK_BUFFER_DIR = "/tmp/volumefs/";
  public static final short DFS_REPLICATION_DEFAULT = 3;

}
