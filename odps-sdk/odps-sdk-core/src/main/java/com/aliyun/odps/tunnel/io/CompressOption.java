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

/**
 * 设置压缩算法、压缩级别、压缩策略。 当前只支持deflate算法，压缩级别和策略请参照zlib的定义
 */
public class CompressOption {

  public static enum CompressAlgorithm {
    ODPS_RAW,
    ODPS_ZLIB,
    ODPS_SNAPPY,
    ODPS_LZ4_FRAME,
    ODPS_ARROW_LZ4_FRAME,
    ODPS_ARROW_ZSTD,
  }

  public CompressOption() {
    algorithm = CompressAlgorithm.ODPS_ZLIB;
    level = 1;
    strategy = 0;
  }

  public CompressOption(CompressAlgorithm a, int l, int s) {
    algorithm = a;
    level = l;
    strategy = s;
  }

  public CompressAlgorithm algorithm;

  //因为我们有网络富裕
  //所以默认选用低压缩率，高速度的算法
  public int level; // 0-9, zlibdefault=-1, we use 1 for default
  public int strategy; // 1-4, default=0
}
