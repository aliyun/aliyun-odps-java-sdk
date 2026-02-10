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

package com.aliyun.odps.sqa.v2;

import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class FallbackInfo {

  @SerializedName("FallbackQuota")
  private final String fallbackQuota;

  // server side prefer boolean as string type
  @SerializedName("Fallback")
  private final String enabled;

  /**
   * 构造一个回退配置信息。
   *
   * @param fallbackQuota 回退到的目标Quota名称，可以为空。
   * @param enabled 是否开启回退功能。
   */
  public FallbackInfo(String fallbackQuota, boolean enabled) {
    this.fallbackQuota = fallbackQuota;
    this.enabled = String.valueOf(enabled);
  }

  /**
   * 一个方便的工厂方法，用于快速创建一个启用的回退配置。
   */
  public static FallbackInfo enable() {
    return new FallbackInfo(null, true);
  }


  public static FallbackInfo enable(String fallbackQuota) {
    return new FallbackInfo(fallbackQuota, true);
  }

  // Getters
  public String getFallbackQuota() {
    return fallbackQuota;
  }
}
