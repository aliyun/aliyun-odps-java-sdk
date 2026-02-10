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
public class MaxQAConnInfo {

  @SerializedName("quotaNickName")
  private String quotaName;

  @SerializedName("connInfo")
  private String connInfo;

  @SerializedName("regionId")
  private String regionId;

  private FallbackInfo fallbackInfo;

  private MaxQAConnInfo(Builder builder) {
    this.connInfo = builder.connInfo;
    this.quotaName = builder.quotaName;
    this.regionId = builder.regionId;
    this.fallbackInfo = builder.fallbackInfo;
  }

  public static Builder builder() {
    return new Builder();
  }

  // Getters for all fields
  public String getConnInfo() {
    return connInfo;
  }

  public String getQuotaName() {
    return quotaName;
  }

  public String getRegionId() {
    return regionId;
  }

  public void setFallbackInfo(FallbackInfo fallbackInfo) {
    this.fallbackInfo = fallbackInfo;
  }

  public FallbackInfo getFallbackInfo() {
    return fallbackInfo;
  }

  public static class Builder {

    private String quotaName;
    private String connInfo;
    private String regionId;
    private FallbackInfo fallbackInfo;

    /**
     * Builder 的构造函数，接收所有必填字段。
     */
    public Builder() {
    }

    public Builder quotaName(String quotaName) {
      this.quotaName = quotaName;
      return this;
    }

    public Builder connInfo(String connInfo) {
      this.connInfo = connInfo;
      return this;
    }

    public Builder regionId(String regionId) {
      this.regionId = regionId;
      return this;
    }

    public Builder fallbackInfo(FallbackInfo fallbackInfo) {
      this.fallbackInfo = fallbackInfo;
      return this;
    }

    public MaxQAConnInfo build() {
      return new MaxQAConnInfo(this);
    }
  }
}
