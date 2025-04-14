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

package com.aliyun.odps.account;

/**
 * 阿里云认证账号
 *
 * <p>
 * accessId/accessKey是阿里云用户的身份标识和认证密钥,请至http://www.aliyun.com查询。
 * </p>
 */
public class ApsaraAccount implements Account {

  private String accessId;
  private String accessKey;

  private ApsaraRequestSigner signer;

  /**
   * 构造AliyunAccount对象
   *
   * @param accessId
   *     AccessId
   * @param accessKey
   *     AccessKey
   */
  public ApsaraAccount(String accessId, String accessKey) {
    this(accessId, accessKey, null);
  }

  /**
   * 构造AliyunAccount对象
   *
   * @param accessId  AccessId
   * @param accessKey AccessKey
   * @param region region name
   */
  public ApsaraAccount(String accessId, String accessKey, String region) {
    this.accessId = accessId;
    this.accessKey = accessKey;

    signer = new ApsaraRequestSigner(accessId, accessKey, region);
  }

  /**
   * 获取当前帐号AccessID
   *
   * @return 当前帐号AccessID
   */
  public String getAccessId() {
    return accessId;
  }

  /**
   * 获取当前帐号AccessKey
   *
   * @return 当前帐号AccessKey
   */
  public String getAccessKey() {
    return accessKey;
  }

  /**
   * 设置region
   * 可用于升级验签方式为v4签名
   * @param region
   */
  public void setRegion(String region) {
    signer.setRegionName(region);
  }

  @Override
  public AccountProvider getType() {
    return AccountProvider.APSARA;
  }

  @Override
  public RequestSigner getRequestSigner() {
    return signer;
  }
}
