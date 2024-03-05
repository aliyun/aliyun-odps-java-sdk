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

package com.aliyun.odps.mapred.cli;


import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account.AccountProvider;
import com.aliyun.odps.conf.Configuration;

/**
 * Odps配置类，通常来自ODPS Console的设置.
 *
 * 使用ODPS Console提交作业时，ODPS Console的project、endpoint等设置会通过 JVM
 * 系统属性（-D）的方式传给MapReduce主程序，这些属性包括：
 * <UL>
 * <LI>{@value #PROJNAME}
 * <LI>{@value #ENDPOINT}
 * <LI>{@value #ACCESSID}
 * <LI>{@value #ACCESSKEY}
 * <LI>{@value #PROXYHOST}
 * <LI>{@value #PROXYPORT}
 * </UL>
 */
public class OdpsConf extends Configuration {

  private final static String PROJNAME = "odps.project.name";
  private final static String ENDPOINT = "odps.end.point";
  private final static String ACCESSID = "odps.access.id";
  private final static String STS_TOKEN = "odps.sts.token";
  private final static String ACCESSKEY = "odps.access.key";
  private final static String APP_ACCESS_ID = "odps.app.access.id";
  private final static String APP_ACCESS_KEY = "odps.app.access.key";
  private final static String ACCOUNT_PROVIDER = "odps.account.provider";


  private final static String PROXYHOST = "odps.proxy.host";
  private final static String PROXYPORT = "odps.proxy.port";
  private final static String RUNMODE = "odps.runner.mode";

  static {
    Configuration.addDefaultResource("odps-conf.xml");
  }

  public OdpsConf() {
  }

  /**
   * 获取当前project.
   *
   * @return
   */
  public String getProjName() {
    return get(PROJNAME);
  }

  /**
   * 设置当前project.
   *
   * @param projName
   */
  public void setProjName(String projName) {
    set(PROJNAME, projName);
  }

  /**
   * 获取连接ODPS服务的URL地址.
   *
   * @return
   */
  public String getEndpoint() {
    return get(ENDPOINT);
  }

  /**
   * 设置连接ODPS服务的URL地址.
   *
   * @param endpoint
   */
  public void setEndpoint(String endpoint) {
    set(ENDPOINT, endpoint);
  }

  /**
   * 获取aliyun.com用户访问ID.
   *
   * @return
   */
  public String getAccessId() {
    return get(ACCESSID);
  }

  /**
   * 设置aliyun.com用户访问ID.
   *
   * @param accessId
   */
  public void setAccessId(String accessId) {
    set(ACCESSID, accessId);
  }

  /**
   * 获取aliyun.com用户访问Key.
   *
   * @return
   */
  public String getAccessKey() {
    return get(ACCESSKEY);
  }

  /**
   * 设置aliyun.com用户访问Key.
   *
   * @param accessKey
   */
  public void setAccessKey(String accessKey) {
    set(ACCESSKEY, accessKey);
  }

  public String getAppAccessId() {
    return get(APP_ACCESS_ID);
  }

  public void setAppAccessId(String appAccessId) {
    set(APP_ACCESS_ID, appAccessId);
  }

  public String getAppAccessKey() {
    return get(APP_ACCESS_KEY);
  }

  public void setAppAccessKey(String appAccessKey) {
    set(APP_ACCESS_KEY, appAccessKey);
  }

  /**
   * 获取STS token
   * @return STS token
   */
  public String getStsToken() {
    return get(STS_TOKEN);
  }

  /**
   * 设置STS token
   */
  public void setStsToken(String stsToken) {
    set(STS_TOKEN, stsToken);
  }

  /**
   * 获取代理主机地址.
   *
   * @return
   */
  public String getProxyHost() {
    return get(PROXYHOST);
  }

  /**
   * 设置代理主机地址.
   *
   * @param host
   */
  public void setProxyHost(String host) {
    set(PROXYHOST, host);
  }

  /**
   * 获取运行模式.
   *
   * @return
   */
  public String getRunMode() {
    return get(RUNMODE, "remote");
  }

  /**
   * 设置运行模式.
   *
   * @param mode
   */
  public void setRunMode(String mode) {
    set(RUNMODE, mode);
  }

  /**
   * 获取代理端口.
   *
   * @return
   */
  public String getProxyPort() {
    return get(PROXYPORT);
  }

  /**
   * 设置代理端口.
   *
   * @param port
   */
  public void setProxyPort(String port) {
    set(PROXYPORT, port);
  }

  /**
   * 获取帐号类型<br/>
   * 取值范围：{@link com.aliyun.odps.account.Account.AccountProvider#ALIYUN} ,
   * {@link com.aliyun.odps.account.Account.AccountProvider#TAOBAO},
   * {@link com.aliyun.odps.account.Account.AccountProvider#BEARER_TOKEN}
   * {@link com.aliyun.odps.account.Account.AccountProvider#STS}<br/>
   * 默认值： {@link com.aliyun.odps.account.Account.AccountProvider#ALIYUN}
   *
   * @return 帐号类型
   * @throws OdpsException
   */
  public AccountProvider getAccountProvider() throws OdpsException {
    String provider = get(ACCOUNT_PROVIDER);
    AccountProvider result = AccountProvider.ALIYUN;
    if (provider != null) {
      provider = provider.trim().toUpperCase();
      try {
        result = AccountProvider.valueOf(provider);
      } catch (Exception exception) {
        throw new OdpsException("Unsupported account provider:" + provider);
      }
    }
    return result;
  }


}
