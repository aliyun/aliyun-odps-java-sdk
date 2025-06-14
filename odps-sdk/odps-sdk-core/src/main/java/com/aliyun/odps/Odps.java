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

package com.aliyun.odps;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AccountFormat;
import com.aliyun.odps.account.AppAccount;
import com.aliyun.odps.account.AppStsAccount;
import com.aliyun.odps.commons.transport.DefaultTransport;
import com.aliyun.odps.ml.OfflineModels;
import com.aliyun.odps.options.OdpsOptions;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TableTunnel;

/**
 * Odps类是ODPS SDK的入口
 *
 * <p>
 * <strong>构造Odps对象:</strong><br />
 *
 * <pre>
 * <code>
 * Account account = new AliyunAccount("my_access_id", "my_access_key");
 *
 * Odps odps = new Odps(account);
 * odps.setDefaultProject("my_project");
 *
 * for (Table t : odps.tables()) {
 *     ....
 * }
 * </code>
 * </pre>
 *
 * <strong>设置默认Project:</strong><br /><br />
 * SDK中很多集合类上提供的方法有两个版本，比如:
 * <ul>
 * <li>odps.tables().get("table_name")</li>
 * <li>odps.tables().get("project_name", "table_name")</li>
 * </ul>
 * 第一个版本的函数会使用Odps对象上保存的默认Project，设置默认Project的方法如下:
 *
 * <pre>
 * <code>odps.setDefaultProject("my_project");</code>
 * </pre>
 *
 * </p>
 */
public class Odps {

  private static final String defaultEndpoint = "http://service.odps.aliyun.com/api";

  private Account account;
  private AppAccount appAccount;
  private AppStsAccount appStsAccount;
  private String endpoint;
  private String defaultProject;
  private String currentSchema;
  private String tunnelEndpoint;

  /* resource collection objects */
  private Tenant tenant;
  private Projects projects;
  private Schemas schemas;
  private Tables tables;
  private Instances instances;
  private Resources resources;
  private Functions functions;
  private Volumes volumes;
  private XFlows xflows;
  private OfflineModels offlineModels;
  private Classifications classifications;
  private Quotas quotas;
  private Streams streams;
  private OdpsOptions options;

  /* RestClient instance */
  protected RestClient client;

  private String userAgent;

  private String logViewHost;
  private String jobInsightHost;
  private AccountFormat accountFormat = null;

  public void setAccount(Account account) {
    this.client.setAccount(account);
    this.account = account;
  }

  public void setAppAccount(AppAccount appAccount) {
    this.client.setAppAccount(appAccount);
    this.appAccount = appAccount;
  }

  public void setAppStsAccount(AppStsAccount appStsAccount) {
    this.client.setAppStsAccount(appStsAccount);
    this.appStsAccount = appStsAccount;
  }

  /**
   * 指定{@link Account}构造Odps对象
   *
   * @param account
   *     认证信息
   */
  public Odps(Account account) {
    this(account, null);
  }

  public Odps(Account account, AppAccount appAccount) {
    this.account = account;
    this.appAccount = appAccount;

    client = new RestClient(new DefaultTransport());
    client.setAccount(account);
    client.setAppAccount(appAccount);
    setUserAgent("");

    setEndpoint(defaultEndpoint);

    tenant = new Tenant(this);
    projects = new Projects(this);
    schemas = new Schemas(this);
    tables = new Tables(this);
    streams = new Streams(this);
    instances = new Instances(this);
    resources = new Resources(this);
    functions = new Functions(this);
    volumes = new Volumes(client);
    xflows = new XFlows(this);
    offlineModels = new OfflineModels(this);
    classifications = new Classifications(this);
    quotas = new Quotas(this);
    options = new OdpsOptions();
  }

  public Odps(Odps odps) {
    this(odps.account, odps.appAccount);
    setDefaultProject(odps.getDefaultProject());
    setUserAgent(odps.getUserAgent());
    setEndpoint(odps.getEndpoint());
    setLogViewHost(odps.getLogViewHost());
    setJobInsightHost(odps.getJobInsightHost());
    setTunnelEndpoint(odps.tunnelEndpoint);
    setCurrentSchema(odps.getCurrentSchema());
    setAccountFormat(odps.getAccountFormat());
    setGlobalSettings(odps.getGlobalSettings());
    setOptions(odps.options);
    client.setIgnoreCerts(odps.getRestClient().isIgnoreCerts());
    client.setPrefix(odps.getRestClient().getPrefix());
    odps.getRestClient().getUserDefinedHeaders()
        .forEach((k, v) -> client.addUserDefinedHeader(k, v));
    if (odps.getRestClient().getProxy() != null) {
      client.setProxy(odps.getRestClient().getProxy());
    }
    instances.setDefaultRunningCluster(odps.instances.getDefaultRunningCluster());
  }

  public Tenant tenant() {
    return tenant;
  }

  /**
   * 获取表示ODPS所有{@link Project}的集合对象
   *
   * @return {@link Projects}
   */
  public Projects projects() {
    return projects;
  }

  public Schemas schemas() {
    return schemas;
  }

  /**
   * 获取表示ODPS所有{@link Table}的集合对象
   *
   * @return {@link Tables}
   */
  public Tables tables() {
    return tables;
  }

  /**
   * 获取表示ODPS所有{@link Stream}的集合对象
   *
   * @return {@link Streams}
   */
  public Streams streams() {
    return streams;
  }

  /**
   * 获得表示ODPS所有{@link Instance}的集合对象
   *
   * @return {@link Instances}
   */
  public Instances instances() {
    return instances;
  }

  /**
   * 获取表示ODPS所有{@link Resource}的集合对象
   *
   * @return {@link Resources}
   */
  public Resources resources() {
    return resources;
  }

  /**
   * 获取表示ODPS所有{@link Function}的集合对象
   *
   * @return {@link Functions}
   */
  public Functions functions() {
    return functions;
  }

  /**
   * 获取表示ODPS所有Volumes的集合对象
   *
   * @return {@link Volumes}
   */
  public Volumes volumes() {
    return volumes;
  }

  /**
   * 获取表示ODPS所有{@link Classification}的集合对象
   */
  public Classifications classifications() {
    return classifications;
  }

  public TableTunnel tableTunnel() {
    return new TableTunnel(this);
  }
  public TableTunnel tableTunnel(Configuration configuration) {
    return new TableTunnel(this, configuration);
  }

  public Quotas quotas() {
    return quotas;
  }

  /**
   * 获取{@link Account} <b>暂未开放，仅限内部使用<b/>
   *
   * @return {@link Account}
   */
  public Account getAccount() {
    return account;
  }

  /**
   * 获取{@link AppAccount}
   * @return {@link AppAccount}
   */
  public AppAccount getAppAccount() {
    return appAccount;
  }

  /**
   * 获取{@link AppStsAccount}
   * @return {@link AppStsAccount}
   */
  public AppStsAccount getAppStsAccount() {
    return appStsAccount;
  }

  /**
   * 获取ODPS服务的地址
   *
   * @return ODPS服务地址
   */
  public String getEndpoint() {
    return endpoint;
  }

  /**
   * 设置ODPS服务的地址
   *
   * @param endpoint
   *     ODPS服务地址
   */
  public void setEndpoint(String endpoint) {

    try {
      URI uri = new URI(endpoint);

      this.endpoint = uri.toString();
      client.setEndpoint(this.endpoint);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public void setTunnelEndpoint(String tunnelEndpoint) {
    this.tunnelEndpoint = tunnelEndpoint;
  }

  public String getTunnelEndpoint() {
    return tunnelEndpoint;
  }

  /**
   * 取得当前对象的默认{@link Project}名称
   *
   * @return 默认{@link Project}名称
   */
  public String getDefaultProject() {
    return defaultProject;
  }

  /**
   * Get current schema.
   *
   * @return Current schema name. Null or empty string means using the default schema.
   */
  public String getCurrentSchema() {
    return currentSchema;
  }

  /**
   * 指定默认使用的{@link Project}名称，同时将当前schema置为该project下的默认schema
   *
   * @param defaultProject
   *     默认{@link Project}名称，不允许为null或空串
   */
  public void setDefaultProject(String defaultProject) {
    this.defaultProject = defaultProject;
    this.currentSchema = null;
    client.setDefaultProject(defaultProject);
    client.setCurrentSchema(null);
  }

  /**
   * Set current schema.
   *
   * @param schema Schema name. Null or empty string means using the default schema.
   */
  public void setCurrentSchema(String schema) {
    this.currentSchema = schema;
    client.setCurrentSchema(schema);
  }

  /**
   * 设置访问API时附加的User-Agent信息
   *
   * @param userAgent
   *     User Agent信息
   */
  public void setUserAgent(String userAgent) {
    this.userAgent = userAgent;
    client.setUserAgent(userAgent);
  }

  /**
   * 获取访问API时附加的User-Agent信息
   *
   * @return User Agent信息
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * 获取Logview工具类
   *
   * @return {@link LogView}
   */
  public LogView logview() {
    return new LogView(this);
  }

  public XFlows xFlows() {
    return xflows;
  }

  public OfflineModels offlineModels() {
    return offlineModels;
  }

  public OdpsOptions options() {
    return options;
  }

  public void setOptions(OdpsOptions options) {
    this.options = options;
  }

  @Override
  public Odps clone() {
    return new Odps(this);
  }

  /**
   * 获取ODPS底层传输接口
   *
   * @return {@link RestClient}
   */
  public RestClient getRestClient() {
    return this.client;
  }

  /**
   * 获取logview host地址，可能为null
   *
   * @return logview host地址
   */
  public String getLogViewHost() {
    return this.logViewHost;
  }

  /**
   * 获取jobInsight host地址，可能为null
   *
   * @return logview host地址
   */
  public String getJobInsightHost() {
    return this.jobInsightHost;
  }

  /**
   * 设置logview host地址
   *
   * @param host
   *     地址
   */
  public void setLogViewHost(String host) {
    this.logViewHost = host;
  }

  /**
   * 设置job-insight host地址
   *
   * @param host
   *     地址
   */
  public void setJobInsightHost(String host) {
    this.jobInsightHost = host;
  }

  /**
   * 设置参数 account 格式
   *
   * @param format
   *     account 格式
   */
  public void setAccountFormat(AccountFormat format) {
    this.accountFormat = format;
  }

  /**
   * 获取参数 account 格式
   *
   * @return
   *     account 格式
   */
  public AccountFormat getAccountFormat() {
    return this.accountFormat;
  }

  /**
   * 设置全局配置，全局配置将会被替换为参数传入的配置
   * @param settings
   *     新的全局配置
   */
  public void setGlobalSettings(Map<String, String> settings) {
    Task.setGlobalSettings(settings);
  }

  /**
   * 获取全局配置的副本（deep copy）
   * @return
   *     全局配置的副本
   */
  public Map<String, String> getGlobalSettings() {
    return Task.getGlobalSettings();
  }
}
