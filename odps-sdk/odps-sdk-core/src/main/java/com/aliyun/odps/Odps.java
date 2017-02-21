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

import com.aliyun.odps.account.Account;
import com.aliyun.odps.commons.transport.DefaultTransport;
import com.aliyun.odps.ml.OfflineModels;
import com.aliyun.odps.rest.RestClient;

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
  private String endpoint;
  private String defaultProject;

  /* resource collection objects */
  private Projects projects;
  private Tables tables;
  private Instances instances;
  private Resources resources;
  private Functions functions;
  private Volumes volumes;
  private Topologies topologies;
  private StreamJobs streamJobs;
  private XFlows xflows;
  private OfflineModels offlineModels;

  /* RestClient instance */
  protected RestClient client;

  private String userAgent;

  private String logViewHost;

  public void setAccount(Account account) {
    this.client.setAccount(account);
    this.account = account;
  }

  /**
   * 指定{@link Account}构造Odps对象
   *
   * @param account
   *     认证信息
   */
  public Odps(Account account) {
    this.account = account;

    client = new RestClient(new DefaultTransport());
    client.setAccount(account);
    setUserAgent("");

    setEndpoint(defaultEndpoint);

    projects = new Projects(client);
    tables = new Tables(this);
    instances = new Instances(this);
    resources = new Resources(this);
    functions = new Functions(this);
    volumes = new Volumes(client);
    topologies = new Topologies(this);
    streamJobs = new StreamJobs(this);
    xflows = new XFlows(this);
    offlineModels = new OfflineModels(this);
  }

  public Odps(Odps odps) {
    this(odps.account);
    setDefaultProject(odps.getDefaultProject());
    setUserAgent(odps.getUserAgent());
    setEndpoint(odps.getEndpoint());
    setLogViewHost(odps.getLogViewHost());
    client.setIgnoreCerts(odps.getRestClient().isIgnoreCerts());
    instances.setDefaultRunningCluster(odps.instances.getDefaultRunningCluster());
  }

  /**
   * 获取表示ODPS所有{@link Project}的集合对象
   *
   * @return {@link Projects}
   */
  public Projects projects() {
    return projects;
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
   * 获取表示Project下所有TOPOLOGY的集合对象
   *
   * @return {@link Topologies}
   */
  public Topologies topologies() {
    return topologies;
  }

  /**
   * 获取表示Project下所有STREAMJOB的集合对象
   *
   * @return {@link StreamJobs}
   */
  public StreamJobs streamJobs() {
    return streamJobs;
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

  /**
   * 取得当前对象的默认{@link Project}名称
   *
   * @return 默认{@link Project}名称
   */
  public String getDefaultProject() {
    return defaultProject;
  }

  /**
   * 指定默认使用的{@link Project}名称
   *
   * @param defaultProject
   *     默认{@link Project}名称
   */
  public void setDefaultProject(String defaultProject) {
    this.defaultProject = defaultProject;
    client.setDefaultProject(defaultProject);
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
   * 设置logview host地址
   *
   * @param host
   *     地址
   */
  public void setLogViewHost(String host) {
    this.logViewHost = host;
  }
}
