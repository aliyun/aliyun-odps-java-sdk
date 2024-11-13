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
import java.util.List;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.GeneralConfiguration;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import com.aliyun.odps.utils.StringUtils;

/**
 * ODPS Tunnel 配置项
 *
 * <p>
 * 用于保存与ODPS Tunnel服务通讯过程需要的配置信息。
 * </p>
 *
 * @author <a href="mailto:chao.liu@alibaba-inc.com">chao.liu</a>
 */
public class Configuration extends GeneralConfiguration {

  private CompressOption compressOption = new CompressOption();
  private String quotaName = "";
  private List<String> tags;
  private TunnelRetryHandler.RetryPolicy retryPolicy;
  private RestClient.RetryLogger retryLogger;

  public Configuration(Odps odps) {
    super(odps);
    if (!StringUtils.isNullOrEmpty(odps.getTunnelEndpoint())) {
      endpoint = URI.create(odps.getTunnelEndpoint());
    }
  }

  public Configuration(Builder builder) {
    super(builder.odps);
    this.retryPolicy = builder.retryPolicy;
    this.quotaName = builder.quotaName;
    this.compressOption = builder.compressOption;
    this.retryLogger = builder.retryLogger;
    this.tags = builder.tags;
  }

  public static Builder builder(Odps odps) {
    return new Builder(odps);
  }


  public CompressOption getCompressOption() {
    return compressOption;
  }

  public void setCompressOption(CompressOption option) {
    this.compressOption = option;
  }

  /**
   * 取得指定Project的Tunnel服务入口地址
   *
   * @return ODPS Tunnel服务入口地址
   * @throws TunnelException
   */
  @Override
  public URI getEndpoint(String projectName) throws TunnelException {
    if (endpoint != null) {
      return endpoint;
    }

    URI u = null;
    try {
      u = new URI(odps.projects().get(projectName).getTunnelEndpoint(quotaName));
    } catch (URISyntaxException e) {
      throw new TunnelException(e.getMessage(), e);
    } catch (OdpsException e) {
      throw new TunnelException(e.getMessage(), e);
    }
    return u;
  }

  public Odps getOdps() {
    return odps;
  }

  public String getQuotaName() {
    return quotaName;
  }

  public List<String> getTags() {
    return tags;
  }

  public TunnelRetryHandler.RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public RestClient.RetryLogger getRetryLogger() {
    return retryLogger;
  }

  public void setQuotaName(String quotaName) {
    this.quotaName = quotaName;
  }

  public boolean availableQuotaName() {
    return !StringUtils.isEmpty(this.quotaName);
  }

  public RestClient newRestClient(String projectName) throws TunnelException {

    RestClient odpsServiceClient = odps.clone().getRestClient();

    odpsServiceClient.setReadTimeout(getSocketTimeout());
    odpsServiceClient.setConnectTimeout(getSocketConnectTimeout());
    odpsServiceClient.setRetryTimes(getSocketRetryTimes());

    if (StringUtils.isNullOrEmpty(odps.getTunnelEndpoint())) {
      odpsServiceClient.setEndpoint(getEndpoint(projectName).toString());
    } else {
      odpsServiceClient.setEndpoint(odps.getTunnelEndpoint());
    }
    return odpsServiceClient;
  }

  public Builder toBuilder() {
    return new Builder(odps).withQuotaName(quotaName).withCompressOptions(compressOption)
        .withRetryPolicy(retryPolicy).withRetryLogger(retryLogger);
  }

  public static class Builder {

    private final Odps odps;
    private String quotaName = "";
    private List<String> tags;
    private CompressOption compressOption = new CompressOption();
    private TunnelRetryHandler.RetryPolicy retryPolicy;
    private RestClient.RetryLogger retryLogger;

    private Builder(Odps odps) {
      this.odps = odps;
    }

    public Builder withQuotaName(String quotaName) {
      this.quotaName = quotaName;
      return this;
    }

    public Builder withTags(List<String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder withCompressOptions(CompressOption compressOption) {
      this.compressOption = compressOption;
      return this;
    }

    public Builder withRetryPolicy(TunnelRetryHandler.RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    public Builder withRetryLogger(RestClient.RetryLogger retryLogger) {
      this.retryLogger = retryLogger;
      return this;
    }

    public Configuration build() {
      return new Configuration(this);
    }
  }
}
