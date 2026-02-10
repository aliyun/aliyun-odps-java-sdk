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

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsOptions {

  private Boolean useLegacyLogview;

  private Boolean skipCheckIfEpv2;

  private ProxyConfig proxyConfig;

  /**
   * 是否允许读取可能过时（stale）的中心化元数据。
   *
   * <p><b>默认值为 {@code false}</b>。在此模式下，SDK会始终尝试从权威数据源获取元数据，
   * 以保证最高的数据一致性（例如，写后立即可读）。如果权威数据源暂时不可用，请求将会失败。
   *
   * <p>当设置为 {@code true} 时，SDK 在特定情况下（如为了提高可用性或读取性能）可能会从一个非权威的、
   * 存在数据同步延迟的副本读取元数据。
   *
   * <p><b>重要提示:</b> 启用此选项意味着您的应用必须能够容忍读到旧的元数据。
   * 例如，在您刚刚修改了Project的属性后，立即发起的读请求可能仍然返回修改前的值。
   *
   * <p><b>影响范围:</b> 此参数当前仅影响对Project和Tenant元数据的读取，具体为
   * {@code odps.project().reload()} 和 {@code odps.tenant().reload()} 两个接口。
   */
  private boolean allowStaleMetadataRead;


  private final Odps odps;

  OdpsOptions(Odps odps) {
    this.odps = odps;
  }

  public void setUseLegacyLogview(Boolean useLegacyLogview) {
    this.useLegacyLogview = useLegacyLogview;
  }

  public void setProxyConfig(ProxyConfig proxyConfig) {
    this.proxyConfig = proxyConfig;
    if (proxyConfig == null) {
      this.odps.client.setProxy(null);
    } else {
      try {
        URI uri = new URI(odps.getEndpoint());
        switch (uri.getScheme()) {
          case "http":
            proxyConfig.getProxy(ProxyConfig.Type.HTTP).ifPresent(proxy ->
                                                                    this.odps.client.setProxy(
                                                                      proxy));
            break;
          case "https":
            proxyConfig.getProxy(ProxyConfig.Type.HTTPS).ifPresent(proxy ->
                                                                     this.odps.client.setProxy(
                                                                       proxy));
            break;
        }
      } catch (URISyntaxException e) {
        // won't happen here, already check in odps.setEndpoint();
        throw new RuntimeException(e);
      }
    }
  }

  public Boolean isUseLegacyLogview() {
    return useLegacyLogview;
  }

  public void setSkipCheckIfEpv2(Boolean skipCheckIfEpv2) {
    this.skipCheckIfEpv2 = skipCheckIfEpv2;
  }

  public Boolean isSkipCheckIfEpv2() {
    return skipCheckIfEpv2;
  }

  public ProxyConfig getProxyConfig() {
    return proxyConfig;
  }

  public void setAllowStaleMetadataRead(boolean allowStaleMetadataRead) {
    this.allowStaleMetadataRead = allowStaleMetadataRead;
  }

  public boolean isAllowStaleMetadataRead() {
    return allowStaleMetadataRead;
  }

  OdpsOptions clone(Odps newOdps) {
    OdpsOptions options = new OdpsOptions(newOdps);
    options.useLegacyLogview = useLegacyLogview;
    options.skipCheckIfEpv2 = skipCheckIfEpv2;
    // proxy config is unmodified, shallow copy is enough
    options.proxyConfig = proxyConfig;
    options.allowStaleMetadataRead = allowStaleMetadataRead;
    return options;
  }
}