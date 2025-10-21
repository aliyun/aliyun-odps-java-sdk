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

  OdpsOptions clone(Odps newOdps) {
    OdpsOptions options = new OdpsOptions(newOdps);
    options.useLegacyLogview = useLegacyLogview;
    options.skipCheckIfEpv2 = skipCheckIfEpv2;
    // proxy config is unmodified, shallow copy is enough
    options.proxyConfig = proxyConfig;
    return options;
  }
}