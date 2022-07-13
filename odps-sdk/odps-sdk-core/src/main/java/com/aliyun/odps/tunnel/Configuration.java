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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.GeneralConfiguration;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.CompressOption;

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

  private CompressOption option = new CompressOption();

  public Configuration(Odps odps) {
    super(odps);
  }

  public CompressOption getCompressOption() {
    return option;
  }

  public void setCompressOption(CompressOption option) {
    this.option = option;
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
      u = new URI(odps.projects().get(projectName).getTunnelEndpoint());
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
}
