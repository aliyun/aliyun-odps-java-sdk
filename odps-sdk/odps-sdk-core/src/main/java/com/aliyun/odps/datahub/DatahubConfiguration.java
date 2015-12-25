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

package com.aliyun.odps.datahub;

import java.net.URI;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.GeneralConfiguration;
import com.aliyun.odps.rest.RestClient;

/**
 * ODPS Datahub 配置项
 *
 * <p>
 * 用于保存与ODPS Datahub服务通讯过程需要的配置信息。
 * </p>
 *
 * @author <a href="mailto:dongxiao.dx@alibaba-inc.com">dongxiao</a>
 */
public class DatahubConfiguration extends GeneralConfiguration {

  public DatahubConfiguration(Odps odps) {
    super(odps);
  }

  /**
   * 取得指定Project的Datahub服务入口地址
   *
   * @return ODPS Datahub服务入口地址
   * @throws com.aliyun.odps.datahub.DatahubException
   */
  @Override
  public URI getEndpoint(String projectName) throws OdpsException {

    if (endpoint != null) {
      return endpoint;
    }
    throw new DatahubException("datahubEndpoint not set yet！");
  }

  RestClient newRestClient(String projectName) throws OdpsException {

    RestClient odpsServiceClient = odps.clone().getRestClient();

    odpsServiceClient.setReadTimeout(getSocketTimeout());
    odpsServiceClient.setConnectTimeout(getSocketConnectTimeout());
    odpsServiceClient.setEndpoint(getEndpoint(projectName).toString());

    return odpsServiceClient;
  }
}
