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
import java.util.HashMap;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;

/**
 * TunnelServerRouter
 */
class TunnelServerRouter {

  private RestClient odpsServiceClient;

  public TunnelServerRouter(RestClient odpsServiceClient) {
    this.odpsServiceClient = odpsServiceClient;
  }

  /**
   * Get tunnel server address for the specified project.
   *
   * @param projectName
   * @return
   * @throws TunnelException
   */
  public URI getTunnelServer(String projectName, String protocol) throws TunnelException {

    if (protocol == null || !(protocol.equals("http") || protocol.equals("https"))) {
      throw new TunnelException("Invalid protocol: " + protocol);
    }

    StringBuilder resource = new StringBuilder();
    resource.append("/projects/").append(projectName).append("/tunnel");

    HashMap<String, String> params = new HashMap<String, String>();
    params.put("service", null);

    Response resp = null;
    try {
      resp = odpsServiceClient.request(resource.toString(), "GET", params, null, null);
    } catch (OdpsException e) {
      throw new TunnelException(e.getMessage(), e);
    }
    String serviceAddr = null;
    if (resp.isOK()) {
      serviceAddr = new String(resp.getBody());
    } else {
      throw new TunnelException("Can't get tunnel server address: " + resp.getStatus());
    }

    URI server = null;
    try {
      server = new URI(protocol + "://" + serviceAddr);
    } catch (URISyntaxException e) {
      throw new TunnelException(e.getMessage(), e);
    }

    return server;
  }
}
