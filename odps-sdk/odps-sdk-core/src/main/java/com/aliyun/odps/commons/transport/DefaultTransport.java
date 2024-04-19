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

package com.aliyun.odps.commons.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;

import com.aliyun.odps.Survey;
import com.aliyun.odps.commons.transport.Request.Method;
import com.aliyun.odps.commons.util.IOUtils;

/**
 * DefaultTransport基于JDK的{@link HttpURLConnection}提供HTTP请求功能
 *
 * @author shenggong.wang@alibaba-inc.com
 */
@Survey
public class DefaultTransport implements Transport {

  private Proxy proxy;

  public DefaultTransport() {
  }

  @Override
  public Connection connect(Request req) throws IOException {
    DefaultConnection conn = new DefaultConnection(proxy);
    conn.connect(req);
    return conn;
  }

  @Override
  public void setProxy(Proxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public Response request(Request req) throws IOException {
    Connection conn = connect(req);
    DefaultResponse resp = null;
    try {
      // send request body
      if (req.getBody() != null) {
        OutputStream out = conn.getOutputStream();
        IOUtils.copyLarge(req.getBody(), out);
        out.close();
      }

      resp = (DefaultResponse) conn.getResponse();

      if (Method.HEAD != req.getMethod()) {
        InputStream in = conn.getInputStream();
        resp.setBody(IOUtils.readFully(in));
      }

    } finally {
      conn.disconnect();
    }
    return resp;
  }

}
