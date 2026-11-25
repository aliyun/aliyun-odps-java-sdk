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
import java.util.logging.Level;
import java.util.logging.Logger;

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

  private static final Logger log = Logger.getLogger(DefaultTransport.class.getName());

  /** The body of a failed response that had nothing readable in it. Never null. */
  private static final byte[] EMPTY_BODY = new byte[0];

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
        resp.setBody(readBody(conn, resp));
      }

    } finally {
      conn.disconnect();
    }
    return resp;
  }

  /**
   * Reads the response body without losing the status line and the headers that
   * {@link Connection#getResponse()} already received.
   *
   * <p>Two shapes reach the catch below on a failed request, and both of them used to discard the
   * whole response:
   * <ul>
   *   <li>the peer sent no error body at all ({@code 4xx/5xx} with {@code Content-Length: 0}), or
   *       the JDK refused to hand one over ({@code HttpURLConnection.getErrorStream()} returns null
   *       for a request whose body was written in streaming mode, so a proxy answering a POST with
   *       {@code 407} reports no body whatever the proxy sent); {@code getInputStream()} then threw
   *       {@code IOException: Invalid connection.};</li>
   *   <li>the error body started arriving and then stopped — a gateway that hangs up mid-response
   *       throws {@code IOException: Premature EOF} from the read.</li>
   * </ul>
   * In both cases the status code and {@code x-odps-request-id} were in hand and gone again by the
   * time the caller saw an exception whose text mentioned neither. {@code RestClient} turns the
   * returned response into an {@code OdpsException} that carries both, so the body is reported as
   * empty rather than as a connection failure.
   *
   * <p>A 2xx whose body cannot be read is still a failure and still throws: reading a successful
   * response halfway is not a result, and nothing on the success path changes.
   */
  private static byte[] readBody(Connection conn, Response resp) throws IOException {
    try {
      InputStream in = conn.getInputStream();
      return IOUtils.readFully(in);
    } catch (IOException e) {
      if (resp.isOK()) {
        throw e;
      }
      if (log.isLoggable(Level.FINE)) {
        log.log(Level.FINE, "HTTP " + resp.getStatus() + " arrived with an unreadable error body ("
            + e.getMessage() + "); reporting status and headers only", e);
      }
      return EMPTY_BODY;
    }
  }

}
