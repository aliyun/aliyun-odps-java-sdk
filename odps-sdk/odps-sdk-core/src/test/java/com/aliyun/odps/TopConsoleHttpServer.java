/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import com.aliyun.odps.account.AliyunAccount;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/** Loopback HTTP peer: the real REST client performs signing, encoding and error conversion. */
public final class TopConsoleHttpServer implements AutoCloseable {
  private final HttpServer server;
  private final BlockingQueue<Reply> replies = new LinkedBlockingQueue<>();
  private final List<Call> calls = new CopyOnWriteArrayList<>();

  public TopConsoleHttpServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  public Odps newOdps() {
    Odps odps = new Odps(new AliyunAccount("test-id", "test-secret"));
    odps.setEndpoint("http://127.0.0.1:" + server.getAddress().getPort() + "/api");
    odps.setDefaultProject("test_project");
    odps.getRestClient().setRetryTimes(0);
    odps.getRestClient().setConnectTimeout(5);
    odps.getRestClient().setReadTimeout(5);
    return odps;
  }

  public void enqueue(int status, String body) {
    replies.add(new Reply(status, body));
  }

  public List<Call> calls() {
    return new ArrayList<>(calls);
  }

  private void handle(HttpExchange exchange) throws IOException {
    try {
      calls.add(new Call(exchange));
      Reply reply = replies.poll();
      if (reply == null) {
        reply = new Reply(500, "<Error><Code>UnexpectedRequest</Code>"
            + "<Message>No response queued</Message></Error>");
      }
      exchange.getResponseHeaders().set("Content-Type", reply.contentType);
      exchange.getResponseHeaders().set("x-odps-owner", "test-owner");
      exchange.getResponseHeaders().set("x-odps-creation-time", "Wed, 01 Jan 2025 00:00:00 GMT");
      exchange.getResponseHeaders().set("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT");
      exchange.sendResponseHeaders(reply.status, reply.body.length);
      exchange.getResponseBody().write(reply.body);
    } finally {
      exchange.close();
    }
  }

  @Override
  public void close() {
    server.stop(0);
  }

  private static final class Reply {
    private final int status;
    private final byte[] body;
    private final String contentType;

    private Reply(int status, String body) {
      this.status = status;
      this.body = body.getBytes(StandardCharsets.UTF_8);
      this.contentType = body.trim().startsWith("<")
          ? "application/xml; charset=utf-8" : "application/json; charset=utf-8";
    }
  }

  public static final class Call {
    public final String method;
    public final String path;
    public final Map<String, String> query;
    public final String body;
    public final String authorization;
    public final String contentType;

    private Call(HttpExchange exchange) throws IOException {
      method = exchange.getRequestMethod();
      URI uri = exchange.getRequestURI();
      path = uri.getRawPath();
      query = decodeQuery(uri.getRawQuery());
      authorization = exchange.getRequestHeaders().getFirst("Authorization");
      contentType = exchange.getRequestHeaders().getFirst("Content-Type");
      try (InputStream in = exchange.getRequestBody();
           ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[4096];
        int count;
        while ((count = in.read(buffer)) != -1) {
          out.write(buffer, 0, count);
        }
        body = new String(out.toByteArray(), StandardCharsets.UTF_8);
      }
    }

    private static Map<String, String> decodeQuery(String raw) throws IOException {
      if (raw == null || raw.isEmpty()) {
        return Collections.emptyMap();
      }
      Map<String, String> result = new HashMap<>();
      for (String entry : raw.split("&")) {
        String[] pair = entry.split("=", 2);
        result.put(URLDecoder.decode(pair[0], "UTF-8"),
            pair.length == 1 ? "" : URLDecoder.decode(pair[1], "UTF-8"));
      }
      return result;
    }
  }
}
