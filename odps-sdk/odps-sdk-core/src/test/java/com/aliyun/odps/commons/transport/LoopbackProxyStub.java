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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A proxy on loopback that requires {@code Proxy-Authorization: Basic}, for tests of the JDK
 * control-plane stack. Test scaffolding only: it binds 127.0.0.1, it is not a product class, and
 * the credential it accepts is a fixture, never a real one.
 *
 * <p>It answers the two shapes a proxy has to handle:
 *
 * <ul>
 *   <li>an absolute-form request ({@code GET http://host/path HTTP/1.1}) — answered as the origin
 *       would, so a cell can tell "the proxy let it through" from "the proxy refused it";</li>
 *   <li>{@code CONNECT host:port} — the tunnel request the JDK writes by itself for an https
 *       endpoint. Authorised tunnels are answered {@code 502} rather than carried on, because there
 *       is nothing behind this stub to talk TLS with; what a cell asserts is whether the credential
 *       reached the proxy at all.</li>
 * </ul>
 *
 * <p>Every request is recorded as one line, {@code auth=ok|none|bad}. The password is never
 * recorded, only whether it matched — so a failing test cannot leak it either.
 */
final class LoopbackProxyStub implements AutoCloseable {

  private static final String REALM = "loopback-proxy-stub";

  private final ServerSocket listener;
  private final String expectedUser;
  private final String expectedPassword;
  private final List<String> log = Collections.synchronizedList(new ArrayList<String>());
  /** The header block of each request, so a cell can compare request shapes, not just outcomes. */
  private final List<String> headerBlocks =
      Collections.synchronizedList(new ArrayList<String>());
  private final AtomicInteger connections = new AtomicInteger();
  private final Thread acceptor;
  private volatile boolean running = true;
  private volatile int originStatus = 200;
  private volatile byte[] originBody = "served-by-stub-origin".getBytes(StandardCharsets.UTF_8);

  LoopbackProxyStub(String expectedUser, String expectedPassword) throws IOException {
    this.expectedUser = expectedUser;
    this.expectedPassword = expectedPassword;
    this.listener = new ServerSocket(0, 32, java.net.InetAddress.getByName("127.0.0.1"));
    this.acceptor = new Thread(this::acceptLoop, "loopback-proxy-stub-" + listener.getLocalPort());
    this.acceptor.setDaemon(true);
    this.acceptor.start();
  }

  int port() {
    return listener.getLocalPort();
  }

  String address() {
    return "127.0.0.1:" + port();
  }

  String endpoint() {
    return "http://" + address();
  }

  /** What the proxy saw, one line per request, oldest first. */
  List<String> log() {
    synchronized (log) {
      return new ArrayList<>(log);
    }
  }

  /**
   * Every header block the proxy received, in order, with the request line first.
   *
   * <p>{@link #withoutVolatileHeaders} is the form a shape comparison should use: the SDK signs
   * every request, so {@code Date} and {@code Authorization} differ between two identical calls and
   * prove nothing.
   */
  List<String> headerBlocks() {
    synchronized (headerBlocks) {
      return new ArrayList<>(headerBlocks);
    }
  }

  /** A header block with the per-request signing values removed, so two of them can be compared. */
  static String withoutVolatileHeaders(String block) {
    StringBuilder kept = new StringBuilder();
    for (String line : block.split("\n")) {
      String name = line.split(":", 2)[0].trim().toLowerCase(Locale.ROOT);
      if (name.equals("date") || name.equals("authorization") || name.startsWith("x-odps-")) {
        continue;
      }
      kept.append(line).append('\n');
    }
    return kept.toString();
  }

  /**
   * How many CONNECT tunnel requests the proxy saw with this authentication verdict.
   *
   * <p>Parsed against the recorded fields rather than by substring, because a line carries
   * {@code kind=CONNECT target=... auth=...} and a glued-together fragment would silently never
   * match — which is how a "not authenticated" cell can look green.
   */
  int countConnect(String authVerdict) {
    int n = 0;
    for (String line : log()) {
      if (line.startsWith("kind=CONNECT ") && line.contains(" auth=" + authVerdict)) {
        n++;
      }
    }
    return n;
  }

  /** How many requests of any kind were answered with this verdict. */
  int countAuth(String verdict) {
    int n = 0;
    for (String line : log()) {
      if (line.contains(" auth=" + verdict)) {
        n++;
      }
    }
    return n;
  }

  int countMatching(String fragment) {
    int n = 0;
    for (String line : log()) {
      if (line.contains(fragment)) {
        n++;
      }
    }
    return n;
  }

  void answerAsOrigin(int status, String body) {
    this.originStatus = status;
    this.originBody = body.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    running = false;
    try {
      listener.close();
    } catch (IOException ignore) {
      // closing a test stub
    }
  }

  private void acceptLoop() {
    while (running) {
      try {
        final Socket socket = listener.accept();
        connections.incrementAndGet();
        Thread t = new Thread(() -> serve(socket), "loopback-proxy-stub-conn");
        t.setDaemon(true);
        t.start();
      } catch (IOException e) {
        return; // listener closed: the test is over
      }
    }
  }

  private void serve(Socket socket) {
    try {
      socket.setSoTimeout(20000);
      InputStream in = socket.getInputStream();
      BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      String requestLine = reader.readLine();
      if (requestLine == null) {
        return;
      }
      String proxyAuthorization = null;
      StringBuilder block = new StringBuilder(requestLine).append('\n');
      while (true) {
        String header = reader.readLine();
        if (header == null || header.isEmpty()) {
          break;
        }
        block.append(header).append('\n');
        int colon = header.indexOf(':');
        if (colon > 0 && "Proxy-Authorization".equalsIgnoreCase(header.substring(0, colon).trim())) {
          proxyAuthorization = header.substring(colon + 1).trim();
        }
      }
      headerBlocks.add(block.toString());

      String[] parts = requestLine.split("\\s+");
      String kind = parts.length > 0 && "CONNECT".equalsIgnoreCase(parts[0]) ? "CONNECT" : "GET";
      String target = parts.length > 1 ? parts[1] : "-";
      String auth = classify(proxyAuthorization);
      log.add("kind=" + kind + " target=" + target + " auth=" + auth);

      OutputStream out = socket.getOutputStream();
      if (!"ok".equals(auth)) {
        write(out, "HTTP/1.1 407 Proxy Authentication Required\r\n"
            + "Proxy-Authenticate: Basic realm=\"" + REALM + "\"\r\n"
            + "Content-Length: 0\r\nConnection: close\r\n\r\n", null);
        return;
      }
      if ("CONNECT".equals(kind)) {
        // Authorised, and deliberately not carried on: the cell asserts on the log line above.
        write(out, "HTTP/1.1 502 NoTunnelBehindStub\r\nContent-Length: 0\r\n"
            + "Connection: close\r\n\r\n", null);
        return;
      }
      write(out, "HTTP/1.1 " + originStatus + " OK\r\n"
          + "Content-Type: application/xml\r\n"
          + "x-odps-request-id: 2026100100000000loopbackproxy00000001\r\n"
          + "Content-Length: " + originBody.length + "\r\nConnection: close\r\n\r\n", originBody);
    } catch (IOException e) {
      log.add("kind=ERROR detail=" + e.getClass().getSimpleName());
    } finally {
      try {
        socket.close();
      } catch (IOException ignore) {
        // test stub
      }
    }
  }

  private String classify(String headerValue) {
    if (headerValue == null) {
      return "none";
    }
    if (!headerValue.startsWith("Basic ")) {
      return "bad";
    }
    try {
      String raw = new String(Base64.getDecoder().decode(headerValue.substring(6).trim()),
                               StandardCharsets.UTF_8);
      int colon = raw.indexOf(':');
      String user = colon < 0 ? raw : raw.substring(0, colon);
      String password = colon < 0 ? "" : raw.substring(colon + 1);
      return user.equals(expectedUser) && password.equals(expectedPassword)
          ? "ok" : ("bad:" + user);
    } catch (IllegalArgumentException e) {
      return "bad";
    }
  }

  private static void write(OutputStream out, String head, byte[] body) throws IOException {
    out.write(head.getBytes(StandardCharsets.US_ASCII));
    if (body != null) {
      out.write(body);
    }
    out.flush();
  }

  /** Read a stream to bytes without closing anything the JDK still owns. */
  static byte[] drain(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] chunk = new byte[4096];
    int n;
    while ((n = in.read(chunk)) > 0) {
      buf.write(chunk, 0, n);
    }
    return buf.toByteArray();
  }

  static String lower(String value) {
    return value == null ? null : value.toLowerCase(Locale.ROOT);
  }
}
