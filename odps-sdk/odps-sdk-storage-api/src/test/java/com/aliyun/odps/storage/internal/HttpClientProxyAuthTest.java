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

package com.aliyun.odps.storage.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.storage.MaxStorageException;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.internal.retry.RetryHandler;
import com.aliyun.odps.storage.internal.retry.RetryPolicy;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.sun.net.httpserver.HttpServer;

/**
 * The storage-api (OkHttp) egress against a proxy that requires {@code Proxy-Authorization}.
 *
 * <p>The origin and the authenticating proxy are both local, so this runs in CI without cloud
 * credentials, while still measuring what matters: whether the header reaches the proxy at all.
 * The expected proxy-side observation sequence mirrors the out-of-band matrix that produced the
 * SDK gap: first attempt without the header ({@code auth=none}), then the retried attempt with it
 * ({@code auth=ok}).
 */
class HttpClientProxyAuthTest {

  private static final String MARKER = "proxy-auth-marker";
  private static final String USER = "alice";
  private static final String PASSWORD = "test-proxy-password";

  private HttpServer origin;
  private String originBase;
  private AuthenticatingProxy proxy;

  @BeforeEach
  void setUp() throws IOException {
    origin = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    origin.createContext("/probe", exchange -> {
      byte[] body = MARKER.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "text/plain");
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    origin.start();
    originBase = "http://127.0.0.1:" + origin.getAddress().getPort();
    proxy = new AuthenticatingProxy(USER, PASSWORD);
  }

  @AfterEach
  void tearDown() {
    origin.stop(0);
    proxy.close();
  }

  @Test
  void credentiallessProxyConfigStillSeesTheChallengeAndFails() {
    HttpClient client = newClient(ProxyConfig.builder()
        .withHttpProxy("http://127.0.0.1:" + proxy.port())
        .build());
    try {
      // Nothing was invented: without a credential OkHttp keeps Authenticator.NONE, the proxy
      // refuses the request, and the caller gets a failure. The proxy-side counters are the proof
      // that the 407 came from "no Proxy-Authorization at all", not from a wrong one.
      assertThrows(MaxStorageException.class, () ->
          client.request("probe", "GET", Collections.emptyMap(), Collections.emptyMap(), null));
      assertEquals(1, proxy.authNone.get());
      assertEquals(0, proxy.authOk.get());
      assertEquals(0, proxy.authBad.get());
    } finally {
      client.shutdown();
    }
  }

  @Test
  void proxyCredentialTurnsTheChallengeIntoASuccessfulRequest() {
    HttpClient client = newClient(ProxyConfig.builder()
        .withHttpProxy("http://127.0.0.1:" + proxy.port(), USER, PASSWORD)
        .build());
    try {
      HttpResponse response = client.request(
          "probe", "GET", Collections.emptyMap(), Collections.emptyMap(), null);

      assertEquals(200, response.getStatusCode());
      assertEquals(MARKER, response.getBody());
      assertEquals(0, proxy.authBad.get());
      assertTrue(proxy.authNone.get() > 0, "the first attempt must go out unauthenticated");
      assertTrue(proxy.authOk.get() > 0, "the retried attempt must carry Proxy-Authorization");
    } finally {
      client.shutdown();
    }
  }

  @Test
  void userInfoInEndpointIsEnoughForTheSameResult() {
    HttpClient client = newClient(ProxyConfig.builder()
        .withHttpProxy("http://" + USER + ":" + PASSWORD + "@127.0.0.1:" + proxy.port())
        .build());
    try {
      HttpResponse response = client.request(
          "probe", "GET", Collections.emptyMap(), Collections.emptyMap(), null);

      assertEquals(200, response.getStatusCode());
      assertTrue(proxy.authOk.get() > 0);
    } finally {
      client.shutdown();
    }
  }

  @Test
  void aWrongCredentialIsNotRetriedForever() {
    HttpClient client = newClient(ProxyConfig.builder()
        .withHttpProxy("http://127.0.0.1:" + proxy.port(), USER, "wrong-password")
        .build());
    try {
      assertThrows(Exception.class, () ->
          client.request("probe", "GET", Collections.emptyMap(), Collections.emptyMap(), null));
      // The authenticator answers the challenge once and then gives up, so the proxy sees a small
      // bounded number of refused attempts instead of a retry loop.
      assertTrue(proxy.authBad.get() > 0);
      assertTrue(proxy.authBad.get() <= 6, "proxy refused attempts must stay bounded, was "
          + proxy.authBad.get());
      assertEquals(0, proxy.authOk.get());
    } finally {
      client.shutdown();
    }
  }

  private HttpClient newClient(ProxyConfig proxyConfig) {
    ICredentials credentials = mock(ICredentials.class);
    when(credentials.getAccessKeyId()).thenReturn("test-ak");
    when(credentials.getAccessKeySecret()).thenReturn("test-sk");
    when(credentials.getSecurityToken()).thenReturn(null);
    ICredentialsProvider provider = mock(ICredentialsProvider.class);
    when(provider.getCredentials()).thenReturn(credentials);

    return new HttpClient(StubSettings.newBuilder()
        .withEndpoint(originBase)
        .withTunnelEndpoint(originBase)
        .withProject("project")
        .withCredentialsProvider(provider)
        // A single attempt per cell keeps the proxy-side counters readable.
        .withRetryHandler(new RetryHandler() {
          @Override
          protected RetryPolicy getRetryPolicy(Exception exception) {
            return new RetryPolicy() {
              @Override
              public boolean shouldRetry(Exception exception, int attempt) {
                return false;
              }

              @Override
              public long getRetryWaitTime(int attempt) {
                return 0;
              }
            };
          }
        })
        .withHttpSettings(HttpSettings.newBuilder()
            .withProxyConfig(proxyConfig)
            .build())
        .build());
  }

  /**
   * Minimal forwarding proxy that requires HTTP Basic proxy authentication, for {@code CONNECT}
   * (https / netty) and absolute-form requests (plain http / OkHttp) alike.
   *
   * <p>Only counters are kept; credentials never land in the log or in an assertion message.
   */
  private static final class AuthenticatingProxy implements AutoCloseable {

    private final ServerSocket listener;
    private final String expectedHeader;
    private final AtomicInteger authNone = new AtomicInteger();
    private final AtomicInteger authOk = new AtomicInteger();
    private final AtomicInteger authBad = new AtomicInteger();
    private volatile boolean closed;

    AuthenticatingProxy(String user, String password) throws IOException {
      listener = new ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"));
      byte[] raw = (user + ":" + password).getBytes(StandardCharsets.UTF_8);
      expectedHeader = "Basic " + java.util.Base64.getEncoder().encodeToString(raw);
      Thread acceptor = new Thread(this::acceptLoop, "test-auth-proxy");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    int port() {
      return listener.getLocalPort();
    }

    @Override
    public void close() {
      closed = true;
      try {
        listener.close();
      } catch (IOException ignored) {
        // already closed
      }
    }

    private void acceptLoop() {
      while (!closed) {
        try {
          Socket client = listener.accept();
          Thread worker = new Thread(() -> serve(client), "test-auth-proxy-conn");
          worker.setDaemon(true);
          worker.start();
        } catch (IOException e) {
          return;
        }
      }
    }

    private void serve(Socket client) {
      try (Socket maybeClient = client) {
        InputStream in = maybeClient.getInputStream();
        String requestLine = readLine(in);
        if (requestLine == null) {
          return;
        }
        Map<String, String> headers = readHeaders(in);
        if (!accepted(headers.get("proxy-authorization"))) {
          respond(maybeClient.getOutputStream(),
                  "HTTP/1.1 407 Proxy Authentication Required",
                  "Proxy-Authenticate: Basic realm=\"odps-proxy-auth-test\"");
          return;
        }
        String[] parts = requestLine.split(" ");
        if ("CONNECT".equalsIgnoreCase(parts[0])) {
          tunnel(maybeClient, parts[1]);
        } else {
          forward(maybeClient, in, headers, requestLine, parts[1]);
        }
      } catch (IOException ignored) {
        // The client is gone; the counters already describe what was observed.
      }
    }

    private boolean accepted(String presented) {
      if (presented == null) {
        authNone.incrementAndGet();
        return false;
      }
      if (presented.equals(expectedHeader)) {
        authOk.incrementAndGet();
        return true;
      }
      authBad.incrementAndGet();
      return false;
    }

    private void tunnel(Socket client, String authority) throws IOException {
      Target target = Target.of("http://" + authority);
      try (Socket upstream = new Socket()) {
        upstream.connect(new InetSocketAddress(target.host, target.port), 5000);
        respond(client.getOutputStream(), "HTTP/1.1 200 Connection Established", null);
        List<Thread> pumps = java.util.Arrays.asList(
            pump(client.getInputStream(), upstream.getOutputStream()),
            pump(upstream.getInputStream(), client.getOutputStream()));
        for (Thread pump : pumps) {
          pump.join(15000);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private void forward(Socket client,
                         InputStream in,
                         Map<String, String> headers,
                         String requestLine,
                         String absoluteUrl) throws IOException {
      Target target = Target.of(absoluteUrl);
      int length = headerInt(headers.get("content-length"));
      byte[] body = length > 0 ? readFully(in, length) : new byte[0];
      try (Socket upstream = new Socket()) {
        upstream.connect(new InetSocketAddress(target.host, target.port), 5000);
        String originForm = target.originForm;
        OutputStream out = upstream.getOutputStream();
        writeLine(out, requestLine.replace(absoluteUrl, originForm));
        for (Map.Entry<String, String> header : headers.entrySet()) {
          if ("proxy-authorization".equals(header.getKey())
              || "connection".equals(header.getKey())
              || "proxy-connection".equals(header.getKey())) {
            continue;
          }
          writeLine(out, header.getKey() + ": " + header.getValue());
        }
        // The origin closes after one response, which keeps this test free of length parsing.
        writeLine(out, "Connection: close");
        writeLine(out, "");
        out.write(body);
        out.flush();
        OutputStream toClient = client.getOutputStream();
        copy(upstream.getInputStream(), toClient);
        toClient.flush();
      }
    }

    private Thread pump(InputStream from, OutputStream to) {
      Thread thread = new Thread(() -> {
        try {
          copy(from, to);
          to.flush();
        } catch (IOException ignored) {
          // tunnel closed
        }
      }, "test-auth-proxy-pump");
      thread.setDaemon(true);
      thread.start();
      return thread;
    }

    private static void respond(OutputStream out, String statusLine, String extraHeader)
        throws IOException {
      StringBuilder sb = new StringBuilder(statusLine).append("\r\n");
      if (extraHeader != null) {
        sb.append(extraHeader).append("\r\n");
      }
      sb.append("Content-Length: 0\r\n\r\n");
      out.write(sb.toString().getBytes(StandardCharsets.US_ASCII));
      out.flush();
    }
  }

  /**
   * An absolute-form or CONNECT target, split by hand: the proxy under test only ever has to deal
   * with {@code http://host:port/path}, so no URI parser (and its percent-encoding rules) is
   * needed here.
   */
  private static final class Target {

    private final String host;
    private final int port;
    private final String originForm;

    private Target(String host, int port, String originForm) {
      this.host = host;
      this.port = port;
      this.originForm = originForm;
    }

    static Target of(String url) {
      int schemeEnd = url.indexOf("://");
      String rest = schemeEnd < 0 ? url : url.substring(schemeEnd + 3);
      int pathStart = rest.indexOf('/');
      String authority = pathStart < 0 ? rest : rest.substring(0, pathStart);
      String path = pathStart < 0 ? "/" : rest.substring(pathStart);
      String host;
      int port = 80;
      if (authority.startsWith("[")) {
        int close = authority.indexOf(']');
        host = authority.substring(1, close);
        String tail = authority.substring(close + 1);
        if (tail.startsWith(":")) {
          port = Integer.parseInt(tail.substring(1));
        }
      } else {
        int colon = authority.indexOf(':');
        host = colon < 0 ? authority : authority.substring(0, colon);
        if (colon >= 0) {
          port = Integer.parseInt(authority.substring(colon + 1));
        }
      }
      return new Target(host, port, path);
    }
  }

  private static Map<String, String> readHeaders(InputStream in) throws IOException {
    Map<String, String> headers = new TreeMap<>();
    String line;
    while ((line = readLine(in)) != null && !line.isEmpty()) {
      int colon = line.indexOf(':');
      if (colon > 0) {
        headers.put(line.substring(0, colon).toLowerCase(Locale.ROOT),
                    line.substring(colon + 1).trim());
      }
    }
    return headers;
  }

  private static String readLine(InputStream in) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream(128);
    int current;
    boolean any = false;
    while ((current = in.read()) != -1) {
      any = true;
      if (current == '\n') {
        byte[] bytes = buffer.toByteArray();
        int length = bytes.length;
        if (length > 0 && bytes[length - 1] == '\r') {
          length--;
        }
        return new String(bytes, 0, length, StandardCharsets.ISO_8859_1);
      }
      buffer.write(current);
    }
    return any ? new String(buffer.toByteArray(), StandardCharsets.ISO_8859_1) : null;
  }

  private static byte[] readFully(InputStream in, int length) throws IOException {
    byte[] bytes = new byte[length];
    int read = 0;
    while (read < length) {
      int step = in.read(bytes, read, length - read);
      if (step < 0) {
        break;
      }
      read += step;
    }
    return bytes;
  }

  private static int headerInt(String value) {
    if (value == null) {
      return 0;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static void writeLine(OutputStream out, String line) throws IOException {
    out.write((line + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
  }

  private static void copy(InputStream from, OutputStream to) throws IOException {
    byte[] buffer = new byte[8192];
    int read;
    while ((read = from.read(buffer)) > 0) {
      to.write(buffer, 0, read);
      to.flush();
    }
  }
}
