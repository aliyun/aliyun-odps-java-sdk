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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.internal.retry.RetryHandler;
import com.aliyun.odps.storage.internal.retry.RetryPolicy;
import com.aliyun.odps.storage.settings.HttpSettings;

import okhttp3.MediaType;
import okhttp3.RequestBody;

/**
 * What a caller gets when a non-2xx response carries nothing parseable.
 *
 * <p>Every shape here is a real wire shape, written byte by byte by {@link RawErrorServer} so the
 * test states the exact framing instead of trusting a server library to imply it:
 * {@code Content-Length: 0}, a literal JSON {@code null}, gateway HTML, and a JSON document whose
 * keys are not {@code Code}/{@code Message}. All of them are non-2xx on paths users hit daily —
 * the most common one being a proxy that demands credentials the SDK was never given, which answers
 * {@code 407} with no body at all.
 *
 * <p>The contract being pinned is "locatable, never silent": the caller must be able to read the
 * HTTP status and the request id (when the peer sent one) off the exception, and the message must
 * not be null, blank, or a bare {@code java.lang.NullPointerException}. Shapes that already worked
 * ({@code {"Code":..,"Message":..}}, the raw body kept for unparseable bodies, the {@code N/A}
 * request-id placeholder) are asserted unchanged, so the fix cannot quietly widen.
 *
 * <p>Loopback only: no cloud credentials, no external endpoint, safe in CI.
 */
/** These talk to a loopback socket; a shape that stops answering must fail, not stall the lane. */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class HttpClientErrorResponseTest {

  private static final String PATH = "err";
  private static final String REQUEST_ID = "req-87352120";

  private RawErrorServer origin;
  private String originBase;

  @BeforeEach
  void setUp() throws IOException {
    origin = new RawErrorServer(200, "OK", Collections.<String, String>emptyMap(), "served");
    originBase = "http://127.0.0.1:" + origin.port() + "/";
  }

  @AfterEach
  void tearDown() {
    origin.close();
  }

  /** {@code Content-Length: 0} on a 500: nothing to parse, so the status is all the caller has. */
  @Test
  void emptyErrorBodyStillReportsStatusAndRequestId() throws IOException {
    origin.answer(500, "Internal Server Error", requestIdHeaders(), new byte[0]);
    HttpClient client = newClient(singleAttemptRetryHandler());
    try {
      ServiceException e = assertThrows(ServiceException.class, () -> get(client));

      assertEquals(500, e.getHttpStatus());
      assertEquals(REQUEST_ID, e.getRequestId());
      assertEquals("N/A", e.getErrorCode());
      assertMessageCarriesContext(e.getMessage(), "500", REQUEST_ID);
    } finally {
      client.shutdown();
    }
  }

  /** A body of the four bytes {@code null} deserializes to a null payload: the same gap. */
  @Test
  void literalNullJsonBodyIsTreatedAsMissingErrorPayload() throws IOException {
    origin.answer(400, "Bad Request", requestIdHeaders(), "null");
    HttpClient client = newClient(singleAttemptRetryHandler());
    try {
      ServiceException e = assertThrows(ServiceException.class, () -> get(client));

      assertEquals(400, e.getHttpStatus());
      assertEquals(REQUEST_ID, e.getRequestId());
      assertMessageCarriesContext(e.getMessage(), "400", REQUEST_ID);
    } finally {
      client.shutdown();
    }
  }

  /**
   * Gateway HTML is not JSON. This shape already produced a ServiceException before the fix, so it
   * is kept as a compatibility control: the raw body stays the message, status and request id stay
   * readable, nothing is reworded.
   */
  @Test
  void nonJsonErrorBodyKeepsTheRawBodyAsTheMessage() throws IOException {
    String html = "<html><body><h1>502 Bad Gateway</h1></body></html>";
    Map<String, String> headers = requestIdHeaders();
    headers.put("Content-Type", "text/html; charset=utf-8");
    origin.answer(502, "Bad Gateway", headers, html);
    HttpClient client = newClient(singleAttemptRetryHandler());
    try {
      ServiceException e = assertThrows(ServiceException.class, () -> get(client));

      assertEquals(502, e.getHttpStatus());
      assertEquals(REQUEST_ID, e.getRequestId());
      assertEquals("N/A", e.getErrorCode());
      assertEquals(html, e.getMessage());
    } finally {
      client.shutdown();
    }
  }

  /**
   * Proxies and gateways do speak JSON, just with other key names ({@code reason}, lowercase
   * {@code message}). A body that parses to an object with null fields used to surface as a
   * ServiceException whose message was {@code null} — correctly typed, but just as silent.
   */
  @Test
  void jsonErrorBodyWithoutCodeOrMessageIsNotSilent() throws IOException {
    String body = "{\"reason\":\"no such table: t1\",\"detail\":\"checked project meta\"}";
    origin.answer(400, "Bad Request", requestIdHeaders(), body);
    HttpClient client = newClient(singleAttemptRetryHandler());
    try {
      ServiceException e = assertThrows(ServiceException.class, () -> get(client));

      assertEquals(400, e.getHttpStatus());
      assertEquals(REQUEST_ID, e.getRequestId());
      assertEquals("N/A", e.getErrorCode());
      assertTrue(e.getMessage() != null && !e.getMessage().trim().isEmpty(),
                 "message must not be null/blank, was: " + e.getMessage());
      assertTrue(e.getMessage().contains("no such table"),
                 "the peer's own wording is the useful part, message was: " + e.getMessage());
    } finally {
      client.shutdown();
    }
  }

  /** The well-formed error contract must survive untouched: same code, same message, same ids. */
  @Test
  void wellFormedErrorBodyContractIsUnchanged() throws IOException {
    origin.answer(404, "Not Found", requestIdHeaders(),
                  "{\"Code\":\"NoSuchTable\",\"Message\":\"Table not found: t1\"}");
    HttpClient client = newClient(singleAttemptRetryHandler());
    try {
      ServiceException e = assertThrows(ServiceException.class, () -> get(client));

      assertEquals(404, e.getHttpStatus());
      assertEquals("NoSuchTable", e.getErrorCode());
      assertEquals("Table not found: t1", e.getMessage());
      assertEquals(REQUEST_ID, e.getRequestId());
    } finally {
      client.shutdown();
    }
  }

  /** A peer that omits the request-id header keeps the documented {@code N/A} placeholder. */
  @Test
  void missingRequestIdHeaderKeepsTheNaPlaceholder() throws IOException {
    origin.answer(500, "Internal Server Error", Collections.<String, String>emptyMap(),
                  new byte[0]);
    HttpClient client = newClient(singleAttemptRetryHandler());
    try {
      ServiceException e = assertThrows(ServiceException.class, () -> get(client));

      assertEquals("N/A", e.getRequestId());
      assertEquals(500, e.getHttpStatus());
      assertMessageCarriesContext(e.getMessage(), "500", "N/A");
    } finally {
      client.shutdown();
    }
  }

  /**
   * Retry decisions are made on the HTTP status carried by a {@link ServiceException}. Losing that
   * status inside a bare NPE also lost the retry policy: an empty-bodied 503 used to fail fast
   * like a 4xx. The status has to reach the policy.
   */
  @Test
  void emptyErrorBodyStillReachesTheStatusBasedRetryDecision() throws IOException {
    origin.answer(503, "Service Unavailable", requestIdHeaders(), new byte[0]);
    final List<Integer> seenStatus = new ArrayList<>();
    RetryHandler recording = new RetryHandler() {
      @Override
      protected RetryPolicy getRetryPolicy(int statusCode) {
        seenStatus.add(statusCode);
        return noRetry();
      }
    };
    HttpClient client = newClient(recording);
    try {
      assertThrows(ServiceException.class, () -> get(client));

      assertEquals(Collections.singletonList(503), seenStatus,
                   "the 503 must be classified by status, not lost in a NullPointerException");
    } finally {
      client.shutdown();
    }
  }

  /**
   * The shape users actually meet: a proxy that requires authentication, configured without a
   * credential, answering {@code 407} with {@code Content-Length: 0}. The origin is never reached
   * — that is what the hit counter proves — so the failure must say "407", not nothing.
   */
  @Test
  void proxyChallengeWithEmptyBodyReportsTheProxyStatus() throws IOException {
    Map<String, String> challenge = new LinkedHashMap<>();
    challenge.put("Proxy-Authenticate", "Basic realm=\"odps-empty-error-body-test\"");
    RawErrorServer proxy = new RawErrorServer(407, "Proxy Authentication Required",
                                              challenge, new byte[0]);
    HttpClient client = newClient(singleAttemptRetryHandler(), ProxyConfig.builder()
        .withHttpProxy("http://127.0.0.1:" + proxy.port())
        .build());
    try {
      ServiceException e = assertThrows(ServiceException.class, () -> get(client));

      assertEquals(407, e.getHttpStatus());
      assertEquals("N/A", e.getRequestId(), "a proxy challenge carries no x-odps-request-id");
      assertMessageCarriesContext(e.getMessage(), "407", "N/A");
      assertEquals(0, origin.hits(),
                   "the request must not have reached the storage api origin");
    } finally {
      client.shutdown();
      proxy.close();
    }
  }

  /** {@code streamUpload} inspects the error body too; it must not be the path left broken. */
  @Test
  void streamingUploadWithEmptyErrorBodyIsAlsoLocatable() throws IOException {
    origin.answer(500, "Internal Server Error", requestIdHeaders(), new byte[0]);
    HttpClient client = newClient(singleAttemptRetryHandler());
    try {
      HttpResponse response = client.streamUpload(PATH, "POST",
          Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap(),
          RequestBody.create(MediaType.parse("application/octet-stream"), new byte[] {1, 2, 3}));
      throw new IllegalStateException("expected a ServiceException, got status "
          + response.getStatusCode());
    } catch (ServiceException e) {
      assertEquals(500, e.getHttpStatus());
      assertEquals(REQUEST_ID, e.getRequestId());
      assertMessageCarriesContext(e.getMessage(), "500", REQUEST_ID);
    } finally {
      client.shutdown();
    }
  }

  // ---- helpers ---------------------------------------------------------------------------------

  private static void assertMessageCarriesContext(String message, String status, String requestId) {
    assertFalse(message == null || message.trim().isEmpty(),
                "an empty-body error must still produce a non-blank message");
    assertFalse(message.contains("NullPointerException"),
                "the caller must not be handed a bare NPE, message was: " + message);
    assertTrue(message.contains(status),
               "message must carry the HTTP status " + status + ", was: " + message);
    assertTrue(message.contains(requestId),
               "message must carry the request id " + requestId + ", was: " + message);
  }

  private HttpResponse get(HttpClient client) {
    return client.request(PATH, "GET", Collections.<String, String>emptyMap(),
                          Collections.<String, String>emptyMap(), null);
  }

  private static Map<String, String> requestIdHeaders() {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put(HttpClient.ODPS_REQUEST_ID, REQUEST_ID);
    headers.put("Content-Type", "application/json");
    return headers;
  }

  private HttpClient newClient(RetryHandler retryHandler) {
    return newClient(retryHandler, null);
  }

  private HttpClient newClient(RetryHandler retryHandler, ProxyConfig proxyConfig) {
    ICredentials credentials = mock(ICredentials.class);
    when(credentials.getAccessKeyId()).thenReturn("test-ak");
    when(credentials.getAccessKeySecret()).thenReturn("test-sk");
    when(credentials.getSecurityToken()).thenReturn(null);
    ICredentialsProvider provider = mock(ICredentialsProvider.class);
    when(provider.getCredentials()).thenReturn(credentials);

    HttpSettings.HttpSettingsBuilder httpSettings = HttpSettings.newBuilder();
    if (proxyConfig != null) {
      httpSettings = httpSettings.withProxyConfig(proxyConfig);
    }
    return new HttpClient(StubSettings.newBuilder()
        .withEndpoint(originBase)
        .withTunnelEndpoint(originBase)
        .withProject("project")
        .withCredentialsProvider(provider)
        .withRetryHandler(retryHandler)
        .withHttpSettings(httpSettings.build())
        .build());
  }

  /**
   * One attempt per shape, so each message describes exactly the response the test just wrote.
   *
   * <p>The hook must be the exception-level one: for a ServiceException, {@code RetryHandler}
   * chooses its policy from the HTTP status (500/502/503 back off up to seven times), so overriding
   * only the default policy would not stop the retries — it would just make this class slow.
   */
  private static RetryHandler singleAttemptRetryHandler() {
    return new RetryHandler() {
      @Override
      protected RetryPolicy getRetryPolicy(Exception exception) {
        return noRetry();
      }
    };
  }

  private static RetryPolicy noRetry() {
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

  /**
   * A loopback HTTP server that writes the response bytes verbatim, so the framing under test
   * ({@code Content-Length: 0} versus a body) is stated by the test instead of implied.
   *
   * <p>Every request is answered with the currently configured response, whether it arrives in
   * absolute form (plain http through a proxy) or origin form. The request head and the body the
   * head declares are consumed before answering — a client still writing an upload body would
   * otherwise see the socket closed under it and report an I/O failure instead of the status under
   * test. The connection is then closed, which is enough for one exchange per socket. Credentials
   * never appear in this class: the proxy cell only refuses, it never echoes.
   */
  private static final class RawErrorServer implements AutoCloseable {

    private final ServerSocket listener;
    private final AtomicInteger hits = new AtomicInteger();
    private volatile int status;
    private volatile String reason;
    private volatile Map<String, String> headers;
    private volatile byte[] body;
    private volatile boolean closed;

    RawErrorServer(int status, String reason, Map<String, String> headers, String body)
        throws IOException {
      this(status, reason, headers, body.getBytes(StandardCharsets.UTF_8));
    }

    RawErrorServer(int status, String reason, Map<String, String> headers, byte[] body)
        throws IOException {
      this.status = status;
      this.reason = reason;
      this.headers = new LinkedHashMap<>(headers);
      this.body = body.clone();
      listener = new ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"));
      Thread acceptor = new Thread(this::acceptLoop, "raw-error-server");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    int port() {
      return listener.getLocalPort();
    }

    int hits() {
      return hits.get();
    }

    void answer(int status, String reason, Map<String, String> headers, String body) {
      answer(status, reason, headers, body.getBytes(StandardCharsets.UTF_8));
    }

    void answer(int status, String reason, Map<String, String> headers, byte[] body) {
      this.status = status;
      this.reason = reason;
      this.headers = new LinkedHashMap<>(headers);
      this.body = body.clone();
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
          Thread worker = new Thread(() -> serve(client), "raw-error-server-conn");
          worker.setDaemon(true);
          worker.start();
        } catch (IOException e) {
          return;
        }
      }
    }

    private void serve(Socket client) {
      try (Socket socket = client) {
        drainRequest(socket.getInputStream());
        hits.incrementAndGet();
        socket.getOutputStream().write(responseBytes());
        socket.getOutputStream().flush();
      } catch (IOException ignored) {
        // Either the response went out and the client closed, or the client is gone. hits was
        // already taken before writing, so the counters stay honest either way.
      }
    }

    private byte[] responseBytes() throws IOException {
      byte[] payload = body;
      StringBuilder head = new StringBuilder("HTTP/1.1 ")
          .append(status).append(' ').append(reason).append("\r\n");
      for (Map.Entry<String, String> header : headers.entrySet()) {
        head.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
      }
      head.append("Content-Length: ").append(payload.length).append("\r\n")
          .append("Connection: close\r\n\r\n");
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(head.toString().getBytes(StandardCharsets.US_ASCII));
      out.write(payload);
      return out.toByteArray();
    }

    private void drainRequest(InputStream in) throws IOException {
      ByteArrayOutputStream seen = new ByteArrayOutputStream();
      int b;
      while ((b = in.read()) != -1) {
        seen.write(b);
        byte[] bytes = seen.toByteArray();
        if (endsHeaderBlock(bytes)) {
          skipDeclaredBody(in, contentLengthOf(new String(bytes, StandardCharsets.US_ASCII)));
          return;
        }
        if (bytes.length > 65536) {
          throw new InterruptedIOException("request head too large");
        }
      }
    }

    private void skipDeclaredBody(InputStream in, int length) throws IOException {
      int remaining = length;
      byte[] sink = new byte[Math.min(Math.max(remaining, 1), 8192)];
      while (remaining > 0) {
        int read = in.read(sink, 0, Math.min(sink.length, remaining));
        if (read < 0) {
          return;
        }
        remaining -= read;
      }
    }

    private int contentLengthOf(String head) {
      for (String line : head.split("\r?\n")) {
        int colon = line.indexOf(':');
        if (colon > 0 && "content-length".equalsIgnoreCase(line.substring(0, colon).trim())) {
          try {
            return Integer.parseInt(line.substring(colon + 1).trim());
          } catch (NumberFormatException e) {
            return 0;
          }
        }
      }
      return 0;
    }

    private boolean endsHeaderBlock(byte[] bytes) {
      int n = bytes.length;
      return n >= 4 && bytes[n - 4] == '\r' && bytes[n - 3] == '\n'
          && bytes[n - 2] == '\r' && bytes[n - 1] == '\n';
    }
  }
}
