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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.rest.RestClient;

/**
 * What a caller is left holding when a non-2xx response carries nothing readable.
 *
 * <p>Every shape here is a wire shape, written byte by byte by {@link RawServer} instead of being
 * implied by a server library: {@code 500} with {@code Content-Length: 0} (a service that answers
 * with a status line and nothing else), {@code 407} from a proxy that refuses a request whose body
 * was streamed (the shape a customer behind an authenticating proxy meets), and a {@code 500} whose
 * body stops mid-transfer (a gateway that hangs up). All three had the same effect before the original regression: {@code HttpURLConnection} hands over no error stream, or hands one over that then
 * breaks, {@code DefaultTransport} throws while reading it, and the status code and
 * {@code x-odps-request-id} — both already read off the response head — go out with the discarded
 * response. What reached the caller was {@code OdpsException: Invalid connection.} or
 * {@code OdpsException: Premature EOF}: text with no status and no id in it.
 *
 * <p>The contract being pinned is "locatable, never silent": the exception must carry the HTTP
 * status, carry the request id when the peer sent one, and say so in its message when the peer sent
 * none. The shapes that already worked are pinned as controls — a 2xx body read byte for byte, gzip
 * still decoded, a well-formed XML error still reported in the server's own words — so the fix
 * cannot quietly widen into the paths that were fine.
 *
 * <p>Loopback only: no cloud identity, no external endpoint, nothing to opt into. Safe in CI.
 */
public class DefaultTransportErrorResponseTest {

  private static final String RESOURCE = "/projects/error_shapes";
  private static final String REQUEST_ID = "2026093017000055aa1122b33cc44d";
  private static final String XML_ERROR =
      "<Error><Code>NoSuchTable</Code><Message>Table not found: t1</Message>"
      + "<RequestId>" + REQUEST_ID + "</RequestId><HostId>odps</HostId></Error>";

  private RawServer origin;
  private String endpoint;

  @Before
  public void setUp() throws IOException {
    origin = new RawServer(200, "OK", headers(REQUEST_ID, "text/plain"), "served");
    endpoint = "http://127.0.0.1:" + origin.port();
  }

  @After
  public void tearDown() {
    origin.close();
  }

  // ---- the shapes that used to lose the response -----------------------------------------------

  /** {@code 500} with {@code Content-Length: 0}: the status and the id are all the caller has. */
  @Test
  public void emptyErrorBodyStillReportsStatusAndRequestId() {
    origin.answer(500, "Internal Server Error", headers(REQUEST_ID, null), new byte[0]);
    OdpsException e = expectFailure(newClient(), "GET", null, "500 Content-Length:0 +id");

    assertEquals(500, statusOf(e));
    assertEquals(REQUEST_ID, e.getRequestId());
    assertMessageCarries(e.getMessage(), "500", REQUEST_ID);
  }

  /**
   * The shape from the proxy that broke a real upload: a POST whose body is streamed, refused with
   * {@code 407}. JDK 8 will not expose the error body of a streamed request at all — the proxy did
   * send one, and it is unreachable from {@code getErrorStream()} — which is exactly why this cell
   * is not a "server sent nothing" cell. What must survive is the status the proxy answered with.
   */
  @Test
  public void proxyChallengeOnStreamedPostReportsTheProxyStatus() throws IOException {
    Map<String, String> challenge = new LinkedHashMap<>();
    challenge.put("Proxy-Authenticate", "Basic realm=\"mce2eproxy\"");
    challenge.put("Content-Type", "text/plain; charset=utf-8");
    RawServer proxy = new RawServer(407, "Proxy Authentication Required", challenge,
                                    "proxy authentication required");
    try {
      RestClient client = newClient();
      client.setProxy(new Proxy(Proxy.Type.HTTP,
                                new InetSocketAddress("127.0.0.1", proxy.port())));
      OdpsException e = expectFailure(client, "POST", "session-fragment".getBytes(StandardCharsets.UTF_8),
                          "407 from proxy, streamed POST body");

      assertEquals(407, statusOf(e));
      assertMessageCarries(e.getMessage(), "407", null);
      assertTrue("a proxy challenge carries no x-odps-request-id, and the message must say so: "
                     + e.getMessage(),
                 e.getMessage().contains("no x-odps-request-id"));
      assertEquals("the refused request must never reach the origin",
                   0, origin.hits());
      assertTrue("the proxy must have been asked", proxy.hits() >= 1);
      assertTrue("the request must have been sent in absolute form",
                 proxy.lastRequestLine().startsWith("POST " + endpoint + RESOURCE));
    } finally {
      proxy.close();
    }
  }

  /**
   * A response that stops mid-transfer still has a status line, and that is the reportable part:
   * this is the "Premature EOF" shape the daily-environment probe left behind, where the only text
   * the caller got was the name of a socket condition.
   */
  @Test
  public void abortedErrorBodyStillReportsStatusAndRequestId() {
    origin.abortChunked500();
    OdpsException e = expectFailure(newClient(), "GET", null, "500 chunked, aborted mid-chunk");

    assertEquals(500, statusOf(e));
    assertEquals(REQUEST_ID, e.getRequestId());
    assertMessageCarries(e.getMessage(), "500", REQUEST_ID);
  }

  /**
   * The same answer with the body trickling to a stop: the reader times out part way through the
   * error body. The peer did answer {@code 500}, so the caller must be told {@code 500} — not that
   * the endpoint might be wrong, which is what a socket-level text gets rewritten into upstream.
   */
  @Test
  public void stalledErrorBodyStillReportsStatusAndRequestId() {
    origin.stallAfterStartingErrorBody();
    RestClient client = newClient();
    client.setReadTimeout(1);
    OdpsException e = expectFailure(client, "GET", null, "500 body stalls past the read timeout");

    assertEquals(500, statusOf(e));
    assertEquals(REQUEST_ID, e.getRequestId());
    assertMessageCarries(e.getMessage(), "500", REQUEST_ID);
    assertFalse("a peer that answered 500 must not be reported as a bad endpoint: " + e.getMessage(),
                e.getMessage().contains("possible reason"));
  }

  // ---- retry decisions are made on the status, and must keep being made on it ------------------

  /**
   * A definitive {@code 4xx} is not a transient failure. {@code RestRetryStrategy} already says so,
   * and honours it whenever the error had a body to parse: the reason it did not apply to an
   * empty-bodied 4xx was that the status never reached the strategy. Pinned per shape so the two
   * cannot drift apart again.
   */
  @Test
  public void emptyBodiedClientErrorIsNotRetriedLikeATransientOne() {
    origin.answer(400, "Bad Request", headers(REQUEST_ID, null), new byte[0]);
    RestClient client = newClient();
    client.setRetryTimes(3);
    client.setRetryWaitTime(1);

    OdpsException e = expectFailure(client, "GET", null, "400 Content-Length:0 +id");

    assertEquals(400, statusOf(e));
    assertEquals("a 4xx the peer answered must be answered once", 1, origin.hits());
  }

  /** A {@code 5xx} with the same missing body is still transient and must still be retried. */
  @Test
  public void emptyBodiedServerErrorIsStillRetried() {
    origin.answer(503, "Service Unavailable", headers(REQUEST_ID, null), new byte[0]);
    RestClient client = newClient();
    client.setRetryTimes(2);
    client.setRetryWaitTime(1);

    OdpsException e = expectFailure(client, "GET", null, "503 Content-Length:0 +id");

    assertEquals(503, statusOf(e));
    assertEquals("5xx keeps the retry budget it had before", 3, origin.hits());
  }

  // ---- controls: the paths that already worked, unchanged --------------------------------------

  /** A successful body is returned byte for byte, with the status and headers attached. */
  @Test
  public void successfulResponseIsReturnedUntouched() throws Exception {
    byte[] payload = "served".getBytes(StandardCharsets.UTF_8);
    RestClient client = newClient();

    Response resp = client.request(RESOURCE, "GET", null, null, (byte[]) null);

    assertEquals(200, resp.getStatus());
    assertEquals(REQUEST_ID, resp.getHeader(Headers.ODPS_REQUEST_ID));
    assertTrue("body must be byte-identical, was: " + new String(resp.getBody(),
                                                                 StandardCharsets.UTF_8),
               Arrays.equals(payload, resp.getBody()));
    assertEquals(1, origin.hits());
  }

  /** {@code Content-Encoding: gzip} on a success response is still decompressed, not passed raw. */
  @Test
  public void gzippedSuccessBodyIsStillDecoded() throws Exception {
    byte[] plain = "the quick brown fox".getBytes(StandardCharsets.UTF_8);
    Map<String, String> headers = headers(REQUEST_ID, null);
    headers.put(Headers.CONTENT_ENCODING, "gzip");
    origin.answer(200, "OK", headers, gzip(plain));

    Response resp = newClient().request(RESOURCE, "GET", null, null, (byte[]) null);

    assertTrue("gzip must still be undone, got " + resp.getBody().length + " bytes",
               Arrays.equals(plain, resp.getBody()));
  }

  /**
   * A 2xx whose body cannot be read is a failed request, not an empty one. The fix must not be
   * readable as "swallow read errors": returning half a successful response is how silent
   * truncation starts.
   */
  @Test
  public void truncatedSuccessBodyStillFails() {
    origin.abortChunked200();

    try {
      Response resp = newClient().request(RESOURCE, "GET", null, null, (byte[]) null);
      fail("a half-read 2xx must not be handed back, got "
           + (resp.getBody() == null ? "null" : resp.getBody().length + " bytes"));
    } catch (OdpsException e) {
      assertTrue("the failure must stay an I/O failure, was: " + describe(e),
                 e.getCause() instanceof IOException);
      assertFalse("the success path must not start reporting a swallowed body: "
                      + e.getMessage(),
                  String.valueOf(e.getMessage()).contains("returned no readable error body"));
    }
  }

  /** A well-formed error document keeps its own wording, code, type and id: nothing reworded. */
  @Test
  public void wellFormedErrorBodyContractIsUnchanged() {
    origin.answer(404, "Not Found", headers(REQUEST_ID, "application/xml"), XML_ERROR);

    OdpsException e = expectFailure(newClient(), "GET", null, "404 XML error document");

    assertTrue("a 404 must still be a NoSuchObjectException, was: " + e.getClass().getName(),
               e instanceof NoSuchObjectException);
    assertEquals("Table not found: t1", e.getMessage());
    assertEquals(404, statusOf(e));
    assertEquals(REQUEST_ID, e.getRequestId());
    assertEquals("NoSuchTable", e.getErrorCode());
  }

  @Test
  public void requestIdOnlyErrorBodyKeepsItsIdWithoutAHeader() {
    String bodyId = "id-only-in-body";
    origin.answer(500, "Internal Server Error", Collections.<String, String>emptyMap(),
                  "<Error><RequestId>" + bodyId + "</RequestId></Error>");

    OdpsException e = expectFailure(newClient(), "GET", null, "request-id-only error document");

    assertEquals(bodyId, e.getRequestId());
    assertEquals(500, statusOf(e));
    assertFalse(e.getMessage() == null || e.getMessage().trim().isEmpty());
  }

  @Test
  public void codeOnlyErrorBodyKeepsItsCodeAndHasAMessage() {
    origin.answer(404, "Not Found", Collections.<String, String>emptyMap(),
                  "<Error><Code>NoSuchTable</Code></Error>");

    OdpsException e = expectFailure(newClient(), "GET", null, "code-only error document");

    assertTrue(e instanceof NoSuchObjectException);
    assertEquals("NoSuchTable", e.getErrorCode());
    assertFalse(e.getMessage() == null || e.getMessage().trim().isEmpty());
  }

  /** An unparseable but present body is still quoted verbatim; only the ids are added to it. */
  @Test
  public void unparseableErrorBodyKeepsThePeerWordingAsTheMessage() {
    String html = "<html><body><h1>502 Bad Gateway</h1></body></html>";
    Map<String, String> headers = headers(REQUEST_ID, "text/html; charset=utf-8");
    origin.answer(502, "Bad Gateway", headers, html);

    OdpsException e = expectFailure(newClient(), "GET", null, "502 HTML body");

    assertEquals(html, e.getMessage());
    assertEquals(502, statusOf(e));
    assertEquals(REQUEST_ID, e.getRequestId());
  }

  /** A peer that sends no request id at all is reported as sending none, not as an empty id. */
  @Test
  public void missingRequestIdIsStatedAndNotInvented() {
    origin.answer(500, "Internal Server Error", Collections.<String, String>emptyMap(),
                  new byte[0]);

    OdpsException e = expectFailure(newClient(), "GET", null, "500 Content-Length:0, no id header");

    assertNull(e.getRequestId());
    assertEquals(500, statusOf(e));
    assertMessageCarries(e.getMessage(), "500", null);
    assertTrue("the message must say the id is absent, was: " + e.getMessage(),
               e.getMessage().contains("no x-odps-request-id"));
  }

  /**
   * The transport itself must hand a response object to its caller for a bodyless failure: that is
   * where the status and the headers are kept. Asserted on {@link Transport#request} directly so
   * the test does not only measure the exception the layer above happens to build.
   */
  @Test
  public void transportKeepsStatusAndHeadersOfABodylessFailure() throws IOException {
    origin.answer(500, "Internal Server Error", headers(REQUEST_ID, null), new byte[0]);

    Request req = newClient().buildRequest(RESOURCE, "GET", null, null);
    Response resp = new DefaultTransport().request(req);

    assertEquals(500, resp.getStatus());
    assertFalse(resp.isOK());
    assertEquals(REQUEST_ID, resp.getHeader(Headers.ODPS_REQUEST_ID));
    assertEquals("an absent body is reported as no body, not as null",
                 0, resp.getBody().length);
  }

  // ---- helpers ---------------------------------------------------------------------------------

  private RestClient newClient() {
    RestClient client = new RestClient(new DefaultTransport());
    client.setAccount(new AliyunAccount("test-ak", "test-sk"));
    client.setEndpoint(endpoint);
    client.setConnectTimeout(2);
    client.setReadTimeout(5);
    // One attempt everywhere except the two cells that are about retrying, so each message
    // describes exactly the response the test just wrote.
    client.setRetryTimes(0);
    return client;
  }

  /**
   * The status is the fact the whole ticket is about, so it is read through a helper: when it is
   * missing the failure says "the status must reach the caller" and quotes the text the caller was
   * left with, instead of failing as an unexplained NullPointerException.
   */
  private static int statusOf(OdpsException e) {
    assertTrue("the HTTP status must reach the caller; the exception was "
                   + e.getClass().getSimpleName() + ": " + e.getMessage(),
               e.getStatus() != null);
    return e.getStatus().intValue();
  }

  private OdpsException expectFailure(RestClient client, String method, byte[] body,
                                      String shape) {
    try {
      Response resp = client.request(RESOURCE, method, null, null,
                                     body == null ? null : new java.io.ByteArrayInputStream(body),
                                     body == null ? 0 : body.length);
      throw new AssertionError("expected an OdpsException, got status " + resp.getStatus()
                               + " and body " + Arrays.toString(resp.getBody()));
    } catch (OdpsException e) {
      System.out.println("ERRORCELL shape=" + shape
                         + " method=" + method
                         + " status=" + e.getStatus()
                         + " requestId=" + e.getRequestId()
                         + " message=" + firstLine(String.valueOf(e.getMessage())));
      return e;
    }
  }

  /** The message must be usable on its own: non-blank, and it names the facts that locate the call. */
  private static void assertMessageCarries(String message, String status, String requestId) {
    assertFalse("an error with no readable body must still produce a message, was: " + message,
                message == null || message.trim().isEmpty());
    assertFalse("the caller must not be handed a connection-level text, was: " + message,
                message.contains("Invalid connection"));
    assertTrue("message must carry the HTTP status " + status + ", was: " + message,
               message.contains(status));
    if (requestId != null) {
      assertTrue("message must carry the request id, was: " + message, message.contains(requestId));
    }
  }

  private static Map<String, String> headers(String requestId, String contentType) {
    Map<String, String> headers = new LinkedHashMap<>();
    if (requestId != null) {
      headers.put(Headers.ODPS_REQUEST_ID, requestId);
    }
    if (contentType != null) {
      headers.put("Content-Type", contentType);
    }
    return headers;
  }

  private static byte[] gzip(byte[] plain) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    OutputStream gz = new GZIPOutputStream(out);
    gz.write(plain);
    gz.close();
    return out.toByteArray();
  }

  private static String describe(Throwable t) {
    return t.getClass().getName() + ": " + t.getMessage()
           + (t.getCause() == null ? "" : " cause=" + t.getCause());
  }

  private static String firstLine(String s) {
    String line = s.replace('\n', ' ');
    return line.length() > 160 ? line.substring(0, 160) : line;
  }

  /**
   * A loopback HTTP server that writes its response bytes verbatim, so the framing under test
   * ({@code Content-Length: 0}, a declared length that is never filled) is stated by the test
   * instead of implied.
   *
   * <p>It answers every request on a fresh connection with whatever is currently configured, and
   * counts the requests, so a cell can tell "retried" from "answered once" and "the proxy refused"
   * from "the origin refused". It also remembers the last request line, which is how the absolute
   * form of a proxied request is checked. No credentials are ever written or echoed.
   */
  private static final class RawServer implements AutoCloseable {

    /** How the answer stops short of what its head promised. */
    private enum Abort { NONE, CHUNKED, STALL }

    private final ServerSocket listener;
    private final AtomicInteger hits = new AtomicInteger();
    private final List<String> requestLines = Collections.synchronizedList(new ArrayList<String>());
    private volatile int status;
    private volatile String reason;
    private volatile Map<String, String> headers;
    private volatile byte[] body;
    /** NONE sends the body as declared; SHORT/CHUNKED abort before the declared end. */
    private volatile Abort abort = Abort.NONE;
    /** The length the head announces; larger than the payload for the aborting styles. */
    private volatile int declaredLength;
    private volatile boolean closed;

    RawServer(int status, String reason, Map<String, String> headers, String body)
        throws IOException {
      this(status, reason, headers, body.getBytes(StandardCharsets.UTF_8));
    }

    RawServer(int status, String reason, Map<String, String> headers, byte[] body)
        throws IOException {
      this.status = status;
      this.reason = reason;
      this.headers = new LinkedHashMap<>(headers);
      this.body = body.clone();
      this.declaredLength = this.body.length;
      this.abort = Abort.NONE;
      listener = new ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"));
      Thread acceptor = new Thread(new Runnable() {
        @Override
        public void run() {
          acceptLoop();
        }
      }, "raw-error-server");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    int port() {
      return listener.getLocalPort();
    }

    int hits() {
      return hits.get();
    }

    String lastRequestLine() {
      synchronized (requestLines) {
        return requestLines.isEmpty() ? "" : requestLines.get(requestLines.size() - 1);
      }
    }

    void answer(int status, String reason, Map<String, String> headers, String body) {
      answer(status, reason, headers, body.getBytes(StandardCharsets.UTF_8));
    }

    void answer(int status, String reason, Map<String, String> headers, byte[] body) {
      this.status = status;
      this.reason = reason;
      this.headers = new LinkedHashMap<>(headers);
      this.body = body.clone();
      this.declaredLength = this.body.length;
      this.abort = Abort.NONE;
    }

    /**
     * A {@code 500} whose chunked body is announced at 1 KiB and stops after 9 bytes: the reader
     * sees an end of stream where a chunk promised more, which is what a gateway that hangs up
     * mid-response looks like from the client side.
     */
    void abortChunked500() {
      Map<String, String> withId = new LinkedHashMap<>();
      withId.put(Headers.ODPS_REQUEST_ID, REQUEST_ID);
      withId.put("Transfer-Encoding", "chunked");
      answer(500, "Internal Server Error", withId, "truncated".getBytes(StandardCharsets.UTF_8));
      this.abort = Abort.CHUNKED;
    }

    /** The same abort on a success response, for the control that must keep failing. */
    void abortChunked200() {
      Map<String, String> withId = new LinkedHashMap<>();
      withId.put(Headers.ODPS_REQUEST_ID, REQUEST_ID);
      withId.put("Transfer-Encoding", "chunked");
      answer(200, "OK", withId, "truncated".getBytes(StandardCharsets.UTF_8));
      this.abort = Abort.CHUNKED;
    }

    /** A {@code 500} that sends the head and 9 bytes, then stalls past the read timeout. */
    void stallAfterStartingErrorBody() {
      answer(500, "Internal Server Error",
             Collections.singletonMap(Headers.ODPS_REQUEST_ID, REQUEST_ID),
             "truncated".getBytes(StandardCharsets.UTF_8));
      this.declaredLength = 4096;
      this.abort = Abort.STALL;
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
          final Socket client = listener.accept();
          Thread worker = new Thread(new Runnable() {
            @Override
            public void run() {
              serve(client);
            }
          }, "raw-error-server-conn");
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
        OutputStream out = socket.getOutputStream();
        byte[] payload = body;
        Abort style = abort;
        if (style == Abort.CHUNKED) {
          out.write(headBytes());
          out.write((Integer.toHexString(declaredLength) + "\r\n").getBytes(StandardCharsets.US_ASCII));
          out.write(payload);
          out.flush();
          return; // hanging up mid-chunk is the point of this answer
        }
        out.write(headBytes());
        out.write(payload, 0, Math.min(payload.length, declaredLength));
        out.flush();
        if (style == Abort.STALL) {
          try {
            Thread.sleep(30_000L); // the reader gives up first; the close below is the response
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      } catch (IOException ignored) {
        // Either the response went out and the client closed, or the client is gone. hits was taken
        // before writing, so the counters stay honest either way.
      }
    }

    private byte[] headBytes() {
      StringBuilder head = new StringBuilder("HTTP/1.1 ")
          .append(status).append(' ').append(reason).append("\r\n");
      for (Map.Entry<String, String> header : headers.entrySet()) {
        head.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
      }
      if (abort != Abort.CHUNKED) {
        head.append("Content-Length: ").append(declaredLength).append("\r\n");
      }
      head.append("Connection: close\r\n\r\n");
      return head.toString().getBytes(StandardCharsets.US_ASCII);
    }

    private void drainRequest(InputStream in) throws IOException {
      ByteArrayOutputStream seen = new ByteArrayOutputStream();
      int b;
      while ((b = in.read()) != -1) {
        seen.write(b);
        byte[] bytes = seen.toByteArray();
        if (endsHeaderBlock(bytes)) {
          rememberRequestLine(new String(bytes, StandardCharsets.US_ASCII));
          skipDeclaredBody(in, contentLengthOf(new String(bytes, StandardCharsets.US_ASCII)));
          return;
        }
        if (bytes.length > 65536) {
          throw new InterruptedIOException("request head too large");
        }
      }
    }

    private void rememberRequestLine(String head) {
      int end = head.indexOf("\r\n");
      requestLines.add(end < 0 ? head : head.substring(0, end));
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
