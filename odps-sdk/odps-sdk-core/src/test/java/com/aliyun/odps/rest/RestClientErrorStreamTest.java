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

package com.aliyun.odps.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Offline boundary tests for the REST error path: they use a loopback HTTP server only, so they
 * need no credentials, endpoint or MaxCompute project.
 */
public class RestClientErrorStreamTest {

  private FakeHttpServer server;
  private RestClient client;

  @Before
  public void setUp() throws IOException {
    server = new FakeHttpServer();
    client = newClient(server.port());
    client.disableDeprecatedLogger();
  }

  @After
  public void tearDown() throws IOException {
    server.close();
  }

  private static RestClient newClient(int port) {
    Odps odps = new Odps(new AliyunAccount("test-id", "test-key"));
    odps.setEndpoint("http://127.0.0.1:" + port);
    odps.setDefaultProject("test_project");
    RestClient restClient = odps.getRestClient();
    restClient.setConnectTimeout(2);
    restClient.setReadTimeout(5);
    restClient.setRetryWaitTime(1);
    restClient.disableDeprecatedLogger();
    return restClient;
  }

  /**
   * An error response is a server answer, not a connection failure. With an empty body the JDK
   * returns no error stream at all, so the HTTP status used to be dropped: callers got a plain
   * OdpsException("Invalid connection.") with no status, and, because the 4xx was no longer
   * visible, the retry filter replayed the request until the retry budget was exhausted.
   */
  @Test
  public void emptyBody404KeepsStatusAndIsNotRetried() {
    server.respond(404, "", 0);
    try {
      client.request("/projects/test_project/missing", "GET", null, null, null, 0);
      fail("expected NoSuchObjectException");
    } catch (OdpsException e) {
      assertTrue("expected NoSuchObjectException but got " + e.getClass().getName()
                     + ": " + e.getMessage(), e instanceof NoSuchObjectException);
      assertEquals("HTTP status must survive an empty error body",
                   Integer.valueOf(404), e.getStatus());
    }
    assertEquals("a permanent 4xx must not be replayed", 1, server.requestCount());
  }

  /** Same for a 5xx that carries no body: retry policy still applies, but the status is visible. */
  @Test
  public void emptyBody503KeepsHttpStatus() {
    server.respond(503, "", 1);
    client.setRetryTimes(1);
    try {
      client.request("/projects/test_project/x", "GET", null, null, null, 0);
      fail("expected OdpsException");
    } catch (OdpsException e) {
      assertEquals(Integer.valueOf(503), e.getStatus());
    }
    assertEquals(2, server.requestCount());
  }

  /** Control: a normal error body keeps working (guards the fix from regressing the common path). */
  @Test
  public void errorBodyXmlStillMaps() {
    server.respond(403,
                   "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>Forbidden</Code>"
                       + "<Message>no permission</Message><RequestId>r-1</RequestId></Error>", 0);
    try {
      client.request("/projects/test_project/y", "GET", null, null, null, 0);
      fail("expected OdpsException");
    } catch (OdpsException e) {
      assertEquals(Integer.valueOf(403), e.getStatus());
      assertTrue(e.getMessage().contains("no permission"));
    }
    assertEquals(1, server.requestCount());
  }

  /**
   * Retries sleep in a back-off window; interrupting that sleep is how a caller cancels the
   * request. Thread.sleep clears the interrupt flag when it throws, so the flag has to be put back
   * before the failure is reported, otherwise the cancel signal disappears into a generic
   * OdpsException and the cancelling thread pool cannot observe it.
   */
  @Test(timeout = 60_000)
  public void interruptDuringRetryBackoffKeepsInterruptFlag() throws Exception {
    server.respond(500,
                   "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>Internal</Code>"
                       + "<Message>boom</Message></Error>", 4);
    client.setRetryTimes(4);

    final AtomicInteger observedFlag = new AtomicInteger(-1);
    final Throwable[] thrown = new Throwable[1];
    Thread worker = new Thread(() -> {
      try {
        client.request("/projects/test_project/slow", "GET", null, null, null, 0);
      } catch (Throwable t) {
        thrown[0] = t;
      }
      observedFlag.set(Thread.currentThread().isInterrupted() ? 1 : 0);
    }, "rest-cancel-test");
    worker.setDaemon(true);
    worker.start();

    // wait until the first attempt has been answered, then cancel while it is backing off
    long deadline = System.currentTimeMillis() + 20_000;
    while (server.requestCount() == 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    worker.interrupt();
    worker.join(30_000);
    assertFalse("worker did not finish after cancel", worker.isAlive());

    assertTrue("expected an OdpsException, got " + thrown[0], thrown[0] instanceof OdpsException);
    assertEquals("interrupt status must survive the retry back-off", 1, observedFlag.get());
    // drain the flag so the assertion above does not leak into other tests
    Thread.interrupted();
  }

  /** Minimal HTTP/1.1 server: replies with the configured status/body to every request. */
  private static final class FakeHttpServer implements AutoCloseable {

    private final ServerSocket socket;
    private final AtomicInteger requests = new AtomicInteger();
    private volatile int status = 200;
    private volatile byte[] body = new byte[0];
    private volatile int extraAttemptsAfterFirst = 0;

    FakeHttpServer() throws IOException {
      socket = new ServerSocket();
      socket.setReuseAddress(true);
      socket.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
      Thread acceptor = new Thread(this::acceptLoop, "fake-http-acceptor");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    int port() {
      return socket.getLocalPort();
    }

    int requestCount() {
      return requests.get();
    }

    void respond(int status, String body, int extraAttemptsAfterFirst) {
      this.status = status;
      this.body = body.getBytes(StandardCharsets.UTF_8);
      this.extraAttemptsAfterFirst = extraAttemptsAfterFirst;
    }

    private void acceptLoop() {
      while (!socket.isClosed()) {
        try {
          Socket accepted = socket.accept();
          Thread worker = new Thread(() -> serve(accepted), "fake-http-connection");
          worker.setDaemon(true);
          worker.start();
        } catch (IOException e) {
          return;
        }
      }
    }

    private void serve(Socket accepted) {
      try (Socket s = accepted) {
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.ISO_8859_1));
        String line;
        int contentLength = 0;
        while ((line = reader.readLine()) != null && !line.isEmpty()) {
          if (line.toLowerCase().startsWith("content-length:")) {
            contentLength = Integer.parseInt(line.substring(15).trim());
          }
        }
        int read = 0;
        while (read < contentLength) {
          int c = reader.read();
          if (c < 0) {
            break;
          }
          read++;
        }
        requests.incrementAndGet();
        byte[] payload = this.body;
        StringBuilder head = new StringBuilder();
        head.append("HTTP/1.1 ").append(status).append(" Status\r\n")
            .append("Content-Length: ").append(payload.length).append("\r\n")
            .append("Content-Type: application/xml\r\n")
            .append("Connection: close\r\n\r\n");
        OutputStream out = s.getOutputStream();
        out.write(head.toString().getBytes(StandardCharsets.ISO_8859_1));
        if (payload.length > 0) {
          out.write(payload);
        }
        out.flush();
      } catch (IOException ignored) {
        // the client went away; the counters above are what the test asserts on
      }
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }
}
