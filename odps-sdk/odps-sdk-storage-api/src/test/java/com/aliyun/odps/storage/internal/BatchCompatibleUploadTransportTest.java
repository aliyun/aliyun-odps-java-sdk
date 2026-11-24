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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.storage.internal.io.StreamingUpload;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(20)
class BatchCompatibleUploadTransportTest {
  private final ExecutorService executor =
      Executors.newCachedThreadPool(
          runnable -> {
            Thread thread = new Thread(runnable, "upload-test-peer");
            thread.setDaemon(true);
            return thread;
          });
  private final List<HttpClient> clients = new ArrayList<>();
  private final List<HttpServer> servers = new ArrayList<>();
  private final List<StreamingUpload> uploads = new ArrayList<>();
  private static final int CHUNK = 4 * 1024 * 1024;

  @AfterEach
  void cleanup() throws Exception {
    for (StreamingUpload upload : uploads) upload.close();
    for (HttpClient client : clients) client.shutdown();
    for (HttpServer server : servers) server.stop(0);
    executor.shutdownNow();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
  }

  private HttpClient client(String endpoint, HttpSettings settings) {
    ICredentials credentials = mock(ICredentials.class);
    when(credentials.getAccessKeyId()).thenReturn("test-id");
    when(credentials.getAccessKeySecret()).thenReturn("test-key");
    ICredentialsProvider provider = mock(ICredentialsProvider.class);
    when(provider.getCredentials()).thenReturn(credentials);
    HttpClient client =
        new HttpClient(
            StubSettings.newBuilder()
                .withEndpoint(endpoint)
                .withTunnelEndpoint(endpoint)
                .withProject("project")
                .withCredentialsProvider(provider)
                .withHttpSettings(settings)
                .build());
    clients.add(client);
    return client;
  }

  private StreamingUpload upload(HttpClient client, int chunk) {
    StreamingUpload upload =
        client.openStreamingUpload("upload", Collections.emptyMap(), Collections.emptyMap(), chunk);
    uploads.add(upload);
    return upload;
  }

  private HttpServer server(com.sun.net.httpserver.HttpHandler handler) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 32);
    server.setExecutor(executor);
    server.createContext("/", handler);
    server.start();
    servers.add(server);
    return server;
  }

  private String endpoint(HttpServer server) {
    return (server instanceof HttpsServer ? "https" : "http")
        + "://127.0.0.1:"
        + server.getAddress().getPort();
  }

  private byte[] read(InputStream input) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    byte[] data = new byte[65536];
    int count;
    while ((count = input.read(data)) != -1) bytes.write(data, 0, count);
    return bytes.toByteArray();
  }

  private void respond(HttpExchange exchange, byte[] body) throws IOException {
    exchange.sendResponseHeaders(200, body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
    exchange.close();
  }

  @Test
  void flushMakesPartialBodyVisibleBeforeEofAndDoesNotCommit() throws Exception {
    byte[] data = new byte[8192];
    CountDownLatch bodySeen = new CountDownLatch(1);
    CountDownLatch flushedBodySeen = new CountDownLatch(1), eofSeen = new CountDownLatch(1);
    AtomicInteger bytes = new AtomicInteger();
    HttpServer server =
        server(
            exchange -> {
              try (InputStream input = exchange.getRequestBody()) {
                // An HTTP read may return only part of the bytes already flushed by the client.
                byte[] fragment = new byte[512];
                int count;
                while ((count = input.read(fragment)) != -1) {
                  int received = bytes.addAndGet(count);
                  bodySeen.countDown();
                  if (received >= data.length) flushedBodySeen.countDown();
                }
                eofSeen.countDown();
                respond(exchange, "ok".getBytes(StandardCharsets.UTF_8));
              } finally {
                exchange.close();
              }
            });
    StreamingUpload upload =
        upload(client(endpoint(server), HttpSettings.newBuilder().build()), CHUNK);
    upload.outputStream().write(data);
    assertFalse(bodySeen.await(100, TimeUnit.MILLISECONDS));
    upload.outputStream().flush();
    assertTrue(flushedBodySeen.await(5, TimeUnit.SECONDS));
    assertEquals(data.length, bytes.get());
    assertFalse(eofSeen.await(100, TimeUnit.MILLISECONDS));
    assertEquals("ok", upload.finish().getBody());
    assertTrue(eofSeen.await(5, TimeUnit.SECONDS));
  }

  @Test
  void reuseWithDifferentChunkSizesPreservesPayloads() throws Exception {
    Set<Integer> remotePorts = Collections.synchronizedSet(new HashSet<>());
    byte[] data = new byte[111111];
    new Random(1729).nextBytes(data);
    HttpServer server =
        server(
            exchange -> {
              remotePorts.add(exchange.getRemoteAddress().getPort());
              respond(
                  exchange, java.util.Base64.getEncoder().encode(read(exchange.getRequestBody())));
            });
    HttpClient client = client(endpoint(server), HttpSettings.newBuilder().build());
    for (int chunk : new int[] {32768, 131072, CHUNK}) {
      StreamingUpload upload = upload(client, chunk);
      upload.outputStream().write(data);
      assertEquals(java.util.Base64.getEncoder().encodeToString(data), upload.finish().getBody());
    }
    assertEquals(1, remotePorts.size(), "the dedicated upload pool must reuse its own socket");
  }

  @Test
  void moreThanFiveLiveUploadsCanFlushBeforeAnyCloses() throws Exception {
    int count = 12;
    CountDownLatch allBodiesSeen = new CountDownLatch(count), allowEof = new CountDownLatch(1);
    HttpServer server =
        server(
            exchange -> {
              try (InputStream input = exchange.getRequestBody()) {
                assertEquals(7, input.read());
                allBodiesSeen.countDown();
                while (input.read() != -1) {}
                respond(exchange, new byte[] {'o', 'k'});
              } finally {
                exchange.close();
              }
            });
    HttpClient client = client(endpoint(server), HttpSettings.newBuilder().build());
    List<Future<HttpResponse>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < count; i++) {
        StreamingUpload upload = upload(client, CHUNK);
        futures.add(
            executor.submit(
                () -> {
                  upload.outputStream().write(7);
                  upload.outputStream().flush();
                  allowEof.await();
                  return upload.finish();
                }));
      }
      assertTrue(allBodiesSeen.await(5, TimeUnit.SECONDS));
      allowEof.countDown();
      for (Future<HttpResponse> future : futures)
        assertEquals(200, future.get(5, TimeUnit.SECONDS).getStatusCode());
    } finally {
      allowEof.countDown();
    }
  }

  @Test
  void abortUnblocksAnActualBackpressuredSocketWrite() throws Exception {
    try (BlockedPeer peer = new BlockedPeer()) {
      StreamingUpload upload =
          upload(
              client(
                  peer.endpoint(),
                  HttpSettings.newBuilder().withWriteTimeout(Duration.ZERO).build()),
              CHUNK);
      Future<IOException> writing = writeUntilBlocked(upload);
      assertTrue(peer.accepted.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> writing.get(150, TimeUnit.MILLISECONDS));
      upload.close();
      assertNotNull(writing.get(5, TimeUnit.SECONDS));
      assertThrows(IOException.class, upload::finish);
    }
  }

  @Test
  void configuredWriteTimeoutStopsABackpressuredSocket() throws Exception {
    try (BlockedPeer peer = new BlockedPeer()) {
      StreamingUpload upload =
          upload(
              client(
                  peer.endpoint(),
                  HttpSettings.newBuilder().withWriteTimeout(Duration.ofMillis(500)).build()),
              CHUNK);
      IOException error = writeUntilBlocked(upload).get(8, TimeUnit.SECONDS);
      assertNotNull(error);
      assertTrue(error.toString().toLowerCase().contains("time"));
      assertThrows(IOException.class, upload::finish);
    }
  }

  private Future<IOException> writeUntilBlocked(StreamingUpload upload) {
    return executor.submit(
        () ->
            assertThrows(
                IOException.class,
                () -> {
                  byte[] data = new byte[65536];
                  for (int i = 0; i < 2048; i++) upload.outputStream().write(data);
                  upload.finish();
                }));
  }

  @Test
  void abortUnblocksFinishWhileWaitingForResponse() throws Exception {
    CountDownLatch eof = new CountDownLatch(1), release = new CountDownLatch(1);
    HttpServer server =
        server(
            exchange -> {
              read(exchange.getRequestBody());
              eof.countDown();
              try {
                release.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              exchange.close();
            });
    StreamingUpload upload =
        upload(client(endpoint(server), HttpSettings.newBuilder().build()), CHUNK);
    Future<IOException> finishing =
        executor.submit(() -> assertThrows(IOException.class, upload::finish));
    try {
      assertTrue(eof.await(5, TimeUnit.SECONDS));
      upload.close();
      assertNotNull(finishing.get(5, TimeUnit.SECONDS));
    } finally {
      release.countDown();
    }
  }

  @Test
  void configuredReadTimeoutFailsInsteadOfReturningACommitResponse() throws Exception {
    CountDownLatch release = new CountDownLatch(1), eof = new CountDownLatch(1);
    HttpServer server =
        server(
            exchange -> {
              read(exchange.getRequestBody());
              eof.countDown();
              try {
                release.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              exchange.close();
            });
    StreamingUpload upload =
        upload(
            client(
                endpoint(server),
                HttpSettings.newBuilder().withReadTimeout(Duration.ofMillis(300)).build()),
            CHUNK);
    try {
      Future<IOException> finishing =
          executor.submit(() -> assertThrows(IOException.class, upload::finish));
      assertTrue(eof.await(5, TimeUnit.SECONDS));
      assertTrue(finishing.get(5, TimeUnit.SECONDS).toString().toLowerCase().contains("time"));
    } finally {
      release.countDown();
    }
  }

  @Test
  void clientShutdownCancelsARequestBodyWaitingForItsProducer() throws Exception {
    try (BlockedPeer peer = new BlockedPeer()) {
      HttpClient client = client(peer.endpoint(), HttpSettings.newBuilder().build());
      StreamingUpload upload = upload(client, CHUNK);
      assertTrue(peer.accepted.await(5, TimeUnit.SECONDS));
      client.shutdown();
      Future<IOException> finishing =
          executor.submit(() -> assertThrows(IOException.class, upload::finish));
      assertNotNull(finishing.get(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void httpErrorPreservesRequestIdAndDoesNotReplayTheBody() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server =
        server(
            exchange -> {
              requests.incrementAndGet();
              read(exchange.getRequestBody());
              byte[] body =
                  "{\"Code\":\"ProbeError\",\"Message\":\"rejected\"}"
                      .getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().add("x-odps-request-id", "probe-request-id");
              exchange.getResponseHeaders().add("Location", "/redirected");
              exchange.sendResponseHeaders(307, body.length);
              try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
              }
              exchange.close();
            });
    StreamingUpload upload =
        upload(client(endpoint(server), HttpSettings.newBuilder().build()), CHUNK);
    upload.outputStream().write(new byte[17]);
    IOException error = assertThrows(IOException.class, upload::finish);
    assertTrue(error.toString().contains("ProbeError"));
    assertTrue(error.toString().contains("probe-request-id"));
    assertEquals(1, requests.get());
    assertThrows(IOException.class, upload::finish);
  }

  @Test
  void dedicatedUploadClientPreservesHttpProxyRouting() throws Exception {
    java.util.concurrent.atomic.AtomicBoolean absolute =
        new java.util.concurrent.atomic.AtomicBoolean();
    HttpServer proxy =
        server(
            exchange -> {
              absolute.set(exchange.getRequestURI().isAbsolute());
              read(exchange.getRequestBody());
              respond(exchange, new byte[] {'o', 'k'});
            });
    HttpSettings settings =
        HttpSettings.newBuilder()
            .withProxyConfig(
                com.aliyun.odps.ProxyConfig.builder().withHttpProxy(endpoint(proxy)).build())
            .build();
    StreamingUpload upload = upload(client("http://storage.invalid", settings), CHUNK);
    upload.outputStream().write(new byte[12345]);
    assertEquals("ok", upload.finish().getBody());
    assertTrue(absolute.get());
  }

  @Test
  void proxyChallengeCannotReplayAOneShotUpload() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer proxy =
        server(
            exchange -> {
              requests.incrementAndGet();
              read(exchange.getRequestBody());
              byte[] body = "proxy authentication required".getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().add("Proxy-Authenticate", "Basic realm=upload-test");
              exchange.sendResponseHeaders(407, body.length);
              try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
              }
              exchange.close();
            });
    HttpSettings settings =
        HttpSettings.newBuilder()
            .withProxyConfig(
                com.aliyun.odps.ProxyConfig.builder().withHttpProxy(endpoint(proxy)).build())
            .build();
    StreamingUpload upload = upload(client("http://storage.invalid", settings), CHUNK);
    upload.outputStream().write(new byte[12345]);
    IOException error = assertThrows(IOException.class, upload::finish);
    assertTrue(error.toString().contains("407"));
    assertEquals(1, requests.get());
  }

  @Test
  void disconnectCannotProduceASuccessfulResponse() throws Exception {
    try (ServerSocket server =
        new ServerSocket(0, 8, java.net.InetAddress.getByName("127.0.0.1"))) {
      Future<?> peer =
          executor.submit(
              () -> {
                try (Socket socket = server.accept()) {
                  socket.setSoLinger(true, 0);
                }
                return null;
              });
      StreamingUpload upload =
          upload(
              client(
                  "http://127.0.0.1:" + server.getLocalPort(), HttpSettings.newBuilder().build()),
              CHUNK);
      assertThrows(
          IOException.class,
          () -> {
            upload.outputStream().write(new byte[8192]);
            upload.finish();
          });
      peer.get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void httpsKeepsNativeTlsAndPreservesPayload() throws Exception {
    HttpsServer server = tlsServer();
    byte[] data = new byte[111111];
    for (int i = 0; i < data.length; i++) data[i] = (byte) ('a' + i % 26);
    HttpClient client =
        client(endpoint(server), HttpSettings.newBuilder().withIgnoreCerts(true).build());
    StreamingUpload upload = upload(client, CHUNK);
    upload.outputStream().write(data);
    assertEquals(new String(data, StandardCharsets.UTF_8), upload.finish().getBody());
  }

  @Test
  void httpsStillRejectsAnUntrustedCertificateByDefault() throws Exception {
    HttpsServer server = tlsServer();
    StreamingUpload upload =
        upload(client(endpoint(server), HttpSettings.newBuilder().build()), CHUNK);
    IOException error = assertThrows(IOException.class, upload::finish);
    assertTrue(error.toString().contains("SSLHandshakeException"));
  }

  @Test
  void httpsFlushAndClientShutdownCancelTheProducerWait() throws Exception {
    CountDownLatch seen = new CountDownLatch(1);
    HttpsServer server =
        tlsServer(
            exchange -> {
              try (InputStream input = exchange.getRequestBody()) {
                byte[] data = new byte[32];
                if (input.read(data) > 0) seen.countDown();
                while (input.read(data) != -1) {}
              } finally {
                exchange.close();
              }
            });
    HttpClient client =
        client(endpoint(server), HttpSettings.newBuilder().withIgnoreCerts(true).build());
    StreamingUpload upload = upload(client, CHUNK);
    upload.outputStream().write(new byte[17]);
    upload.outputStream().flush();
    assertTrue(seen.await(5, TimeUnit.SECONDS));
    client.shutdown();
    Future<IOException> finishing =
        executor.submit(() -> assertThrows(IOException.class, upload::finish));
    assertNotNull(finishing.get(5, TimeUnit.SECONDS));
  }

  @Test
  void httpsWriteTimeoutStillBoundsSocketBackpressure() throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    HttpsServer server =
        tlsServer(
            exchange -> {
              entered.countDown();
              try {
                release.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              exchange.close();
            });
    StreamingUpload upload =
        upload(
            client(
                endpoint(server),
                HttpSettings.newBuilder()
                    .withIgnoreCerts(true)
                    .withWriteTimeout(Duration.ofSeconds(2))
                    .build()),
            CHUNK);
    try {
      Future<IOException> writing = writeUntilBlocked(upload);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertTrue(writing.get(8, TimeUnit.SECONDS).toString().toLowerCase().contains("time"));
      assertThrows(IOException.class, upload::finish);
    } finally {
      release.countDown();
    }
  }

  private HttpsServer tlsServer() throws Exception {
    return tlsServer(exchange -> respond(exchange, read(exchange.getRequestBody())));
  }

  private HttpsServer tlsServer(com.sun.net.httpserver.HttpHandler handler) throws Exception {

    okhttp3.tls.HeldCertificate certificate = new okhttp3.tls.HeldCertificate.Builder()
        .commonName("untrusted.invalid")
        .addSubjectAlternativeName("untrusted.invalid")
        .build();
    SSLContext context = new okhttp3.tls.HandshakeCertificates.Builder()
        .heldCertificate(certificate)
        .build()
        .sslContext();
    HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 8);
    server.setHttpsConfigurator(new HttpsConfigurator(context));
    server.setExecutor(executor);
    server.createContext("/", handler);
    server.start();
    servers.add(server);
    return server;
  }

  private final class BlockedPeer implements AutoCloseable {
    final ServerSocket server = new ServerSocket();
    final CountDownLatch accepted = new CountDownLatch(1), release = new CountDownLatch(1);
    final Future<?> peer;

    BlockedPeer() throws IOException {
      server.setReceiveBufferSize(1024);
      server.bind(new InetSocketAddress("127.0.0.1", 0));
      peer =
          executor.submit(
              () -> {
                try (Socket socket = server.accept()) {
                  accepted.countDown();
                  release.await();
                }
                return null;
              });
    }

    String endpoint() {
      return "http://127.0.0.1:" + server.getLocalPort();
    }

    @Override
    public void close() throws Exception {
      release.countDown();
      server.close();
      peer.get(5, TimeUnit.SECONDS);
    }
  }
}
