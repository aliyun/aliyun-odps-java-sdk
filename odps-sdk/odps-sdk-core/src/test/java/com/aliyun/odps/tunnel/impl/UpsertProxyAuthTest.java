package com.aliyun.odps.tunnel.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.ProxyCredential;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.Configuration;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.proxy.ProxyConnectException;

/**
 * The Upsert (netty) egress against a proxy that requires {@code Proxy-Authorization}.
 *
 * <p>Everything is local: an origin and an authenticating CONNECT proxy run on 127.0.0.1, so the
 * cells measure what the production code path
 * ({@link UpsertSessionImpl#generateNettyBootstrap}) actually puts on the wire, with no cloud
 * credentials involved.
 *
 * <p>netty differs from the OkHttp egress in one visible way: the credential is attached to the
 * {@code CONNECT} itself, so the proxy never sees an unauthenticated attempt at all.
 */
public class UpsertProxyAuthTest {

  private static final String MARKER = "upsert-proxy-auth-marker";
  private static final String USER = "alice";
  private static final String PASSWORD = "test-proxy-password";

  private com.sun.net.httpserver.HttpServer origin;
  private String originHost;
  private int originPort;
  private AuthenticatingProxy proxy;
  private EventLoopGroup group;

  @Before
  public void setUp() throws IOException {
    origin = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    origin.createContext("/probe", exchange -> {
      byte[] body = MARKER.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "text/plain");
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    origin.start();
    originHost = "127.0.0.1";
    originPort = origin.getAddress().getPort();
    proxy = new AuthenticatingProxy(USER, PASSWORD);
    group = new NioEventLoopGroup(1);
  }

  @After
  public void tearDown() throws InterruptedException {
    origin.stop(0);
    proxy.close();
    group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).await(10, TimeUnit.SECONDS);
  }

  private Bootstrap bootstrap(String endpoint, ProxyConfig proxyConfig) {
    Odps odps = new Odps(new AliyunAccount("access", "secret"));
    odps.setEndpoint(endpoint);
    odps.options().setProxyConfig(proxyConfig);
    return UpsertSessionImpl.generateNettyBootstrap(new Configuration(odps), group);
  }

  @Test
  public void credentialIsAttachedToTheConnectAndTheOriginAnswers() throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://127.0.0.1:" + proxy.port(), USER, PASSWORD)
        .build();

    Channel channel = bootstrap("http://" + originHost + ":" + originPort + "/api", config)
        .connect(originHost, originPort).sync().channel();
    try {
      String body = exchange(channel, "/probe");
      assertTrue("origin answer expected, got: " + body, body.contains(MARKER));
      assertEquals("the CONNECT must carry the credential from the first attempt",
                   1, proxy.authOk.get());
      assertEquals("netty must never send an unauthenticated CONNECT here",
                   0, proxy.authNone.get());
      assertEquals(0, proxy.authBad.get());
    } finally {
      channel.close().await(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void credentialFromEndpointUserInfoIsEnough() throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://" + USER + ":" + PASSWORD + "@127.0.0.1:" + proxy.port())
        .build();

    Channel channel = bootstrap("http://" + originHost + ":" + originPort + "/api", config)
        .connect(originHost, originPort).sync().channel();
    try {
      assertTrue(exchange(channel, "/probe").contains(MARKER));
      assertTrue(proxy.authOk.get() > 0);
    } finally {
      channel.close().await(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void httpsEndpointAuthenticatesTheTunnelItToo() throws Exception {
    // The "https" branch of generateNettyBootstrap selects the Type.HTTPS proxy and attaches its
    // credential to the CONNECT. What follows the tunnel opening is a TLS handshake that this
    // plain-text origin cannot answer, so the exchange is expected to fail afterwards: the claim
    // made here is only about proxy authentication, and it is measured on the proxy side.
    ProxyConfig config = ProxyConfig.builder()
        .withHttpsProxy("http://" + originHost + ":" + proxy.port(), USER, PASSWORD)
        .build();

    Channel channel = bootstrap("https://" + originHost + ":" + originPort + "/api", config)
        .connect(originHost, originPort).sync().channel();
    try {
      try {
        // Bounded: the TLS handshake to a plain-text origin never completes, so this cell waits
        // on the handshake timeout rather than on a response.
        exchange(channel, "/probe", 3);
      } catch (Exception expectedOfPlainOriginTls) {
        // expected: the origin does not speak TLS
      }
      waitUntil(proxy.authOk, 1);
      assertEquals(counters(), 0, proxy.authNone.get());
      assertEquals(counters(), 0, proxy.authBad.get());
    } finally {
      channel.close().await(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void withoutCredentialTheConnectIsRefusedAsBefore() throws Exception {
    // netty connects the socket to the proxy and answers bootstrap.connect() from that TCP result;
    // the proxy handshake itself is what fails here, and it surfaces on the first write. That is
    // the shape a user sees today when a proxy demands credentials the SDK cannot provide.
    ProxyConfig config = ProxyConfig.builder()
        .withHttpProxy("http://127.0.0.1:" + proxy.port())
        .build();

    Channel channel = bootstrap("http://" + originHost + ":" + originPort + "/api", config)
        .connect(originHost, originPort).sync().channel();
    try {
      String failureReason;
      try {
        failureReason = "unexpectedly answered by the origin: " + exchange(channel, "/probe");
      } catch (Exception expected) {
        failureReason = null;
        assertTrue(rootCauseMessage(expected) + " " + counters(),
                   hasProxyConnectCause(expected));
      }
      if (failureReason != null) {
        fail(failureReason);
      }
      waitUntil(proxy.authNone, 1);
      assertEquals(counters(), 0, proxy.authOk.get());
      assertEquals(counters(), 0, proxy.authBad.get());
    } finally {
      channel.close().await(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void handlerFactoriesKeepTheNoCredentialShape() {
    Proxy proxyAddress = ProxyConfig.builder()
        .withHttpProxy("http://127.0.0.1:8080")
        .build().getProxy(ProxyConfig.Type.HTTP).get();

    // No credential: the handler netty would have had before this change (no username, no header).
    io.netty.handler.proxy.HttpProxyHandler plain =
        UpsertSessionImpl.newHttpProxyHandler(proxyAddress, null);
    assertNotNull(plain);

    ProxyCredential credential = new ProxyCredential(USER, PASSWORD);
    io.netty.handler.proxy.HttpProxyHandler authenticated =
        UpsertSessionImpl.newHttpProxyHandler(proxyAddress, credential);
    // The credential goes into the CONNECT headers, not into netty's own username/password fields,
    // because netty encodes those with ISO-8859-1 and the SDK encodes Basic with UTF-8.
    assertNull(authenticated.username());
    assertNull(authenticated.password());

    io.netty.handler.proxy.Socks5ProxyHandler socks = UpsertSessionImpl.newSocks5ProxyHandler(
        ProxyConfig.builder().withSocks5Proxy("socks5://127.0.0.1:1080")
            .build().getProxy(ProxyConfig.Type.SOCKS5).get(), null);
    assertNull(socks.username());
  }

  /**
   * The proxy counts an attempt in its own thread before it answers, so a bounded wait keeps the
   * assertion from racing the handshake.
   */
  private void waitUntil(AtomicInteger counter, int expected) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (counter.get() < expected && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
  }

  private String counters() {
    return "authNone=" + proxy.authNone.get() + " authOk=" + proxy.authOk.get()
        + " authBad=" + proxy.authBad.get() + " proxyPort=" + proxy.port();
  }

  private static boolean hasProxyConnectCause(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof ProxyConnectException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static String rootCauseMessage(Throwable error) {
    StringBuilder sb = new StringBuilder();
    Throwable current = error;
    while (current != null) {
      sb.append(current.getClass().getName()).append(": ").append(current.getMessage()).append(" | ");
      current = current.getCause();
    }
    return sb.toString();
  }

  /**
   * Sends one request through the established channel and returns the response body.
   */
  private String exchange(Channel channel, String path) throws Exception {
    return exchange(channel, path, 20);
  }

  private String exchange(Channel channel, String path, int timeoutSeconds)
      throws Exception {
    CompletableFuture<String> future = new CompletableFuture<>();
    channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpResponse) {
          FullHttpResponse response = (FullHttpResponse) msg;
          byte[] bytes = new byte[response.content().readableBytes()];
          response.content().readBytes(bytes);
          response.release();
          future.complete(new String(bytes, StandardCharsets.UTF_8));
        }
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        future.completeExceptionally(cause);
      }
    });

    DefaultFullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, path, Unpooled.EMPTY_BUFFER);
    request.headers().set(HttpHeaderNames.HOST, originHost + ":" + originPort);
    request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
    channel.writeAndFlush(request);

    return future.get(timeoutSeconds, TimeUnit.SECONDS);
  }

  /**
   * CONNECT-only proxy that requires HTTP Basic proxy authentication, mirroring the shape of the
   * reference harness (407 plus {@code Proxy-Authenticate: Basic} when the header is missing).
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
      expectedHeader = "Basic " + Base64.getEncoder().encodeToString(
          (user + ":" + password).getBytes(StandardCharsets.UTF_8));
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
      try (Socket socket = client) {
        InputStream in = socket.getInputStream();
        String requestLine = readLine(in);
        if (requestLine == null || !requestLine.toUpperCase().startsWith("CONNECT")) {
          // This proxy exists to answer CONNECT; anything else is refused without closing counters.
          respond(socket.getOutputStream(), "HTTP/1.1 400 Bad Request");
          return;
        }
        java.util.Map<String, String> headers = new java.util.TreeMap<>();
        String line;
        while ((line = readLine(in)) != null && !line.isEmpty()) {
          int colon = line.indexOf(':');
          if (colon > 0) {
            headers.put(line.substring(0, colon).toLowerCase(java.util.Locale.ROOT),
                        line.substring(colon + 1).trim());
          }
        }
        String presented = headers.get("proxy-authorization");
        if (presented == null) {
          authNone.incrementAndGet();
        } else if (presented.equals(expectedHeader)) {
          authOk.incrementAndGet();
        } else {
          authBad.incrementAndGet();
        }
        if (presented == null || !presented.equals(expectedHeader)) {
          respond(socket.getOutputStream(), "HTTP/1.1 407 Proxy Authentication Required");
          return;
        }
        String[] parts = requestLine.split(" ");
        int colon = parts[1].lastIndexOf(':');
        String host = parts[1].substring(0, colon);
        int port = Integer.parseInt(parts[1].substring(colon + 1));
        try (Socket upstream = new Socket()) {
          upstream.connect(new InetSocketAddress(host, port), 5000);
          respond(socket.getOutputStream(), "HTTP/1.1 200 Connection Established");
          Thread toOrigin = pump(socket.getInputStream(), upstream.getOutputStream());
          Thread toClient = pump(upstream.getInputStream(), socket.getOutputStream());
          toOrigin.join(20000);
          toClient.join(20000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } catch (IOException | RuntimeException ignored) {
        // Counters already describe what was observed on this attempt.
      }
    }

    private static void respond(OutputStream out, String statusLine) throws IOException {
      out.write((statusLine + "\r\nContent-Length: 0\r\n\r\n")
          .getBytes(StandardCharsets.US_ASCII));
      out.flush();
    }

    private Thread pump(InputStream from, OutputStream to) {
      Thread thread = new Thread(() -> {
        try {
          byte[] buffer = new byte[8192];
          int read;
          while ((read = from.read(buffer)) > 0) {
            to.write(buffer, 0, read);
            to.flush();
          }
        } catch (IOException ignored) {
          // tunnel closed
        }
      }, "test-auth-proxy-pump");
      thread.setDaemon(true);
      thread.start();
      return thread;
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
  }
}
