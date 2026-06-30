package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.HttpStatus;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.utils.FixedNettyChannelPool;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class UpsertStreamImplReproTest {

  private static final int SOCKET_TIMEOUT_SECONDS = 5;

  @Test
  public void flushRetriesWhenServerStopsReadingRequestBody() throws Exception {
    try (SessionServer server = new SessionServer()) {
      server.start();

      AtomicReference<Throwable> retryError = new AtomicReference<>();
      AtomicInteger retryLogs = new AtomicInteger();
      UpsertSessionImpl session = newSession(server.getPort(), retryError, retryLogs);
      CountDownLatch requestWriteStarted = new CountDownLatch(1);
      AtomicReference<EmbeddedChannel> activeChannel = new AtomicReference<>();
      AtomicInteger writeAttempts = new AtomicInteger();
      installRetryingRequestBodyChannelPool(
          session, requestWriteStarted, activeChannel, writeAttempts);

      UpsertStream stream = session.buildUpsertStream().build();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Future<Void> flushFuture = null;
      try {
        Record record = session.newRecord();
        record.setBigint("id", 1L);
        stream.upsert(record);

        flushFuture = executor.submit(() -> {
          stream.flush();
          return null;
        });

        Assert.assertTrue(requestWriteStarted.await(5, TimeUnit.SECONDS));
        EmbeddedChannel stalledChannel = activeChannel.get();
        Assert.assertNull(stalledChannel.pipeline().get(ReadTimeoutHandler.class));

        stalledChannel.advanceTimeBy(SOCKET_TIMEOUT_SECONDS + 1L, TimeUnit.SECONDS);
        stalledChannel.runScheduledPendingTasks();
        stalledChannel.runPendingTasks();

        waitForWriteAttempts(writeAttempts, 2);
        flushFuture.get(5, TimeUnit.SECONDS);

        Assert.assertEquals(1, server.getSessionRequestCount());
        Assert.assertEquals(2, writeAttempts.get());
        Assert.assertEquals(1, retryLogs.get());
        Assert.assertTrue(retryError.get() instanceof TunnelException);

        TunnelException retryException = (TunnelException) retryError.get();
        Assert.assertEquals(
            Integer.valueOf(HttpStatus.INTERNAL_SERVER_ERROR), retryException.getStatus());
        Assert.assertEquals(
            TunnelConstants.UPSERT_FLUSH_WRITE_TIMEOUT, retryException.getErrorCode());
        Assert.assertTrue(retryException.getMessage().contains("Flush write timed out"));
        Assert.assertTrue(
            retryException.getMessage().contains("stopped reading the request body"));
      } finally {
        if (flushFuture != null) {
          flushFuture.cancel(true);
        }
        executor.shutdownNow();
        EmbeddedChannel channel = activeChannel.get();
        if (channel != null) {
          channel.finishAndReleaseAll();
        }
        session.close();
      }
    }
  }

  private static UpsertSessionImpl newSession(
      int port, AtomicReference<Throwable> retryError, AtomicInteger retryLogs) throws Exception {
    Odps odps = new Odps(new AliyunAccount("accessId", "accessKey"));
    odps.setDefaultProject("test_project");
    odps.setEndpoint("http://127.0.0.1:" + port);
    odps.setTunnelEndpoint("http://127.0.0.1:" + port);

    Configuration config = Configuration.builder(odps)
        .withRetryLogger(new RestClient.RetryLogger() {
          @Override
          public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
            retryError.compareAndSet(null, e);
            retryLogs.incrementAndGet();
          }
        })
        .build();
    config.setEndpoint(new URI("http://127.0.0.1:" + port));
    config.setSocketConnectTimeout(2);
    config.setSocketTimeout(SOCKET_TIMEOUT_SECONDS);

    return new UpsertSessionImpl.Builder()
        .setConfig(config)
        .setProjectName("test_project")
        .setTableName("test_table")
        .setUpsertId("test-upsert-id")
        .setSlotNum(1)
        .setConcurrentNum(1)
        .setNetworkThreadNum(1)
        .build();
  }

  private static void installRetryingRequestBodyChannelPool(
      UpsertSessionImpl session,
      CountDownLatch requestWriteStarted,
      AtomicReference<EmbeddedChannel> activeChannel,
      AtomicInteger writeAttempts)
      throws Exception {
    FixedNettyChannelPool channelPool = new FixedNettyChannelPool(1, () -> {
      EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
          int attempt = writeAttempts.incrementAndGet();
          ReferenceCountUtil.release(msg);
          if (attempt == 1) {
            requestWriteStarted.countDown();
            return;
          }
          promise.setSuccess();
          ((EmbeddedChannel) ctx.channel()).writeInbound(successResponse());
        }
      });
      activeChannel.set(channel);
      return channel;
    });

    Field channelPoolField = UpsertSessionImpl.class.getDeclaredField("channelPool");
    channelPoolField.setAccessible(true);
    channelPoolField.set(session, channelPool);
  }

  private static DefaultFullHttpResponse successResponse() {
    DefaultFullHttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.EMPTY_BUFFER);
    response.headers().set(HttpHeaders.HEADER_ODPS_REQUEST_ID, "retry-success");
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
    return response;
  }

  private static void waitForWriteAttempts(AtomicInteger writeAttempts, int expected)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
    while (writeAttempts.get() < expected && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    Assert.assertTrue("write attempts: " + writeAttempts.get(), writeAttempts.get() >= expected);
  }

  private static final class SessionServer implements Closeable {
    private final ServerSocket serverSocket;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final AtomicInteger sessionRequestCount = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(true);

    private SessionServer() throws IOException {
      this.serverSocket = new ServerSocket(0);
    }

    void start() {
      executor.submit(() -> {
        while (running.get()) {
          try {
            Socket socket = serverSocket.accept();
            executor.submit(() -> handle(socket));
          } catch (IOException e) {
            if (running.get()) {
              throw new RuntimeException(e);
            }
            return;
          }
        }
      });
    }

    int getPort() {
      return serverSocket.getLocalPort();
    }

    int getSessionRequestCount() {
      return sessionRequestCount.get();
    }

    private void handle(Socket socket) {
      try (Socket closeable = socket;
           BufferedInputStream input = new BufferedInputStream(closeable.getInputStream());
           BufferedOutputStream output = new BufferedOutputStream(closeable.getOutputStream())) {
        Request request = readRequest(input);
        if ("GET".equals(request.method)) {
          sessionRequestCount.incrementAndGet();
          writeJsonResponse(output, sessionJson());
          return;
        }
        writeNotFound(output);
      } catch (IOException ignored) {
      }
    }

    private static Request readRequest(BufferedInputStream input) throws IOException {
      String requestLine = readLine(input);
      if (requestLine == null || requestLine.isEmpty()) {
        throw new IOException("missing request line");
      }
      String[] parts = requestLine.split(" ");
      Map<String, String> headers = new HashMap<>();
      String line;
      while ((line = readLine(input)) != null && !line.isEmpty()) {
        int sep = line.indexOf(':');
        if (sep > 0) {
          headers.put(line.substring(0, sep).trim().toLowerCase(Locale.ROOT),
              line.substring(sep + 1).trim());
        }
      }
      int contentLength = Integer.parseInt(headers.getOrDefault("content-length", "0"));
      drainBody(input, contentLength);
      return new Request(parts[0]);
    }

    private static String readLine(BufferedInputStream input) throws IOException {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      int current;
      while ((current = input.read()) != -1) {
        if (current == '\n') {
          break;
        }
        if (current != '\r') {
          buffer.write(current);
        }
      }
      if (current == -1 && buffer.size() == 0) {
        return null;
      }
      return buffer.toString(StandardCharsets.UTF_8.name());
    }

    private static void drainBody(BufferedInputStream input, int contentLength) throws IOException {
      int remaining = contentLength;
      byte[] buffer = new byte[4096];
      while (remaining > 0) {
        int read = input.read(buffer, 0, Math.min(buffer.length, remaining));
        if (read < 0) {
          throw new IOException("unexpected eof while reading request body");
        }
        remaining -= read;
      }
    }

    private static void writeJsonResponse(BufferedOutputStream output, String json)
        throws IOException {
      byte[] body = json.getBytes(StandardCharsets.UTF_8);
      output.write(("HTTP/1.1 200 OK\r\n"
          + "Content-Type: application/json\r\n"
          + "Content-Length: " + body.length + "\r\n"
          + "x-odps-request-id: test-request-id\r\n"
          + "Connection: close\r\n\r\n").getBytes(StandardCharsets.UTF_8));
      output.write(body);
      output.flush();
    }

    private static void writeNotFound(BufferedOutputStream output) throws IOException {
      byte[] body = "not found".getBytes(StandardCharsets.UTF_8);
      output.write(("HTTP/1.1 404 Not Found\r\n"
          + "Content-Length: " + body.length + "\r\n"
          + "Connection: close\r\n\r\n").getBytes(StandardCharsets.UTF_8));
      output.write(body);
      output.flush();
    }

    private String sessionJson() {
      return "{"
          + "\"id\":\"test-upsert-id\","
          + "\"schema\":{\"columns\":[{\"name\":\"id\",\"type\":\"bigint\",\"nullable\":\"false\","
          + "\"column_id\":\"1\"}],\"partitionKeys\":[]},"
          + "\"hash_key\":[\"id\"],"
          + "\"hasher\":\"default\","
          + "\"slots\":[{\"slot_id\":\"0\",\"worker_addr\":\"127.0.0.1:" + getPort()
          + "\",\"buckets\":[0]}],"
          + "\"status\":\"" + TunnelConstants.SESSION_STATUS_NORMAL + "\""
          + "}";
    }

    @Override
    public void close() throws IOException {
      running.set(false);
      serverSocket.close();
      executor.shutdownNow();
    }

    private static final class Request {
      private final String method;

      private Request(String method) {
        this.method = method;
      }
    }
  }
}
