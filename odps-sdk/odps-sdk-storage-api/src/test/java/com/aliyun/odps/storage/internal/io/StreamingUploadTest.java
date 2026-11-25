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

package com.aliyun.odps.storage.internal.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.aliyun.odps.storage.internal.models.HttpResponse;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okio.Buffer;
import okio.Okio;
import okio.Sink;
import okio.Timeout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class StreamingUploadTest {
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final List<StreamingUpload> uploads = new ArrayList<>();

  @AfterEach
  void cleanup() throws Exception {
    for (StreamingUpload upload : uploads) {
      upload.close();
    }
    executor.shutdownNow();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
  }

  private StreamingUpload upload(int capacity, long timeout) {
    StreamingUpload upload = new StreamingUpload(capacity, timeout);
    uploads.add(upload);
    return upload;
  }

  private Call start(StreamingUpload upload, Sink sink) {
    Call call = mock(Call.class);
    upload.start(
        call,
        () -> {
          upload.writeTo(Okio.buffer(sink));
          return new HttpResponse();
        });
    return call;
  }

  private Call stalled(StreamingUpload upload) {
    Call call = mock(Call.class);
    upload.start(
        call,
        () -> {
          new CountDownLatch(1).await();
          return new HttpResponse();
        });
    return call;
  }

  @Test
  void sendsHttpHeadersBeforeDataIsAvailable() throws Exception {
    StreamingUpload upload = upload(4096, 5000);
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
      server.setSoTimeout(5000);
      Request request = new Request.Builder()
          .url("http://127.0.0.1:" + server.getLocalPort() + "/upload")
          .post(upload)
          .build();
      Call call = new OkHttpClient.Builder().callTimeout(10, TimeUnit.SECONDS)
          .build().newCall(request);
      upload.start(call, () -> {
        try (Response response = call.execute()) {
          HttpResponse result = new HttpResponse();
          result.setStatusCode(response.code());
          return result;
        }
      });

      try (Socket connection = server.accept()) {
        connection.setSoTimeout(3000);
        BufferedReader input = new BufferedReader(
            new InputStreamReader(connection.getInputStream(), StandardCharsets.US_ASCII));
        assertEquals("POST /upload HTTP/1.1", input.readLine());
        String header;
        do {
          header = input.readLine();
          assertNotNull(header);
        } while (!header.isEmpty());

        upload.outputStream().write("abc".getBytes(StandardCharsets.US_ASCII));
        upload.outputStream().close();
        assertEquals("3", input.readLine());
        assertEquals("abc", input.readLine());
        assertEquals("0", input.readLine());
        assertEquals("", input.readLine());
        connection.getOutputStream().write(
            ("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .getBytes(StandardCharsets.US_ASCII));
        assertEquals(200, upload.finish().getStatusCode());
      }
    }
  }

  @Test
  void copiesCallerBytesBeforeReturningAndPreservesStream() throws Exception {
    StreamingUpload upload = upload(4096, 5000);
    RecordingSink sink = new RecordingSink();
    start(upload, sink);
    byte[] data = new byte[77777];
    new Random(1729).nextBytes(data);
    byte[] expected = data.clone();
    upload.outputStream().write(data);
    Arrays.fill(data, (byte) 0);
    upload.finish();
    assertArrayEquals(expected, sink.bytes.toByteArray());
    assertTrue(upload.isOneShot());
  }

  @Test
  void flushWaitsForTheActualDownstreamFlush() throws Exception {
    StreamingUpload upload = upload(4096, 5000);
    CountDownLatch flushing = new CountDownLatch(1), release = new CountDownLatch(1);
    start(upload, blockedFlush(flushing, release));
    upload.outputStream().write(new byte[13]);
    Future<?> flushed =
        executor.submit(
            () -> {
              upload.outputStream().flush();
              return null;
            });
    try {
      assertTrue(flushing.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> flushed.get(100, TimeUnit.MILLISECONDS));
      release.countDown();
      flushed.get(5, TimeUnit.SECONDS);
      upload.finish();
    } finally {
      release.countDown();
    }
  }

  @Test
  void flushTimeoutCancelsAndCannotReturnAResponse() throws Exception {
    StreamingUpload upload = upload(4096, 150);
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    Call call = start(upload, blockedFlush(entered, release));
    try {
      upload.outputStream().write(new byte[13]);
      IOException error =
          assertThrows(InterruptedIOException.class, () -> upload.outputStream().flush());
      assertTrue(error.getMessage().contains("timed out"));
      verify(call).cancel();
      assertThrows(IOException.class, upload::finish);
    } finally {
      release.countDown();
    }
  }

  @Test
  void boundedBufferAppliesBackpressureAndTimesOutWithoutProgress() throws Exception {
    StreamingUpload upload = upload(4096, 150);
    Call call = stalled(upload);
    upload.outputStream().write(new byte[4096]);
    IOException error =
        assertThrows(
            InterruptedIOException.class, () -> upload.outputStream().write(new byte[4096]));
    assertTrue(error.getMessage().contains("write timed out"));
    verify(call).cancel();
    assertThrows(IOException.class, upload::finish);
  }

  @Test
  void abortUnblocksAnUntimedProducerAndFinish() throws Exception {
    StreamingUpload upload = upload(4096, 0);
    Call call = stalled(upload);
    upload.outputStream().write(new byte[4096]);
    CountDownLatch entered = new CountDownLatch(1);
    Future<IOException> writing =
        executor.submit(
            () -> {
              entered.countDown();
              return assertThrows(
                  IOException.class, () -> upload.outputStream().write(new byte[4096]));
            });
    assertTrue(entered.await(5, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> writing.get(100, TimeUnit.MILLISECONDS));
    upload.close();
    assertNotNull(writing.get(5, TimeUnit.SECONDS));
    assertThrows(IOException.class, upload::finish);
    verify(call).cancel();
  }

  @Test
  void interruptingAProducerPreservesInterruptStatusAndCancels() throws Exception {
    StreamingUpload upload = upload(4096, 0);
    Call call = stalled(upload);
    upload.outputStream().write(new byte[4096]);
    CountDownLatch entered = new CountDownLatch(1), done = new CountDownLatch(1);
    AtomicBoolean interrupted = new AtomicBoolean();
    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread producer =
        new Thread(
            () -> {
              entered.countDown();
              try {
                upload.outputStream().write(new byte[4096]);
              } catch (Throwable e) {
                error.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
              } finally {
                done.countDown();
              }
            });
    producer.start();
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      producer.interrupt();
      assertTrue(done.await(5, TimeUnit.SECONDS));
      assertTrue(error.get() instanceof InterruptedIOException);
      assertTrue(interrupted.get());
      verify(call).cancel();
    } finally {
      producer.interrupt();
      producer.join(5000);
    }
  }

  @Test
  void downstreamFailureWakesBlockedProducerAndIsRetained() throws Exception {
    StreamingUpload upload = upload(4096, 5000);
    IOException expected = new IOException("downstream failed");
    Call call =
        start(
            upload,
            new RecordingSink() {
              @Override
              public void write(Buffer source, long count) throws IOException {
                throw expected;
              }
            });
    assertSame(
        expected,
        assertThrows(IOException.class, () -> upload.outputStream().write(new byte[100000])));
    assertSame(expected, assertThrows(IOException.class, upload::finish));
    verify(call).cancel();
  }

  @Test
  void writesAfterOutputCloseCannotPoisonTheCompletedStream() throws Exception {
    StreamingUpload upload = upload(4096, 5000);
    RecordingSink sink = new RecordingSink();
    start(upload, sink);
    upload.outputStream().write(new byte[] {1, 2, 3});
    upload.outputStream().close();
    assertThrows(IOException.class, () -> upload.outputStream().write(4));
    assertNotNull(upload.finish());
    assertArrayEquals(new byte[] {1, 2, 3}, sink.bytes.toByteArray());
  }

  @Test
  void failureCannotReturnBeforeTheCallHasBeenCanceled() throws Exception {
    StreamingUpload upload = upload(4096, 5000);
    IOException expected = new IOException("downstream failed");
    CountDownLatch cancelEntered = new CountDownLatch(1), cancelReleased = new CountDownLatch(1);
    Call call = mock(Call.class);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              cancelEntered.countDown();
              cancelReleased.await();
              return null;
            })
        .when(call)
        .cancel();
    upload.start(
        call,
        () -> {
          upload.writeTo(
              Okio.buffer(
                  new RecordingSink() {
                    @Override
                    public void write(Buffer source, long count) throws IOException {
                      throw expected;
                    }
                  }));
          return new HttpResponse();
        });
    Future<IOException> writing =
        executor.submit(
            () ->
                assertThrows(
                    IOException.class, () -> upload.outputStream().write(new byte[100000])));
    try {
      assertTrue(cancelEntered.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> writing.get(100, TimeUnit.MILLISECONDS));
      cancelReleased.countDown();
      assertSame(expected, writing.get(5, TimeUnit.SECONDS));
      assertSame(expected, assertThrows(IOException.class, upload::finish));
    } finally {
      cancelReleased.countDown();
    }
  }

  @Test
  void abortBeforeStartDoesNotRunARequestBody() throws Exception {
    StreamingUpload upload = upload(4096, 5000);
    Call call = mock(Call.class);
    AtomicBoolean ran = new AtomicBoolean();
    upload.close();
    upload.start(
        call,
        () -> {
          ran.set(true);
          return new HttpResponse();
        });
    verify(call).cancel();
    assertFalse(ran.get());
    assertThrows(IOException.class, upload::finish);
  }

  @Test
  void outputBufferIsAppliedAtTheSingleSegmentHandoffStage() throws Exception {
    for (int configured : new int[] {0, 16384, 131072}) {
      StreamingUpload upload = upload(65536, 5000);
      upload.configureBufferSize(configured);
      int target = Math.min(65536, Math.max(8192, configured));
      CountDownLatch received = new CountDownLatch(1);
      RecordingSink sink =
          new RecordingSink() {
            @Override
            public void write(Buffer source, long count) throws IOException {
              super.write(source, count);
              received.countDown();
            }
          };
      start(upload, sink);
      upload.outputStream().write(new byte[target - 1]);
      assertFalse(received.await(100, TimeUnit.MILLISECONDS));
      assertThrows(IllegalStateException.class, () -> upload.configureBufferSize(32768));
      upload.outputStream().write(1);
      assertTrue(received.await(5, TimeUnit.SECONDS));
      assertEquals(target, sink.bytes.size());
      upload.finish();
    }
  }

  private Sink blockedFlush(CountDownLatch entered, CountDownLatch release) {
    return new RecordingSink() {
      @Override
      public void flush() throws IOException {
        if (bytes.size() == 0) {
          return;
        }
        entered.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("flush interrupted");
        }
      }
    };
  }

  private static class RecordingSink implements Sink {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    @Override
    public void write(Buffer source, long count) throws IOException {
      bytes.write(source.readByteArray(count));
    }

    @Override
    public void flush() throws IOException {}

    @Override
    public void close() {}

    @Override
    public Timeout timeout() {
      return Timeout.NONE;
    }
  }
}
