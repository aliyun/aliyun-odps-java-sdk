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

package com.aliyun.odps.table.impl.v2;

import static org.junit.Assert.*;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.storage.internal.HttpClient;
import com.aliyun.odps.storage.internal.StubSettings;
import com.aliyun.odps.storage.internal.io.StreamingUpload;
import com.aliyun.odps.storage.settings.HttpSettings;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.Test;

/** Fault-injection tests against real sockets; no production credentials or services. */
public class V2StreamingFailureTest {
  private HttpClient client(MockWebServer server, int writeSeconds) {
    return new HttpClient(
        StubSettings.newBuilder()
            .withEndpoint(server.url("/").toString())
            .withTunnelEndpoint(server.url("/").toString())
            .withCredentialsProvider(
                new AccountCredentialProvider(new AliyunAccount("fake-id", "fake-key")))
            .withHttpSettings(
                HttpSettings.newBuilder()
                    .withWriteTimeout(Duration.ofSeconds(writeSeconds))
                    .withReadTimeout(Duration.ofSeconds(2))
                    .build())
            .build());
  }

  @Test
  public void abortUnblocksProducerAgainstNonReadingPeer() throws Exception {
    verifyBackpressure(true);
  }

  @Test
  public void writeTimeoutUnblocksProducerAgainstNonReadingPeer() throws Exception {
    verifyBackpressure(false);
  }

  private void verifyBackpressure(boolean abort) throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.STALL_SOCKET_AT_START));
      HttpClient client = client(server, abort ? 0 : 1);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try (StreamingUpload upload =
          client.openStreamingUpload(
              "upload", Collections.emptyMap(), Collections.emptyMap(), 4096)) {
        CountDownLatch started = new CountDownLatch(1);
        Future<IOException> producer =
            executor.submit(
                () -> {
                  byte[] batch = new byte[64 * 1024];
                  started.countDown();
                  try {
                    OutputStream output = upload.outputStream();
                    for (int i = 0; i < 4096; i++) {
                      output.write(batch);
                    }
                    return null;
                  } catch (IOException error) {
                    return error;
                  }
                });
        assertTrue(started.await(2, TimeUnit.SECONDS));
        if (abort) {
          try {
            producer.get(200, TimeUnit.MILLISECONDS);
            fail("unbounded buffering consumed 256MB instead of applying backpressure");
          } catch (TimeoutException expected) {
          }
          upload.close();
        }
        assertNotNull(producer.get(5, TimeUnit.SECONDS));
        try {
          upload.finish();
          fail("failed stream cannot become successful");
        } catch (IOException expected) {
        }
      } finally {
        executor.shutdownNow();
        client.shutdown();
      }
    }
  }

  @Test
  public void abortUnblocksFlushAndResponseWaiters() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
      HttpClient client = client(server, 2);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try (StreamingUpload upload =
          client.openStreamingUpload(
              "upload", Collections.emptyMap(), Collections.emptyMap(), 4096)) {
        upload.outputStream().write(new byte[] {1, 2, 3});
        upload.outputStream().flush();
        Future<Boolean> finish =
            executor.submit(
                () -> {
                  try {
                    upload.finish();
                    return false;
                  } catch (IOException expected) {
                    return true;
                  }
                });
        assertNotNull(server.takeRequest(2, TimeUnit.SECONDS));
        upload.close();
        assertTrue(finish.get(2, TimeUnit.SECONDS));
      } finally {
        executor.shutdownNow();
        client.shutdown();
      }
    }
  }

  @Test
  public void serverFailureIsNotReplayed() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      server.enqueue(
          new MockResponse()
              .setResponseCode(503)
              .setBody("{\"Code\":\"Unavailable\",\"Message\":\"failed\"}"));
      HttpClient client = client(server, 2);
      try (StreamingUpload upload =
          client.openStreamingUpload(
              "upload", Collections.emptyMap(), Collections.emptyMap(), 4096)) {
        upload.outputStream().write(new byte[] {1, 2, 3});
        for (int i = 0; i < 2; i++) {
          try {
            upload.finish();
            fail("server error must be preserved");
          } catch (IOException expected) {
          }
        }
        assertEquals(1, server.getRequestCount());
      } finally {
        client.shutdown();
      }
    }
  }

  @Test
  public void connectionFailureUnblocksProducer() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start();
    HttpClient client = client(server, 1);
    server.shutdown();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (StreamingUpload upload =
        client.openStreamingUpload(
            "upload", Collections.emptyMap(), Collections.emptyMap(), 4096)) {
      Future<Boolean> write =
          executor.submit(
              () -> {
                try {
                  upload.outputStream().write(new byte[128 * 1024]);
                  upload.finish();
                  return false;
                } catch (IOException expected) {
                  return true;
                }
              });
      assertTrue(write.get(3, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
      client.shutdown();
    }
  }

  @Test
  public void interruptedResponseWaitCancelsUploadAndPreservesInterrupt() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
      HttpClient client = client(server, 2);
      CountDownLatch stopped = new CountDownLatch(1);
      java.util.concurrent.atomic.AtomicBoolean interrupted =
          new java.util.concurrent.atomic.AtomicBoolean();
      try (StreamingUpload upload =
          client.openStreamingUpload(
              "upload", Collections.emptyMap(), Collections.emptyMap(), 4096)) {
        Thread waiter =
            new Thread(
                () -> {
                  try {
                    upload.finish();
                  } catch (IOException expected) {
                    interrupted.set(Thread.currentThread().isInterrupted());
                  } finally {
                    stopped.countDown();
                  }
                });
        waiter.start();
        try {
          assertNotNull(server.takeRequest(2, TimeUnit.SECONDS));
          waiter.interrupt();
          assertTrue(stopped.await(2, TimeUnit.SECONDS));
          assertTrue(interrupted.get());
          try {
            upload.finish();
            fail("interrupted upload cannot be resumed");
          } catch (IOException expected) {
          }
        } finally {
          upload.close();
          waiter.interrupt();
          waiter.join(2000);
        }
      } finally {
        client.shutdown();
      }
    }
  }
}
