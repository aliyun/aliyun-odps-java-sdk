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
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowWriterFactory;
import com.aliyun.odps.table.arrow.compression.OdpsCompressionFactory;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.table.write.impl.batch.WriterCommitMessageImpl;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.Test;

public class V2CompatibleContractTest {
  private static final String SESSION =
      "{\"SessionId\":\"s\",\"SessionStatus\":\"NORMAL\","
          + "\"SplitMode\":\"Size\",\"SplitsCount\":1,\"DataSchema\":{"
          + "\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"BIGINT\"}],\"PartitionColumns\":[]}}";

  private EnvironmentSettings settings(String url, String name) {
    return EnvironmentSettings.newBuilder()
        .withServiceEndpoint(url)
        .withTunnelEndpoint(url)
        .withSharingQuotaToken(name + "-quota")
        .withCredentials(
            Credentials.newBuilder().withAccount(new AliyunAccount(name, "fake-key")).build())
        .withRestOptions(
            RestOptions.newBuilder()
                .witUserAgent(name)
                .withRetryTimes(0)
                .withReadTimeout(5)
                .withWriteTimeout(5)
                .build())
        .build();
  }

  private TableBatchWriteSession session(MockWebServer server) throws Exception {
    server.enqueue(new MockResponse().setBody(SESSION));
    return new TableWriteSessionBuilder()
        .withSessionProvider("v2")
        .identifier(TableIdentifier.of("p", "t"))
        .withSettings(settings(server.url("/").toString(), "coordinator"))
        .buildBatchWriteSession();
  }

  @Test
  public void wrongCommitTypeIsRejectedBeforeAnyRequest() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      TableBatchWriteSession session = session(server);
      server.takeRequest();
      try {
        session.commit(
            new WriterCommitMessage[] {null, new WriterCommitMessageImpl(7, "old-block")});
        fail("must reject a foreign provider's message");
      } catch (IllegalArgumentException expected) {
        assertEquals(1, server.getRequestCount());
      }
    }
  }

  @Test
  public void readerUsesWorkerSettingsWithNativeCompression() throws Exception {
    assertWorkerReader(CompressionCodec.NO_COMPRESSION);
  }

  @Test
  public void readerIgnoresLegacyZstdOption() throws Exception {
    assertWorkerReader(CompressionCodec.ZSTD);
  }

  @Test
  public void readerIgnoresLegacyLz4Option() throws Exception {
    assertWorkerReader(CompressionCodec.LZ4_FRAME);
  }

  private void assertWorkerReader(CompressionCodec codec) throws Exception {
    try (MockWebServer coordinator = new MockWebServer();
        MockWebServer worker = new MockWebServer();
        RootAllocator allocator = new RootAllocator()) {
      coordinator.start();
      worker.start();
      coordinator.enqueue(new MockResponse().setBody(SESSION));
      TableBatchReadSession session =
          new TableReadSessionBuilder()
              .withSessionProvider("v2")
              .identifier(TableIdentifier.of("p", "t"))
              .withSettings(settings(coordinator.url("/").toString(), "coordinator"))
              .buildBatchReadSession();
      session = transferRead(session);
      {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (BigIntVector value = new BigIntVector("a", allocator);
            VectorSchemaRoot root = VectorSchemaRoot.of(value);
            com.aliyun.odps.table.arrow.ArrowWriter out =
                ArrowWriterFactory.newBuilder(bytes)
                    .withCompression(CompressionCodec.NO_COMPRESSION)
                    .build()) {
          value.allocateNew(1);
          value.setSafe(0, 42);
          root.setRowCount(1);
          out.writeBatch(root);
        }
        worker.enqueue(new MockResponse().setBody(new Buffer().write(bytes.toByteArray())));
        try (SplitReader<VectorSchemaRoot> reader =
            session.createArrowReader(
                session.getInputSplitAssigner().getSplit(0),
                ReaderOptions.newBuilder()
                    .withSettings(settings(worker.url("/").toString(), "worker"))
                    .withBufferAllocator(allocator)
                    .withCompressionCodec(codec)
                    .build())) {
          assertTrue(reader.hasNext());
          assertEquals(42L, ((BigIntVector) reader.get().getVector(0)).get(0));
          assertFalse(reader.hasNext());
        }
        RecordedRequest request = worker.takeRequest(2, TimeUnit.SECONDS);
        assertNotNull(request);
        assertTrue(request.getHeader("Authorization").contains("worker"));
        assertTrue(request.getHeader("User-Agent").contains("worker"));
        assertEquals("worker-quota", request.getRequestUrl().queryParameter("sharingQuotaToken"));
        assertEquals("x-lz4-frame", request.getHeader("Accept-Encoding"));
      }
      assertEquals(1, coordinator.getRequestCount());
    }
  }

  protected TableBatchReadSession transferRead(TableBatchReadSession session) throws Exception {
    return session;
  }

  protected TableBatchWriteSession transferWrite(TableBatchWriteSession session) throws Exception {
    return session;
  }

  @Test
  public void flushUploadsReusableBatchesBeforeClose() throws Exception {
    assertStreaming(CompressionCodec.NO_COMPRESSION);
  }

  @Test
  public void writerIgnoresLegacyZstdOption() throws Exception {
    assertStreaming(CompressionCodec.ZSTD);
  }

  @Test
  public void writerIgnoresLegacyLz4Option() throws Exception {
    assertStreaming(CompressionCodec.LZ4_FRAME);
  }

  @Test
  public void nativeLz4WriterUsesStandardLz4ArrowBuffers() throws Exception {
    assertStreaming(CompressionCodec.LZ4_FRAME_NATIVE);
  }

  @Test
  public void emptyBlockSkipsWorkerRequestsAndStillCommitsOverwriteSession() throws Exception {
    try (MockWebServer coordinator = new MockWebServer();
        MockWebServer worker = new MockWebServer();
        RootAllocator allocator = new RootAllocator()) {
      coordinator.start();
      worker.start();
      coordinator.enqueue(new MockResponse().setBody(SESSION));
      coordinator.enqueue(new MockResponse().setBody("{\"SessionStatus\":\"COMMITTED\"}"));
      TableBatchWriteSession session = new TableWriteSessionBuilder()
          .withSessionProvider("v2")
          .identifier(TableIdentifier.of("p", "t"))
          .withSettings(settings(coordinator.url("/").toString(), "coordinator"))
          .withWriteCheck(true)
          .overwrite(true)
          .buildBatchWriteSession();
      BatchWriter<VectorSchemaRoot> writer = transferWrite(session).createArrowWriter(
          0, WriterAttemptId.of(0), WriterOptions.newBuilder()
              .withSettings(settings(worker.url("/").toString(), "worker"))
              .withBufferAllocator(allocator).build());
      try (VectorSchemaRoot root = writer.newElement()) {
        writer.write(root);
        writer.flush();
        writer.close();
        WriterCommitMessage message = writer.commit();
        assertNull(message);
        assertEquals(0, worker.getRequestCount());
        session.commit(new WriterCommitMessage[] {message});
      } finally {
        writer.abort();
      }
      assertEquals(2, coordinator.getRequestCount());
      assertNotNull(coordinator.takeRequest(1, TimeUnit.SECONDS));
      RecordedRequest commit = coordinator.takeRequest(1, TimeUnit.SECONDS);
      assertNotNull(commit);
      assertEquals("TableCommitWriteSession", commit.getRequestUrl().queryParameter("Action"));
      assertEquals(0, JsonParser.parseString(commit.getBody().readUtf8())
          .getAsJsonObject().getAsJsonArray("CommitMessages").size());
    }
  }

  private void assertStreaming(CompressionCodec codec) throws Exception {
    CountDownLatch firstBatch = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    java.util.concurrent.atomic.AtomicLong receivedBytes =
        new java.util.concurrent.atomic.AtomicLong();
    AtomicReference<String> auth = new AtomicReference<>();
    AtomicReference<String> userAgent = new AtomicReference<>();
    AtomicReference<String> query = new AtomicReference<>();
    HttpServer worker = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ExecutorService executor = Executors.newCachedThreadPool();
    worker.setExecutor(executor);
    worker.createContext(
        "/",
        exchange -> {
          String body;
          if (exchange.getRequestURI().getQuery().contains("Action=TableCreateWriteStream")) {
            exchange.getResponseHeaders().add("x-odps-max-storage-route-token", "route");
            body = "{\"QuotaToken\":\"quota\"}";
            while (exchange.getRequestBody().read() != -1) {}
          } else {
            auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
            userAgent.set(exchange.getRequestHeaders().getFirst("User-Agent"));
            query.set(exchange.getRequestURI().getQuery());
            long rows = 0;
            try (RootAllocator allocator = new RootAllocator();
                org.apache.commons.io.input.CountingInputStream input =
                    new org.apache.commons.io.input.CountingInputStream(exchange.getRequestBody());
                ArrowStreamReader reader =
                    new ArrowStreamReader(
                        input,
                        allocator,
                        new OdpsCompressionFactory() {
                          @Override
                          public org.apache.arrow.vector.compression.CompressionCodec createCodec(
                              org.apache.arrow.vector.compression.CompressionUtil.CodecType type) {
                            assertEquals(
                                codec == CompressionCodec.LZ4_FRAME_NATIVE
                                    ? org.apache.arrow.vector.compression.CompressionUtil.CodecType.LZ4_FRAME
                                    : org.apache.arrow.vector.compression.CompressionUtil.CodecType.ZSTD,
                                type);
                            return super.createCodec(type);
                          }
                        })) {
              assertEquals(
                  "a", reader.getVectorSchemaRoot().getSchema().getFields().get(0).getName());
              while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                if (root.getRowCount() > 0) {
                  assertEquals(42L, ((BigIntVector) root.getVector(0)).get(0));
                }
                rows += root.getRowCount();
                firstBatch.countDown();
              }
              receivedBytes.set(input.getByteCount());
            } catch (Throwable error) {
              failure.set(error);
            }
            body = "{\"RecordCount\":" + rows + ",\"CommitMessage\":\"ok\"}";
          }
          byte[] response = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    worker.start();
    try (MockWebServer coordinator = new MockWebServer();
        RootAllocator allocator = new RootAllocator()) {
      coordinator.start();
      TableBatchWriteSession session = transferWrite(session(coordinator));
      String url = "http://127.0.0.1:" + worker.getAddress().getPort() + "/";
      BatchWriter<VectorSchemaRoot> writer =
          session.createArrowWriter(
              0,
              WriterAttemptId.of(0),
              WriterOptions.newBuilder()
                  .withSettings(settings(url, "worker"))
                  .withBufferAllocator(allocator)
                  .withCompressionCodec(codec)
                  .withChunkSize(4096)
                  .build());
      try {
        try (VectorSchemaRoot root = writer.newElement()) {
          BigIntVector vector = (BigIntVector) root.getVector(0);
          vector.allocateNew(8192);
          for (int j = 0; j < 8192; j++) {
            vector.setSafe(j, 42);
          }
          root.setRowCount(8192);
          writer.write(root);
          writer.flush();
          assertTrue(
              "flush must reach server before close", firstBatch.await(3, TimeUnit.SECONDS));
          for (int i = 0; i < 63; i++) {
            writer.write(root);
          }
        }
        assertNotNull(writer.commit());
        assertNull(failure.get());
        assertTrue(
            "native compatible IPC must compress repeated values",
            receivedBytes.get() < 1024 * 1024);
        assertTrue(auth.get().contains("worker"));
        assertTrue(userAgent.get().contains("worker"));
        assertTrue(query.get().contains("sharingQuotaToken=worker-quota"));
        assertEquals(1, coordinator.getRequestCount());
      } finally {
        writer.abort();
      }
    } finally {
      worker.stop(0);
      executor.shutdownNow();
    }
  }
}
