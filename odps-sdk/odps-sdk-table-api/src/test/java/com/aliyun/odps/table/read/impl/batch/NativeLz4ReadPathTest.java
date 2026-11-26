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

package com.aliyun.odps.table.read.impl.batch;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.credentials.StaticCredentialProvider;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.StubSettings;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.arrow.ArrowWriterFactory;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplitWithIndex;
import com.aliyun.odps.tunnel.HttpHeaders;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Native client selection must not introduce a new server-side compression protocol. */
public class NativeLz4ReadPathTest {
  @Test
  public void v1NativeReaderKeepsStandardHeaderAndHandlesExtendedStreams() throws Exception {
    check(false);
  }

  @Test
  public void v2NativeReaderKeepsStandardHeaderAndHandlesExtendedStreams() throws Exception {
    check(true);
  }

  @Test
  public void v2AutomaticReaderHandlesExistingCodecSettings() throws Exception {
    for (CompressionCodec codec : new CompressionCodec[] {
        CompressionCodec.NO_COMPRESSION, CompressionCodec.LZ4_FRAME, CompressionCodec.ZSTD}) {
      check(true, codec);
    }
  }

  @Test
  public void v1AutomaticReaderKeepsStandardHeader() throws Exception {
    check(false, CompressionCodec.LZ4_FRAME);
  }

  @Test
  public void v1Lz4ReaderHandlesLinkedFrames() throws Exception {
    check(false, CompressionCodec.LZ4_FRAME, true);
  }

  @Test
  public void v1OtherCodecsRemainUnchanged() throws Exception {
    check(false, CompressionCodec.ZSTD);
    check(false, CompressionCodec.NO_COMPRESSION);
  }

  /** Synthetic multi-block frame, prepared before a child JVM disables native loading. */
  public static byte[] linkedStream() throws Exception {
    try (RootAllocator allocator = new RootAllocator(16L * 1024 * 1024)) {
      byte[] bytes = stream(allocator, false, CompressionCodec.LZ4_FRAME_NATIVE, 65536);
      boolean linked = false;
      for (int i = 0; i + 5 < bytes.length; i++) {
        if (bytes[i] == 4 && bytes[i + 1] == 0x22 && bytes[i + 2] == 0x4d
            && bytes[i + 3] == 0x18 && (bytes[i + 4] & 0x20) == 0) {
          linked = true;
        }
      }
      assertTrue("Fixture must contain a linked-block LZ4 frame", linked);
      return bytes;
    }
  }

  @Test
  public void v2StorageReaderHandlesLz4Streams() throws Exception {
    for (boolean extended : new boolean[] {false, true}) {
      try (MockWebServer server = new MockWebServer();
           RootAllocator allocator = new RootAllocator(16L * 1024 * 1024)) {
        server.start();
        StorageStub stub = new StorageStub(StubSettings.newBuilder()
            .withEndpoint(server.url("/").toString())
            .withTunnelEndpoint(server.url("/").toString()).withProject("proj")
            .withCredentialsProvider(StaticCredentialProvider.of("test-id", "test-key"))
            .withHttpSettings(com.aliyun.odps.storage.settings.HttpSettings.newBuilder().build())
            .build());
        CreateTableReadSessionResponse response = StorageStub.createWireGson().fromJson(
            "{\"SessionId\":\"s\",\"SplitMode\":\"Size\",\"SplitsCount\":1,"
                + "\"ExtendedArrowIPCEnabled\":" + extended + ",\"DataSchema\":{"
                + "\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"BIGINT\"}],"
                + "\"PartitionColumns\":[],\"SystemColumns\":[]}}",
            CreateTableReadSessionResponse.class);
        TableIdentifier table = TableIdentifier.of("proj", "default", "tbl");
        com.aliyun.odps.storage.read.TableReadSession session =
            new com.aliyun.odps.storage.read.TableReadSession(stub, table, allocator, response,
                new com.aliyun.odps.storage.read.TableReadSessionBuilder(stub, allocator, table));
        server.enqueue(new MockResponse().setBody(new Buffer()
            .write(stream(allocator, extended, CompressionCodec.LZ4_FRAME))));
        try (ArrowReader reader = session.createReaderBuilder(session.getSplits().get(0)).build()) {
          assertTrue(reader.nextBatch());
          VectorSchemaRoot root = reader.getCurrentValue();
          assertEquals(4096, root.getRowCount());
          for (int i = 0; i < root.getRowCount(); i++) {
            assertEquals(i % 31, ((BigIntVector) root.getVector("a")).get(i));
          }
          assertFalse(reader.nextBatch());
        }
        assertEquals("x-lz4-frame", server.takeRequest(5, TimeUnit.SECONDS)
            .getHeader("Accept-Encoding"));
        assertEquals(0, allocator.getAllocatedMemory());
        stub.close();
      }
    }
  }

  private void check(boolean v2) throws Exception {
    check(v2, CompressionCodec.LZ4_FRAME_NATIVE);
  }

  private void check(boolean v2, CompressionCodec codec) throws Exception {
    check(v2, codec, false);
  }

  private void check(boolean v2, CompressionCodec codec, boolean linked) throws Exception {
    int rowCount = linked ? 65536 : 4096;
    for (boolean extended : new boolean[] {false, true}) {
      for (boolean reuse : new boolean[] {false, true}) {
        try (MockWebServer server = new MockWebServer();
             RootAllocator allocator = new RootAllocator(16L * 1024 * 1024)) {
          server.start();
          String endpoint = server.url("/").toString();
          EnvironmentSettings settings = EnvironmentSettings.newBuilder()
              .withCredentials(Credentials.newBuilder()
                  .withAccount(new AliyunAccount("test-id", "test-key")).build())
              .withServiceEndpoint(endpoint).withTunnelEndpoint(endpoint)
              .withRestOptions(RestOptions.newBuilder().withRetryTimes(0)
                  .withReadTimeout(5).withWriteTimeout(5).build())
              .build();
          TableIdentifier table = TableIdentifier.of("proj", "default", "tbl");
          TableBatchReadSession session = null;
          if (v2) {
            server.enqueue(new MockResponse().setBody("{\"SessionId\":\"s\","
                + "\"SessionStatus\":\"NORMAL\",\"SplitMode\":\"Size\",\"SplitsCount\":1,"
                + "\"ExtendedArrowIPCEnabled\":" + extended + ",\"DataSchema\":{"
                + "\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"BIGINT\"}],"
                + "\"PartitionColumns\":[],\"SystemColumns\":[]}}"));
            session = new TableReadSessionBuilder().identifier(table).withSettings(settings)
                .withSessionProvider("v2").buildBatchReadSession();
            assertNotNull(server.takeRequest(5, TimeUnit.SECONDS));
          }
          byte[] body;
          if (linked) {
            String fixture = System.getProperty("test.odps.linkedLz4Stream");
            byte[] ipc = fixture == null ? linkedStream() : Files.readAllBytes(Paths.get(fixture));
            ByteArrayOutputStream framed = new ByteArrayOutputStream();
            if (extended) {
              framed.write(1);
            }
            framed.write(ipc);
            body = framed.toByteArray();
          } else {
            CompressionCodec responseCodec = !v2 ? codec
                : codec == CompressionCodec.LZ4_FRAME_NATIVE ? codec : CompressionCodec.LZ4_FRAME;
            body = stream(allocator, extended, responseCodec);
          }
          server.enqueue(new MockResponse()
              .setHeader(HttpHeaders.HEADER_EXTENDED_ARROW_IPC_ENABLED, Boolean.toString(extended))
              .setBody(new Buffer().write(body)));
          ReaderOptions options = ReaderOptions.newBuilder().withSettings(settings)
              .withBufferAllocator(allocator).withReuseBatch(reuse)
              .withCompressionCodec(codec).build();
          InputSplitWithIndex split = new InputSplitWithIndex() {
            public String getSessionId() {
              return "s";
            }

            public int getSplitIndex() {
              return 0;
            }
          };
          try (SplitReader<VectorSchemaRoot> reader = v2
              ? session.createArrowReader(session.getInputSplitAssigner().getSplit(0), options)
              : new SplitArrowReaderImpl(table, split, options)) {
            assertTrue(reader.hasNext());
            VectorSchemaRoot root = reader.get();
            try {
              assertEquals(rowCount, root.getRowCount());
              BigIntVector values = (BigIntVector) root.getVector("a");
              for (int r = 0; r < root.getRowCount(); r++) {
                assertEquals(r % 31, values.get(r));
              }
            } finally {
              if (!reuse) {
                root.close();
              }
            }
            assertFalse(reader.hasNext());
          }
          RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
          assertNotNull(request);
          String encoding = codec == CompressionCodec.NO_COMPRESSION ? null : codec.getWireName();
          assertEquals(v2 ? "x-lz4-frame" : encoding, request.getHeader("Accept-Encoding"));
          assertEquals(0, allocator.getAllocatedMemory());
        }
      }
    }
  }

  private static byte[] stream(RootAllocator allocator, boolean extended, CompressionCodec codec)
      throws Exception {
    return stream(allocator, extended, codec, 4096);
  }

  private static byte[] stream(RootAllocator allocator, boolean extended, CompressionCodec codec,
      int rowCount) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    if (extended) {
      bytes.write(1);
    }
    try (VectorSchemaRoot root = VectorSchemaRoot.of(new BigIntVector("a", allocator));
         ArrowWriter writer = ArrowWriterFactory.newBuilder(bytes)
             .withCompression(codec).build()) {
      BigIntVector values = (BigIntVector) root.getVector(0);
      values.allocateNew(rowCount);
      for (int r = 0; r < rowCount; r++) {
        values.set(r, r % 31);
      }
      values.setValueCount(rowCount);
      root.setRowCount(rowCount);
      writer.writeBatch(root);
    }
    return bytes.toByteArray();
  }
}
