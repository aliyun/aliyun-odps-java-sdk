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

package com.aliyun.odps.table.write.impl.v2;

import static org.junit.Assert.*;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.distribution.ClusteredDistribution;
import com.aliyun.odps.table.distribution.Distribution;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.order.NullOrdering;
import com.aliyun.odps.table.utils.ArrowUtils;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.google.gson.JsonParser;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Exercises the production v2 provider rather than the request/response helper alone. */
public class V2WritePathRegressionTest {
    private MockWebServer server;
    private TableWriteSessionBuilder builder;

    @Before
    public void setup() throws Exception {
        server = new MockWebServer();
        server.start();
        String endpoint = server.url("/").toString();
        builder = new TableWriteSessionBuilder()
                .identifier(TableIdentifier.of("proj", "default", "tbl"))
                .withSessionProvider("v2").withSettings(EnvironmentSettings.newBuilder()
                        .withCredentials(Credentials.newBuilder()
                                .withAccount(new AliyunAccount("test-id", "test-key")).build())
                        .withServiceEndpoint(endpoint).withTunnelEndpoint(endpoint).build());
        enqueueSession();
    }

    private void enqueueSession() {
        server.enqueue(new MockResponse().setHeader("Content-Type", "application/json")
                .setBody("{\"SessionId\":\"s1\",\"SessionStatus\":\"NORMAL\","
                        + "\"DataSchema\":{\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"BIGINT\","
                        + "\"Nullable\":true}],\"PartitionColumns\":[]},\"MaxBlockNumber\":100,"
                        + "\"SupportedDataFormat\":[{\"Type\":\"arrow\",\"Version\":\"v5\"}],"
                        + "\"RequiredOrdering\":[{\"Name\":\"a\",\"SortDirection\":\"ASC\","
                        + "\"NullOrdering\":\"NULLS_FIRST\"}],\"RequiredDistribution\":{"
                        + "\"Type\":\"HASH\",\"ClusterKeys\":[\"a\"],\"BucketsNumber\":4}}"));
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    public void staticPartitionIsSentToServer() throws Exception {
        builder.partition(new PartitionSpec("ds=1")).overwrite(true)
                .withMaxFieldSize(4096L).withWriteCheck(true).buildBatchWriteSession();
        RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
        assertNotNull(request);
        com.google.gson.JsonObject body = JsonParser.parseString(request.getBody().readUtf8())
                .getAsJsonObject();
        assertEquals("ds=1", body.get("PartitionSpec").getAsString());
        assertTrue(body.get("Overwrite").getAsBoolean());
        assertEquals(4096L, body.get("MaxFieldSize").getAsLong());
        assertTrue(body.get("EnhanceWriteCheck").getAsBoolean());
        assertFalse(body.get("SupportWriteCluster").getAsBoolean());
    }

    @Test
    public void explicitlyPassingDefaultDynamicOptionsIsAllowed() throws Exception {
        builder.withDynamicPartitionOptions(DynamicPartitionOptions.createDefault())
                .buildBatchWriteSession();
        assertEquals(1, server.getRequestCount());
    }

    @Test
    public void nonDefaultDynamicOptionsReachServer() throws Exception {
        builder.withDynamicPartitionOptions(DynamicPartitionOptions.newBuilder()
                .withDynamicPartitionLimit(42).withInvalidLimit(7)
                .withInvalidStrategy(DynamicPartitionOptions.InvalidStrategy.DROP).build())
                .buildBatchWriteSession();
        com.google.gson.JsonObject options = JsonParser.parseString(
                server.takeRequest(1, TimeUnit.SECONDS).getBody().readUtf8())
                .getAsJsonObject().getAsJsonObject("DynamicPartitionOptions");
        assertEquals(42, options.get("DynamicPartitionLimit").getAsInt());
        assertEquals(7, options.get("InvalidLimit").getAsInt());
        assertEquals("Drop", options.get("InvalidStrategy").getAsString());
    }

    @Test
    public void clusterCapabilityReachesServer() throws Exception {
        builder.withCapabilities(com.aliyun.odps.table.write.TableWriteCapabilities.newBuilder()
                .supportHashBuckets(true).build()).buildBatchWriteSession();
        assertTrue(JsonParser.parseString(server.takeRequest(1, TimeUnit.SECONDS)
                .getBody().readUtf8()).getAsJsonObject().get("SupportWriteCluster").getAsBoolean());
    }

    @Test
    public void serverMetadataSurvivesTypedResponseAndReload() throws Exception {
        assertMetadata(builder.buildBatchWriteSession());
        assertEquals(1, server.getRequestCount());
        assertNotNull(server.takeRequest(1, TimeUnit.SECONDS));
        enqueueSession();
        assertMetadata(builder.withSessionId("s1").buildBatchWriteSession());
        assertEquals(2, server.getRequestCount());
        RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
        assertNotNull(request);
        assertEquals("TableGetWriteSession", request.getRequestUrl().queryParameter("Action"));
    }

    @Test
    public void blankSessionIdDoesNotCreateSession() {
        assertThrows(IllegalArgumentException.class,
                () -> builder.withSessionId(" \t").buildBatchWriteSession());
        assertThrows(IllegalArgumentException.class,
                () -> builder.withSessionId("\u2003").buildBatchWriteSession());
        assertEquals(0, server.getRequestCount());
    }

    private void assertMetadata(TableBatchWriteSession session) {
        assertEquals(1, session.requiredOrdering().length);
        assertEquals(NullOrdering.NULLS_FIRST, session.requiredOrdering()[0].nullOrdering());
        assertEquals(Distribution.Type.HASH, session.requiredDistribution().getType());
        assertEquals(Integer.valueOf(4), ((ClusteredDistribution) session.requiredDistribution())
                .getBucketsNumber().get());
        assertTrue(session.supportsDataFormat(ArrowUtils.getDefaultDataFormat()));
    }

    @Test
    public void newElementUsesCallerAllocator() throws Exception {
        verifyAllocator(false);
    }

    @Test
    public void reloadedSessionUsesCallerAllocator() throws Exception {
        verifyAllocator(true);
    }

  @Test
  public void uploadBufferOptionsPreserveArrowPayloadWithTransportAggregation() throws Exception {
        TableBatchWriteSession session = builder.buildBatchWriteSession();
        server.takeRequest(5, TimeUnit.SECONDS);
        byte[] expected = null;
        try (RootAllocator allocator = new RootAllocator()) {
            for (Integer bufferSize : new Integer[]{null, 0, 16384}) {
                server.enqueue(new MockResponse().setHeader("Content-Type", "application/json")
                        .setHeader("x-odps-max-storage-route-token", "route")
                        .setBody("{\"QuotaToken\":\"quota\"}"));
                server.enqueue(new MockResponse().setHeader("Content-Type", "application/json")
                        .setBody("{\"RecordCount\":32768,\"CommitMessage\":\"message\"}"));
                WriterOptions.Builder options = WriterOptions.newBuilder()
                        .withSettings(builder.getSettings()).withBufferAllocator(allocator);
                if (bufferSize != null) {
                    options.withUploadBufferSize(bufferSize);
                }
                BatchWriter<VectorSchemaRoot> writer = session.createArrowWriter(0,
                        WriterAttemptId.of(0), options.build());
                try (VectorSchemaRoot root = writer.newElement()) {
                    root.allocateNew();
                    BigIntVector values = (BigIntVector) root.getVector("a");
                    Random random = new Random(1729);
                    for (int row = 0; row < 32768; row++) {
                        values.setSafe(row, random.nextLong());
                    }
                    root.setRowCount(32768);
                    writer.write(root);
                    writer.commit();
                } finally {
                    writer.close();
                }
                assertNotNull(server.takeRequest(5, TimeUnit.SECONDS)); // quota reservation
                RecordedRequest upload = server.takeRequest(5, TimeUnit.SECONDS);
                assertNotNull(upload);
                assertTrue(upload.getPath().contains("Action=TableWrite"));
                byte[] actual = upload.getBody().readByteArray();
        assertFalse(upload.getChunkSizes().isEmpty());
        assertEquals("chunked", upload.getHeader("Transfer-Encoding"));
                if (expected == null) {
                    expected = actual;
                } else {
                    assertArrayEquals(expected, actual);
        }
        // The transport may coalesce writes independently of the Arrow output buffer.
        // Validate decoded data and EOF instead of depending on thread scheduling/chunks.
        try (org.apache.arrow.vector.ipc.ArrowStreamReader reader =
            new org.apache.arrow.vector.ipc.ArrowStreamReader(
                new java.io.ByteArrayInputStream(actual),
                allocator,
                com.aliyun.odps.table.arrow.compression.OdpsCompressionFactory.INSTANCE)) {
          Random values = new Random(1729);
          int rows = 0;
          while (reader.loadNextBatch()) {
            VectorSchemaRoot batch = reader.getVectorSchemaRoot();
            BigIntVector vector = (BigIntVector) batch.getVector("a");
            for (int row = 0; row < batch.getRowCount(); row++) {
              assertEquals(values.nextLong(), vector.get(row));
              rows++;
            }
                    }
          assertEquals(32768, rows);
                }
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeUploadBufferIsRejected() {
        WriterOptions.newBuilder().withUploadBufferSize(-1);
    }

    private void verifyAllocator(boolean reload) throws Exception {
        if (reload) {
            builder.withSessionId("s1");
        }
        TableBatchWriteSession session = builder.buildBatchWriteSession();
        server.enqueue(new MockResponse().setHeader("Content-Type", "application/json")
                .setHeader("x-odps-max-storage-route-token", "route")
                .setBody("{\"QuotaToken\":\"quota\"}"));
        try (RootAllocator allocator = new RootAllocator()) {
            BatchWriter<VectorSchemaRoot> writer = session.createArrowWriter(0,
                    WriterAttemptId.of(0), WriterOptions.newBuilder()
                            .withSettings(builder.getSettings()).withBufferAllocator(allocator).build());
            try (VectorSchemaRoot root = writer.newElement()) {
                root.allocateNew();
                assertSame(allocator, root.getVector("a").getAllocator());
                assertTrue(allocator.getAllocatedMemory() > 0);
            } finally {
                writer.abort();
            }
            assertEquals(0, allocator.getAllocatedMemory());
            // Closing/aborting a writer must not close the caller-owned allocator.
            try (org.apache.arrow.memory.ArrowBuf buffer = allocator.buffer(8)) {
                assertTrue(buffer.capacity() >= 8);
            }
        }
    }
}
