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

package com.aliyun.odps.table.read.impl.v2;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.metrics.MetricNames;
import okio.Buffer;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableIncrementalReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.TableSnapshotSpec;
import com.aliyun.odps.table.read.split.InputSplitWithBucket;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplitAssigner;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/** Exercises public provider creation and restore, including typed wire response conversion. */
public class V2ReadPathRegressionTest {
    private MockWebServer server;
    private EnvironmentSettings settings;

    @Before
    public void setup() throws Exception {
        server = new MockWebServer();
        server.start();
        String endpoint = server.url("/").toString();
        settings = EnvironmentSettings.newBuilder()
                .withCredentials(Credentials.newBuilder()
                        .withAccount(new AliyunAccount("test-id", "test-key")).build())
                .withServiceEndpoint(endpoint).withTunnelEndpoint(endpoint).build();
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    private TableReadSessionBuilder builder() {
        return new TableReadSessionBuilder()
                .identifier(TableIdentifier.of("proj", "default", "tbl"))
                .withSettings(settings).withSessionProvider("v2");
    }

    private void enqueue(String fields) {
        server.enqueue(new MockResponse().setHeader("Content-Type", "application/json")
                .setBody("{\"SessionId\":\"s\",\"SessionStatus\":\"NORMAL\","
                        + "\"DataSchema\":{\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"BIGINT\"}],"
                        + "\"PartitionColumns\":[],\"SystemColumns\":[],\"BlobColumns\":[]},"
                        + fields + "}"));
    }

    @Test
    public void sizeResponseWithoutRecordCountKeepsAllSplits() throws Exception {
        enqueue("\"SplitMode\":\"Size\",\"SplitsCount\":7");
        TableBatchReadSession session = builder().buildBatchReadSession();
        assertTrue(session.getInputSplitAssigner() instanceof IndexedInputSplitAssigner);
        assertEquals(7, session.getInputSplitAssigner().getAllSplits().length);
        TableBatchReadSession restored = builder().fromJson(session.toJson()).buildBatchReadSession();
        assertEquals(7, restored.getInputSplitAssigner().getAllSplits().length);
        assertEquals(1, server.getRequestCount());
    }

    @Test
    public void bucketModeWinsOverRecordCount() throws Exception {
        enqueue("\"SplitMode\":\"Bucket\",\"SplitsCount\":2,"
                + "\"RecordCount\":0,\"SplitBucketId\":[3,9]");
        TableBatchReadSession session = builder().withSplitOptions(
                SplitOptions.newBuilder().SplitByBucket().build()).buildBatchReadSession();
        assertEquals(2, session.getInputSplitAssigner().getAllSplits().length);
        assertEquals(9, ((InputSplitWithBucket) session.getInputSplitAssigner().getSplit(1))
                .getBucketId());
    }

    @Test
    public void rowOffsetModeKeepsRecordCount() throws Exception {
        enqueue("\"SplitMode\":\"RowOffset\",\"RecordCount\":12");
        TableBatchReadSession session = builder().withSplitOptions(
                SplitOptions.newBuilder().SplitByRowOffset().build()).buildBatchReadSession();
        assertEquals(12, session.getInputSplitAssigner().getTotalRowCount());
    }

    @Test
    public void timestampResponseWithoutVersionsCanCreateAndRestore() throws Exception {
        enqueue("\"SplitMode\":\"Size\",\"SplitsCount\":1,\"IncrementalReadOptions\":{"
                + "\"StartTimeStamp\":\"2026-01-01 00:00:00\","
                + "\"EndTimeStamp\":\"2026-01-02 00:00:00\","
                + "\"Mode\":\"cdc\",\"StartTxnId\":\"txn-7\"}");
        TableIncrementalReadSession session = builder().withIncrementalOptions(
                IncrementalOptions.newBuilder().startTimeStamp("2026-01-01 00:00:00")
                        .endTimeStamp("2026-01-02 00:00:00").build())
                .buildIncrementalReadSession();
        IncrementalOptions options = session.getIncrementalOptions();
        assertEquals(TableSnapshotSpec.Type.TIMESTAMP, options.getType());
        assertFalse(options.getStartVersion().isPresent());
        assertEquals(IncrementalOptions.IncrementalMode.CDC, options.getIncrementalMode());
        assertEquals("txn-7", options.getStartTxnId().get());
        TableIncrementalReadSession restored = builder().fromJson(session.toJson())
                .buildIncrementalReadSession();
        assertEquals(options, restored.getIncrementalOptions());
        assertEquals(1, server.getRequestCount());
    }

    @Test
    public void versionZeroIsPreservedAcrossJsonRestore() throws Exception {
        enqueue("\"SplitMode\":\"Size\",\"SplitsCount\":1,\"IncrementalReadOptions\":{"
                + "\"StartVersion\":0,\"EndVersion\":12,\"Mode\":\"append\"}");
        TableIncrementalReadSession session = builder().withIncrementalOptions(
                IncrementalOptions.newBuilder().startVersion(0L).endVersion(12L).build())
                .buildIncrementalReadSession();
        TableIncrementalReadSession restored = builder().fromJson(session.toJson())
                .buildIncrementalReadSession();
        assertNotNull(restored.getIncrementalOptions());
        assertEquals(session.getIncrementalOptions(), restored.getIncrementalOptions());
        assertEquals(Long.valueOf(0), restored.getIncrementalOptions().getStartVersion().get()
                .getVersion().get());
    }

    @Test
    public void extendedReaderKeepsMetricsOutOfRows() throws Exception {
        verifyExtendedReader(false);
    }

    @Test
    public void asyncExtendedReaderReadsTagBeforeStartingWorker() throws Exception {
        verifyExtendedReader(true);
    }

    private void verifyExtendedReader(boolean async) throws Exception {
        enqueue("\"SplitMode\":\"Size\",\"SplitsCount\":1,\"ExtendedArrowIPCEnabled\":true");
        TableBatchReadSession session = builder().enableExtendedArrowIPC(true).buildBatchReadSession();
        try (RootAllocator allocator = new RootAllocator()) {
            ByteArrayOutputStream wire = new ByteArrayOutputStream();
            for (int i = 0; i < 2; i++) {
                wire.write(1);
                try (BigIntVector vector =
                             new BigIntVector("a", allocator)) {
                    vector.allocateNew(1);
                    vector.setSafe(0, 42 + i);
                    vector.setValueCount(1);
                    try (VectorSchemaRoot root =
                                 VectorSchemaRoot.of(vector);
                         ArrowStreamWriter writer =
                                 new ArrowStreamWriter(root, null, wire)) {
                        root.setRowCount(1);
                        writer.start();
                        writer.writeBatch();
                        writer.end();
                    }
                }
                wire.write(2);
                Map<String, String> metadata = new HashMap<>();
                metadata.put("server_processing_duration_us", "10");
                metadata.put("rate_limit_duration_us", "3");
                try (VectorSchemaRoot root =
                             VectorSchemaRoot.create(
                                     new Schema(
                                             Collections.emptyList(), metadata), allocator);
                     ArrowStreamWriter writer =
                             new ArrowStreamWriter(root, null, wire)) {
                    writer.start();
                    writer.end();
                }
            }
            server.enqueue(new MockResponse().setBody(new Buffer().write(wire.toByteArray())));
            ReaderOptions options =
                    ReaderOptions.newBuilder()
                            .withSettings(settings).withBufferAllocator(allocator).withAsync(async).build();
            try (SplitReader<VectorSchemaRoot> reader =
                         session.createArrowReader(session.getInputSplitAssigner().getSplit(0), options)) {
                for (int i = 0; i < 2; i++) {
                    assertTrue(reader.hasNext());
                    assertEquals(42 + i, ((BigIntVector)
                            reader.get().getVector("a")).get(0));
                }
                assertFalse(reader.hasNext());
                assertFalse(reader.hasNext());
                assertEquals(20, reader.currentMetricsValues().counter(
                        MetricNames.SERVER_PROCESS_COST).get().getCount());
                assertEquals(6, reader.currentMetricsValues().counter(
                        MetricNames.RATE_LIMIT_COST).get().getCount());
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }
    @Test
    public void callerOwnedBatchSurvivesReaderClose() throws Exception {
        for (boolean extended : new boolean[]{false, true}) {
            enqueue("\"SplitMode\":\"Size\",\"SplitsCount\":1,\"ExtendedArrowIPCEnabled\":" + extended);
            TableBatchReadSession session = builder().buildBatchReadSession();
            try (RootAllocator allocator = new RootAllocator()) {
                server.enqueue(new MockResponse().setBody(new Buffer().write(oneBatch(allocator, extended))));
                ReaderOptions options = ReaderOptions.newBuilder().withSettings(settings)
                        .withBufferAllocator(allocator).withReuseBatch(false).build();
                VectorSchemaRoot retained;
                try (SplitReader<VectorSchemaRoot> reader = session.createArrowReader(
                        session.getInputSplitAssigner().getSplit(0), options)) {
                    assertTrue(reader.hasNext());
                    retained = reader.get();
                    assertFalse(reader.hasNext());
                }
                try (VectorSchemaRoot root = retained) {
                    assertEquals(42, ((BigIntVector) root.getVector("a")).get(0));
                }
                assertEquals(0, allocator.getAllocatedMemory());
            }
        }
    }

    @Test
    public void recordReaderSupportsBothBatchOwnershipModes() throws Exception {
        for (boolean reuse : new boolean[]{false, true}) {
            for (boolean extended : new boolean[]{false, true}) {
                enqueue("\"SplitMode\":\"Size\",\"SplitsCount\":1,\"ExtendedArrowIPCEnabled\":" + extended);
                TableBatchReadSession session = builder().buildBatchReadSession();
                try (RootAllocator allocator = new RootAllocator()) {
                    server.enqueue(new MockResponse().setBody(new Buffer().write(oneBatch(allocator, extended))));
                    ReaderOptions options = ReaderOptions.newBuilder().withSettings(settings)
                            .withBufferAllocator(allocator).withReuseBatch(reuse).build();
                    try (SplitReader<com.aliyun.odps.data.ArrayRecord> reader = session.createRecordReader(
                            session.getInputSplitAssigner().getSplit(0), options)) {
                        assertTrue(reader.hasNext());
                        assertEquals(Long.valueOf(42), reader.get().getBigint(0));
                        assertFalse(reader.hasNext());
                    }
                    assertEquals(0, allocator.getAllocatedMemory());
                }
            }
        }
    }

    private byte[] oneBatch(RootAllocator allocator, boolean extended) throws Exception {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        if (extended) {
            wire.write(1);
        }
        try (BigIntVector vector = new BigIntVector("a", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, 42);
            vector.setValueCount(1);
            try (VectorSchemaRoot root = VectorSchemaRoot.of(vector);
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, null, wire)) {
                root.setRowCount(1);
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
        return wire.toByteArray();
    }

}
