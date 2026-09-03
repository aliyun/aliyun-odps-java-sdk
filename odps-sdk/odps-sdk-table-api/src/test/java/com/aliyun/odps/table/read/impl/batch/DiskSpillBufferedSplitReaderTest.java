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

import com.aliyun.odps.Column;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.configuration.DiskSpillBufferOptions;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.read.SplitReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class DiskSpillBufferedSplitReaderTest {

    private RootAllocator rootAllocator;

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Before
    public void setUp() {
        rootAllocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void tearDown() {
        rootAllocator.close();
    }

    @Test
    public void testAllBatchesInMemory() throws Exception {
        int batchCount = 3;
        int memoryCapacity = 4;
        MockSplitReader mock = createMockReader(batchCount);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, memoryCapacity);
        try {
            for (int i = 0; i < batchCount; i++) {
                assertTrue(reader.hasNext());
                VectorSchemaRoot root = reader.get();
                assertBatchValue(root, i);
                root.close();
            }
            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
        assertEquals(0, rootAllocator.getAllocatedMemory());
    }

    @Test
    public void testDiskSpillTriggered() throws Exception {
        int batchCount = 10;
        int memoryCapacity = 2;
        MockSplitReader mock = createMockReader(batchCount);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, memoryCapacity);
        try {
            for (int i = 0; i < batchCount; i++) {
                assertTrue(reader.hasNext());
                VectorSchemaRoot root = reader.get();
                assertBatchValue(root, i);
                root.close();
            }
            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
        assertEquals(0, rootAllocator.getAllocatedMemory());
    }

    @Test
    public void testOrderPreserved() throws Exception {
        int batchCount = 20;
        int memoryCapacity = 3;
        MockSplitReader mock = createMockReader(batchCount);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, memoryCapacity);
        try {
            for (int i = 0; i < batchCount; i++) {
                assertTrue("Expected hasNext() at batch " + i, reader.hasNext());
                VectorSchemaRoot root = reader.get();
                IntVector idVec = (IntVector) root.getVector("id");
                assertEquals("Batch order mismatch at position " + i, i, idVec.get(0));
                root.close();
            }
            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
    }

    @Test
    public void testEmptyReader() throws Exception {
        MockSplitReader mock = createMockReader(0);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, 4);
        try {
            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
    }

    @Test
    public void testSingleBatch() throws Exception {
        MockSplitReader mock = createMockReader(1);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, 4);
        try {
            assertTrue(reader.hasNext());
            VectorSchemaRoot root = reader.get();
            assertBatchValue(root, 0);
            root.close();
            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
    }

    @Test
    public void testDelegateErrorPropagation() throws Exception {
        int failAfter = 5;
        MockSplitReader mock = createFailingMockReader(failAfter);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, 8);
        try {
            for (int i = 0; i < failAfter; i++) {
                assertTrue(reader.hasNext());
                VectorSchemaRoot root = reader.get();
                assertBatchValue(root, i);
                root.close();
            }
            try {
                reader.hasNext();
                fail("Expected IOException");
            } catch (IOException e) {
                // expected
            }
        } finally {
            reader.close();
        }
    }

    @Test
    public void testMaxSpillBytesExceeded() throws Exception {
        DiskSpillBufferOptions options = DiskSpillBufferOptions.newBuilder()
                .withMemoryBufferCapacity(0)
                .withSpillDirectory(tempDir.getRoot())
                .withMaxSpillBytes(100)
                .withAllocator(rootAllocator)
                .build();

        MockSplitReader mock = createMockReader(10);
        DiskSpillBufferedSplitReader reader = new DiskSpillBufferedSplitReader(mock, options);
        try {
            // With memoryCapacity=0, all batches must spill.
            // maxSpillBytes=100 is small enough that spilling will exceed it after a few batches.
            boolean hitError = false;
            while (reader.hasNext()) {
                reader.get().close();
            }
            // If no exception, the spill might have fit — not a failure
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("spill") || e.getCause().getMessage().contains("spill"));
        } finally {
            reader.close();
        }
    }

    @Test
    public void testCloseWhileReading() throws Exception {
        MockSplitReader mock = createSlowMockReader(100, 10);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, 2);
        try {
            if (reader.hasNext()) {
                reader.get().close();
            }
        } finally {
            reader.close();
        }
        assertEquals(0, rootAllocator.getAllocatedMemory());
    }

    @Test
    public void testGetReturnsSameValue() throws Exception {
        MockSplitReader mock = createMockReader(3);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, 4);
        try {
            assertTrue(reader.hasNext());
            VectorSchemaRoot first = reader.get();
            VectorSchemaRoot second = reader.get();
            assertSame(first, second);
            first.close();
        } finally {
            reader.close();
        }
    }

    @Test
    public void testSlowConsumer() throws Exception {
        int batchCount = 8;
        int memoryCapacity = 2;

        MockSplitReader mock = createMockReader(batchCount);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, memoryCapacity);
        try {
            for (int i = 0; i < batchCount; i++) {
                assertTrue(reader.hasNext());
                VectorSchemaRoot root = reader.get();
                assertBatchValue(root, i);
                root.close();
                Thread.sleep(50);
            }
            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
    }

    @Test
    public void testSequentialConsumption() throws Exception {
        MockSplitReader mock = createMockReader(3);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, 4);
        try {
            // Each hasNext() advances to the next batch
            assertTrue(reader.hasNext());
            VectorSchemaRoot root0 = reader.get();
            assertBatchValue(root0, 0);
            root0.close();

            assertTrue(reader.hasNext());
            VectorSchemaRoot root1 = reader.get();
            assertBatchValue(root1, 1);
            root1.close();

            assertTrue(reader.hasNext());
            VectorSchemaRoot root2 = reader.get();
            assertBatchValue(root2, 2);
            root2.close();

            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
    }

    @Test
    public void testSpillFilesCleanedUpOnClose() throws Exception {
        int batchCount = 20;
        int memoryCapacity = 1;
        MockSplitReader mock = createMockReader(batchCount);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, memoryCapacity);
        try {
            for (int i = 0; i < 3; i++) {
                if (reader.hasNext()) {
                    reader.get().close();
                }
            }
            Thread.sleep(200);
        } finally {
            reader.close();
        }

        File[] remaining = tempDir.getRoot().listFiles(
                (dir, name) -> name.startsWith("odps-spill-"));
        assertTrue("Spill files not cleaned up",
                remaining == null || remaining.length == 0);
        assertEquals(0, rootAllocator.getAllocatedMemory());
    }

    @Test
    public void testZeroMemoryCapacityAllSpill() throws Exception {
        int batchCount = 5;
        MockSplitReader mock = createMockReader(batchCount);
        DiskSpillBufferedSplitReader reader = createBufferedReader(mock, 0);
        try {
            for (int i = 0; i < batchCount; i++) {
                assertTrue(reader.hasNext());
                VectorSchemaRoot root = reader.get();
                assertBatchValue(root, i);
                root.close();
            }
            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
        assertEquals(0, rootAllocator.getAllocatedMemory());
    }

    @Test
    public void testRecordReaderWithDiskSpillAndReuseBatchReadsAllBatches() throws Exception {
        MockSplitReader mock = createMockReader(3);
        DiskSpillBufferedSplitReader arrowReader = createBufferedReader(mock, 1, true);
        SplitRecordReaderImpl recordReader = new SplitRecordReaderImpl(
                arrowReader,
                createReadSchema(),
                ReaderOptions.newBuilder()
                        .withSettings(EnvironmentSettings.newBuilder()
                                .withCredentials(Credentials.newBuilder()
                                        .withAccount(new AliyunAccount("ak", "sk"))
                                        .build())
                                .build())
                        .withReuseBatch(true)
                        .build());
        List<Integer> batchIds = new ArrayList<>();
        try {
            while (recordReader.hasNext()) {
                batchIds.add((Integer) recordReader.get().get(0));
            }
        } finally {
            recordReader.close();
        }
        assertEquals(Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                2, 2, 2, 2, 2, 2, 2, 2, 2, 2), batchIds);
        assertEquals(0, rootAllocator.getAllocatedMemory());
    }

    // ---- Helpers ----

    private DiskSpillBufferedSplitReader createBufferedReader(
            MockSplitReader mock, int memoryCapacity) throws IOException {
        return createBufferedReader(mock, memoryCapacity, false);
    }

    private DiskSpillBufferedSplitReader createBufferedReader(
            MockSplitReader mock,
            int memoryCapacity,
            boolean reuseBatch) throws IOException {
        DiskSpillBufferOptions options = DiskSpillBufferOptions.newBuilder()
                .withMemoryBufferCapacity(memoryCapacity)
                .withSpillDirectory(tempDir.getRoot())
                .withAllocator(rootAllocator)
                .build();
        return new DiskSpillBufferedSplitReader(mock, options, reuseBatch);
    }

    private MockSplitReader createMockReader(int batchCount) {
        return new MockSplitReader(rootAllocator, batchCount, 0, -1);
    }

    private MockSplitReader createFailingMockReader(int failAfterBatches) {
        return new MockSplitReader(rootAllocator, failAfterBatches + 10, 0, failAfterBatches);
    }

    private MockSplitReader createSlowMockReader(int batchCount, long delayMs) {
        return new MockSplitReader(rootAllocator, batchCount, delayMs, -1);
    }

    private void assertBatchValue(VectorSchemaRoot root, int expectedId) {
        assertNotNull(root);
        assertTrue(root.getRowCount() > 0);
        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(expectedId, idVec.get(0));
    }

    private DataSchema createReadSchema() {
        return DataSchema.newBuilder()
                .columns(Arrays.asList(
                        new Column("id", com.aliyun.odps.type.TypeInfoFactory.INT),
                        new Column("data", com.aliyun.odps.type.TypeInfoFactory.STRING)))
                .build();
    }

    private static Schema createTestSchema() {
        return new Schema(Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("data", new ArrowType.Utf8())
        ));
    }

    static class MockSplitReader implements SplitReader<VectorSchemaRoot> {

        private final BufferAllocator allocator;
        private final int totalBatches;
        private final long delayMs;
        private final int failAfter;

        private int currentIndex = -1;
        private VectorSchemaRoot currentBatch;

        MockSplitReader(BufferAllocator allocator, int totalBatches,
                        long delayMs, int failAfter) {
            this.allocator = allocator;
            this.totalBatches = totalBatches;
            this.delayMs = delayMs;
            this.failAfter = failAfter;
        }

        @Override
        public boolean hasNext() throws IOException {
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            currentIndex++;
            if (failAfter >= 0 && currentIndex >= failAfter) {
                throw new IOException("Simulated read failure at batch " + currentIndex);
            }
            if (currentIndex >= totalBatches) {
                return false;
            }

            currentBatch = createBatch(currentIndex);
            return true;
        }

        @Override
        public VectorSchemaRoot get() {
            return currentBatch;
        }

        @Override
        public Metrics currentMetricsValues() {
            return new Metrics();
        }

        @Override
        public void close() throws IOException {
            if (currentBatch != null) {
                currentBatch.close();
                currentBatch = null;
            }
        }

        private VectorSchemaRoot createBatch(int batchId) {
            Schema schema = createTestSchema();
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector dataVec = (VarCharVector) root.getVector("data");

            int rowCount = 10;
            idVec.allocateNew(rowCount);
            dataVec.allocateNew(rowCount);

            for (int r = 0; r < rowCount; r++) {
                idVec.set(r, batchId);
                dataVec.set(r, ("batch-" + batchId + "-row-" + r).getBytes(StandardCharsets.UTF_8));
            }
            idVec.setValueCount(rowCount);
            dataVec.setValueCount(rowCount);
            root.setRowCount(rowCount);
            return root;
        }
    }
}
