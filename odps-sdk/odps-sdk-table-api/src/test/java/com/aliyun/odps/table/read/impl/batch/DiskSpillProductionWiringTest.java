package com.aliyun.odps.table.read.impl.batch;

import com.aliyun.odps.table.configuration.DiskSpillBufferOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.utils.ArrowUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Mimics the exact flink-connector-odps wiring:
 *  - DiskSpillBufferOptions built WITHOUT withAllocator() -> uses getDefaultRootAllocator()
 *  - delegate reader uses a SEPARATE allocator
 *  - reuseBatch=false semantics, consumer closes each root (like ColumnarRowIterator)
 *  - default memoryBufferCapacity (64) with a slow consumer to force real backpressure spill
 */
public class DiskSpillProductionWiringTest {

    static class FakeArrowReader implements SplitReader<VectorSchemaRoot> {
        private final BufferAllocator allocator;
        private final int numBatches;
        private final int rowsPerBatch;
        private int produced = 0;
        private VectorSchemaRoot current;
        private final Schema schema = new Schema(Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("data", new ArrowType.Utf8())));

        FakeArrowReader(BufferAllocator allocator, int numBatches, int rowsPerBatch) {
            this.allocator = allocator;
            this.numBatches = numBatches;
            this.rowsPerBatch = rowsPerBatch;
        }

        @Override
        public boolean hasNext() {
            if (produced >= numBatches) {
                return false;
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            IntVector id = (IntVector) root.getVector("id");
            VarCharVector data = (VarCharVector) root.getVector("data");
            id.allocateNew(rowsPerBatch);
            data.allocateNew(rowsPerBatch);
            for (int i = 0; i < rowsPerBatch; i++) {
                id.set(i, produced * rowsPerBatch + i);
                data.set(i, ("b" + produced + "-r" + i).getBytes(StandardCharsets.UTF_8));
            }
            id.setValueCount(rowsPerBatch);
            data.setValueCount(rowsPerBatch);
            root.setRowCount(rowsPerBatch);
            current = root;
            produced++;
            return true;
        }

        @Override
        public VectorSchemaRoot get() {
            return current;
        }

        @Override
        public Metrics currentMetricsValues() {
            return new Metrics();
        }

        @Override
        public void close() {
            // delegate.close() in producerLoop; nothing outstanding here
        }
    }

    @Test
    public void testProductionWiringDefaultAllocatorNoLeak() throws Exception {
        // Faithful to production: delegate (SplitArrowReaderImpl) allocates from
        // ReaderOptions.bufferAllocator which defaults to getDefaultRootAllocator().
        RootAllocator defaultRoot = ArrowUtils.getDefaultRootAllocator();
        BufferAllocator sourceAllocator = defaultRoot;
        long baseline = defaultRoot.getAllocatedMemory();

        int numBatches = 300;
        int rowsPerBatch = 200;
        FakeArrowReader delegate = new FakeArrowReader(sourceAllocator, numBatches, rowsPerBatch);

        // Exactly like OdpsSourceSplitReader: no withAllocator, default capacity 64.
        DiskSpillBufferOptions options = DiskSpillBufferOptions.newBuilder()
                .withMemoryBufferCapacity(64)
                .withMaxSpillBytes(10L * 1024 * 1024 * 1024)
                .build();

        DiskSpillBufferedSplitReader reader = new DiskSpillBufferedSplitReader(delegate, options);

        long expected = 0;
        int consumed = 0;
        try {
            while (reader.hasNext()) {
                VectorSchemaRoot root = reader.get();
                IntVector id = (IntVector) root.getVector("id");
                for (int i = 0; i < root.getRowCount(); i++) {
                    Assert.assertEquals("order mismatch", expected, id.get(i));
                    expected++;
                }
                consumed++;
                root.close();                  // consumer owns it (ColumnarRowIterator.releaseBatch)
                if (consumed % 5 == 0) {
                    Thread.sleep(2);           // slow consumer -> backpressure -> spill
                }
            }
        } finally {
            reader.close();
        }

        long leaked = defaultRoot.getAllocatedMemory() - baseline;
        long srcLeaked = sourceAllocator.getAllocatedMemory();
        System.out.println("consumed=" + consumed + " expectedRows=" + expected
                + " defaultRootLeaked=" + leaked + " sourceLeaked=" + srcLeaked);

        Assert.assertEquals((long) numBatches * rowsPerBatch, expected);
        Assert.assertEquals("default root allocator leaked", 0, leaked);
        // sourceAllocator is the shared default root; do not close
    }

    @Test
    public void testProductionWiringAllowsDeferredConsumerRelease() throws Exception {
        RootAllocator defaultRoot = ArrowUtils.getDefaultRootAllocator();
        BufferAllocator sourceAllocator = defaultRoot;
        long baseline = defaultRoot.getAllocatedMemory();

        FakeArrowReader delegate = new FakeArrowReader(sourceAllocator, 120, 200);
        DiskSpillBufferOptions options = DiskSpillBufferOptions.newBuilder()
                .withMemoryBufferCapacity(8)
                .withMaxSpillBytes(10L * 1024 * 1024 * 1024)
                .build();

        DiskSpillBufferedSplitReader reader = new DiskSpillBufferedSplitReader(delegate, options);
        List<VectorSchemaRoot> pendingRelease = new ArrayList<>();
        long expected = 0;
        try {
            while (reader.hasNext()) {
                VectorSchemaRoot root = reader.get();
                IntVector id = (IntVector) root.getVector("id");
                for (int i = 0; i < root.getRowCount(); i++) {
                    Assert.assertEquals("order mismatch", expected, id.get(i));
                    expected++;
                }
                pendingRelease.add(root);
                if (pendingRelease.size() % 4 == 0) {
                    Thread.sleep(2);
                }
            }
            reader.close();
        } finally {
            for (VectorSchemaRoot root : pendingRelease) {
                root.close();
            }
        }

        long leaked = defaultRoot.getAllocatedMemory() - baseline;
        Assert.assertEquals(120L * 200, expected);
        Assert.assertEquals("default root allocator leaked", 0, leaked);
    }
}
