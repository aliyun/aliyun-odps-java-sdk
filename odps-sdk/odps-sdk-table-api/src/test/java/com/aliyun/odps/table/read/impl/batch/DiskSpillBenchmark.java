package com.aliyun.odps.table.read.impl.batch;

import com.aliyun.odps.table.configuration.DiskSpillBufferOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.read.SplitReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Benchmark comparing direct read vs. disk-spill buffered read.
 * Run with: mvn exec:java or just call main().
 */
public class DiskSpillBenchmark {

    private static final int WARMUP_BATCHES = 100;
    private static final int BENCH_BATCHES = 1000;
    private static final int ROUNDS = 3;
    private static final int[] ROW_COUNTS = {1000, 4096, 10000};

    public static void main(String[] args) throws Exception {
        System.out.println("=== DiskSpillBufferedSplitReader Benchmark ===\n");

        for (int rowCount : ROW_COUNTS) {
            System.out.printf("--- %d rows/batch, %d batches ---\n", rowCount, BENCH_BATCHES);

            long batchBytes = estimateBatchBytes(rowCount);
            System.out.printf("Estimated batch size: %.1f KB\n", batchBytes / 1024.0);

            // Warmup
            runDirect(WARMUP_BATCHES, rowCount);
            runBuffered(WARMUP_BATCHES, rowCount, WARMUP_BATCHES + 10);
            runBuffered(WARMUP_BATCHES, rowCount, 0);

            // Benchmark: take best of N rounds
            long directNs = Long.MAX_VALUE;
            long memoryNs = Long.MAX_VALUE;
            long spillNs = Long.MAX_VALUE;
            for (int r = 0; r < ROUNDS; r++) {
                directNs = Math.min(directNs, runDirect(BENCH_BATCHES, rowCount));
                memoryNs = Math.min(memoryNs, runBuffered(BENCH_BATCHES, rowCount, BENCH_BATCHES + 10));
                spillNs = Math.min(spillNs, runBuffered(BENCH_BATCHES, rowCount, 0));
            }

            double directThroughput = throughputMBs(BENCH_BATCHES, batchBytes, directNs);
            double memoryThroughput = throughputMBs(BENCH_BATCHES, batchBytes, memoryNs);
            double spillThroughput = throughputMBs(BENCH_BATCHES, batchBytes, spillNs);

            System.out.printf("  Direct read:         %8.1f ms  %8.1f MB/s  (baseline)\n",
                    directNs / 1e6, directThroughput);
            System.out.printf("  Buffered (in-memory): %7.1f ms  %8.1f MB/s  (%+.1f%%)\n",
                    memoryNs / 1e6, memoryThroughput, pctChange(directNs, memoryNs));
            System.out.printf("  Buffered (all-spill): %7.1f ms  %8.1f MB/s  (%+.1f%%)\n",
                    spillNs / 1e6, spillThroughput, pctChange(directNs, spillNs));
            System.out.println();
        }
    }

    /**
     * Baseline: read directly from mock reader, close each batch.
     */
    static long runDirect(int batchCount, int rowCount) throws Exception {
        RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        BenchMockReader mock = new BenchMockReader(alloc, batchCount, rowCount);

        long start = System.nanoTime();
        while (mock.hasNext()) {
            VectorSchemaRoot root = mock.get();
            root.close();
        }
        long elapsed = System.nanoTime() - start;

        mock.close();
        alloc.close();
        return elapsed;
    }

    /**
     * Read through DiskSpillBufferedSplitReader.
     * memoryCapacity=0 forces all-spill; memoryCapacity > batchCount forces all-memory.
     */
    static long runBuffered(int batchCount, int rowCount, int memoryCapacity) throws Exception {
        RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        BenchMockReader mock = new BenchMockReader(alloc, batchCount, rowCount);

        File tmpDir = new File(System.getProperty("java.io.tmpdir"),
                "bench-spill-" + System.nanoTime());
        tmpDir.mkdirs();

        DiskSpillBufferOptions options = DiskSpillBufferOptions.newBuilder()
                .withMemoryBufferCapacity(memoryCapacity)
                .withSpillDirectory(tmpDir)
                .withAllocator(alloc)
                .build();

        DiskSpillBufferedSplitReader reader = new DiskSpillBufferedSplitReader(mock, options);

        long start = System.nanoTime();
        while (reader.hasNext()) {
            VectorSchemaRoot root = reader.get();
            root.close();
        }
        long elapsed = System.nanoTime() - start;

        reader.close();
        deleteDir(tmpDir);
        alloc.close();
        return elapsed;
    }

    static long estimateBatchBytes(int rowCount) {
        // int(4) + double(8) + varchar(~64 avg) = ~76 bytes/row
        return (long) rowCount * 76;
    }

    static double throughputMBs(int batches, long batchBytes, long elapsedNs) {
        double totalBytes = (double) batches * batchBytes;
        double seconds = elapsedNs / 1e9;
        return totalBytes / (1024.0 * 1024.0) / seconds;
    }

    static double pctChange(long baselineNs, long actualNs) {
        return ((double) actualNs / baselineNs - 1.0) * 100.0;
    }

    static void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) f.delete();
        }
        dir.delete();
    }

    // -- Mock reader producing realistic-sized batches --

    static Schema benchSchema() {
        return new Schema(Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("value", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                Field.nullable("payload", new ArrowType.Utf8())
        ));
    }

    static class BenchMockReader implements SplitReader<VectorSchemaRoot> {
        private final BufferAllocator allocator;
        private final int totalBatches;
        private final int rowCount;
        private int currentIndex = -1;
        private VectorSchemaRoot currentBatch;

        BenchMockReader(BufferAllocator allocator, int totalBatches, int rowCount) {
            this.allocator = allocator;
            this.totalBatches = totalBatches;
            this.rowCount = rowCount;
        }

        @Override
        public boolean hasNext() throws IOException {
            currentIndex++;
            if (currentIndex >= totalBatches) return false;
            currentBatch = createBatch(currentIndex);
            return true;
        }

        @Override
        public VectorSchemaRoot get() { return currentBatch; }

        @Override
        public Metrics currentMetricsValues() { return new Metrics(); }

        @Override
        public void close() throws IOException {
            if (currentBatch != null) {
                currentBatch.close();
                currentBatch = null;
            }
        }

        private VectorSchemaRoot createBatch(int batchId) {
            VectorSchemaRoot root = VectorSchemaRoot.create(benchSchema(), allocator);
            IntVector idVec = (IntVector) root.getVector("id");
            Float8Vector valVec = (Float8Vector) root.getVector("value");
            VarCharVector payloadVec = (VarCharVector) root.getVector("payload");

            idVec.allocateNew(rowCount);
            valVec.allocateNew(rowCount);
            payloadVec.allocateNew((long) rowCount * 64, rowCount);

            byte[] payload = new byte[64];
            Arrays.fill(payload, (byte) 'x');

            for (int r = 0; r < rowCount; r++) {
                idVec.set(r, batchId * rowCount + r);
                valVec.set(r, r * 0.1);
                payloadVec.set(r, payload);
            }
            idVec.setValueCount(rowCount);
            valVec.setValueCount(rowCount);
            payloadVec.setValueCount(rowCount);
            root.setRowCount(rowCount);
            return root;
        }
    }
}
