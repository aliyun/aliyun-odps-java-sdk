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

package com.aliyun.odps.storage.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.storage.MaxStorageException;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.internal.models.WriteSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.type.TypeInfoFactory;

import okhttp3.RequestBody;

/**
 * Tests for {@link TableArrowWriter#flushAsync()} multi-buffered upload semantics.
 *
 * <p>{@link StorageStub#writeTable} is stubbed so no network calls are made; the focus is on
 * buffer management, backpressure, ordering, and exception propagation.
 */
class TableArrowWriterFlushAsyncTest {

    private BufferAllocator allocator;
    private StorageStub stub;
    private ExecutorService executor;
    private TableArrowWriter writer;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(64L * 1024 * 1024);
        stub = mock(StorageStub.class);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "test-async-flush");
            t.setDaemon(true);
            return t;
        });
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    private TableArrowWriter newWriter(long bufferSize) {
        return newWriter(bufferSize, 1);
    }

    private TableArrowWriter newWriter(long bufferSize, int maxPendingBuffers) {
        TableIdentifier tid = TableIdentifier.of("p", "t");
        Column col = new Column("a", TypeInfoFactory.INT);
        WriteSchema ws = new WriteSchema();
        ws.setColumns(Collections.singletonList(col));

        CreateWriteStreamResponse resp = mock(CreateWriteStreamResponse.class);
        when(resp.getDataSchema()).thenReturn(ws);
        when(resp.getTableId()).thenReturn("tid");
        when(resp.getSchemaVersion()).thenReturn(1L);
        when(resp.getRouteToken()).thenReturn(null);

        TableWriterBuilder b = new TableWriterBuilder(stub, tid, null, allocator,
                                                      "sess", "stream", 1L,
                                                      WriteMode.STREAMING, null, null);
        b.withAutoFlushEnabled(true).withBufferSize(bufferSize);
        b.withExecutorService(executor);
        b.withMaxPendingBuffers(maxPendingBuffers);

        return new TableArrowWriter(b, resp);
    }

    private VectorSchemaRoot makeBatch(int rows) {
        IntVector v = new IntVector("a", allocator);
        v.allocateNew(rows);
        for (int i = 0; i < rows; i++) {
            v.set(i, i);
        }
        v.setValueCount(rows);
        return new VectorSchemaRoot(
            Collections.singletonList(v.getField()),
            Collections.<FieldVector>singletonList(v),
            rows);
    }

    private HttpResponse okResponse() {
        HttpResponse r = new HttpResponse();
        r.setStatusCode(200);
        r.setRequestId("req-test");
        r.setHeaders(new HashMap<>());
        return r;
    }

    @Test
    void emptyBufferReturnsCompletedFutureWithoutNetworkCall() throws Exception {
        writer = newWriter(64L * 1024 * 1024);

        Future<Void> f = writer.flushAsync();

        assertTrue(f.isDone(), "future should be completed when buffer is empty");
        assertNull(f.get(1, TimeUnit.SECONDS));
        verify(stub, never()).writeTable(any(), any(), any(), anyLong(), anyLong(),
                                          any(RequestBody.class), any(), any(), any(), anyLong(), any(), any());
    }

    @Test
    void swapHappensSynchronouslyOnCallerThreadAndUploadOnExecutor() throws Exception {
        writer = newWriter(64L * 1024 * 1024);
        AtomicReference<String> uploadThreadName = new AtomicReference<>();
        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenAnswer(inv -> {
                uploadThreadName.set(Thread.currentThread().getName());
                return okResponse();
            });

        try (VectorSchemaRoot batch = makeBatch(10)) {
            writer.writeBatch(batch);
        }
        assertTrue(writer.getCachedSize() > 0, "data should be in buffer before flush");

        Future<Void> f = writer.flushAsync();
        assertEquals(0L, writer.getCachedSize(),
            "cachedSize must be reset on caller thread before submitting upload");
        assertNotNull(f);

        f.get(2, TimeUnit.SECONDS);
        verify(stub, times(1)).writeTable(any(), any(), any(), anyLong(), anyLong(),
                                           any(RequestBody.class), any(), any(), any(), anyLong(), any(), any());
        assertEquals("test-async-flush", uploadThreadName.get(),
            "upload must run on the configured executor, not the caller thread");
    }

    @Test
    void writesAfterFlushGoIntoNewBufferAndUploadSeparately() throws Exception {
        writer = newWriter(64L * 1024 * 1024);
        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenReturn(okResponse());

        try (VectorSchemaRoot b1 = makeBatch(10)) {
            writer.writeBatch(b1);
        }
        Future<Void> f1 = writer.flushAsync();
        try (VectorSchemaRoot b2 = makeBatch(5)) {
            writer.writeBatch(b2);
        }
        assertTrue(writer.getCachedSize() > 0, "second writeBatch must populate the new buffer");

        Future<Void> f2 = writer.flushAsync();
        f1.get(2, TimeUnit.SECONDS);
        f2.get(2, TimeUnit.SECONDS);

        verify(stub, times(2)).writeTable(any(), any(), any(), anyLong(), anyLong(),
                                           any(RequestBody.class), any(), any(), any(), anyLong(), any(), any());
    }

    @Test
    void backpressureBlocksSecondFlushUntilFirstCompletes() throws Exception {
        writer = newWriter(64L * 1024 * 1024);
        CountDownLatch firstStart = new CountDownLatch(1);
        CountDownLatch firstRelease = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger();

        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenAnswer(inv -> {
                int n = callCount.incrementAndGet();
                if (n == 1) {
                    firstStart.countDown();
                    firstRelease.await();
                }
                return okResponse();
            });

        try (VectorSchemaRoot b1 = makeBatch(10)) {
            writer.writeBatch(b1);
        }
        Future<Void> f1 = writer.flushAsync();
        assertTrue(firstStart.await(2, TimeUnit.SECONDS), "first upload should start");
        assertFalse(f1.isDone(), "first upload should still be running");

        try (VectorSchemaRoot b2 = makeBatch(5)) {
            writer.writeBatch(b2);
        }
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                Future<Void> f2 = writer.flushAsync();
                f2.get(5, TimeUnit.SECONDS);
            } catch (Throwable t) {
                secondFailure.set(t);
            }
        }, "second-flush-caller");
        caller.start();

        Thread.sleep(200);
        assertTrue(caller.isAlive(),
            "second flushAsync must block on first while it is in flight");

        firstRelease.countDown();
        caller.join(5_000);
        assertFalse(caller.isAlive(), "second caller should finish after first releases");
        assertNull(secondFailure.get(), "second flush should succeed");

        verify(stub, times(2)).writeTable(any(), any(), any(), anyLong(), anyLong(),
                                           any(RequestBody.class), any(), any(), any(), anyLong(), any(), any());
    }

    @Test
    void asyncFailureSurfacesOnNextWriteBatch() throws Exception {
        writer = newWriter(64L * 1024 * 1024);
        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenThrow(new MaxStorageException("boom"));

        try (VectorSchemaRoot b = makeBatch(10)) {
            writer.writeBatch(b);
        }
        Future<Void> f = writer.flushAsync();
        try {
            f.get(2, TimeUnit.SECONDS);
            fail("expected ExecutionException from failed upload");
        } catch (ExecutionException expected) {
            assertTrue(expected.getCause() instanceof MaxStorageException);
        }

        assertThrows(MaxStorageException.class, () -> {
            try (VectorSchemaRoot b2 = makeBatch(1)) {
                writer.writeBatch(b2);
            }
        });
    }

    @Test
    void asyncFailureSurfacesOnNextFlushAsync() throws Exception {
        writer = newWriter(64L * 1024 * 1024);
        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenThrow(new MaxStorageException("boom"));

        try (VectorSchemaRoot b = makeBatch(10)) {
            writer.writeBatch(b);
        }
        Future<Void> f = writer.flushAsync();
        try {
            f.get(2, TimeUnit.SECONDS);
        } catch (ExecutionException ignored) { /* expected */ }

        assertThrows(MaxStorageException.class, () -> writer.flushAsync());
    }

    @Test
    void asyncFailureUsesDistinctExceptionsForWriteAndClose() throws Exception {
        writer = newWriter(64L * 1024 * 1024);
        ServiceException uploadFailure =
            new ServiceException(409, "ConcurrentOperation", "boom", "req-upload");
        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenThrow(uploadFailure);

        try (VectorSchemaRoot b = makeBatch(10)) {
            writer.writeBatch(b);
        }
        Future<Void> future = writer.flushAsync();
        assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));

        ServiceException writeFailure = assertThrows(ServiceException.class, () -> {
            try (VectorSchemaRoot b = makeBatch(1)) {
                writer.writeBatch(b);
            }
        });
        ServiceException closeFailure = assertThrows(ServiceException.class, writer::close);

        assertNotSame(writeFailure, closeFailure,
            "write and cleanup must not rethrow the same Throwable instance");
        assertEquals(uploadFailure.getHttpStatus(), writeFailure.getHttpStatus());
        assertEquals(uploadFailure.getErrorCode(), writeFailure.getErrorCode());
        assertEquals(uploadFailure.getRequestId(), writeFailure.getRequestId());
        assertSame(uploadFailure, writeFailure.getCause());
        assertSame(uploadFailure, closeFailure.getCause());
        writeFailure.addSuppressed(closeFailure);
        assertEquals(1, writeFailure.getSuppressed().length);
    }

    @Test
    void syncFlushFailurePreservesBufferedRowCountForRetry() {
        writer = newWriter(64L * 1024 * 1024);
        List<Long> rowCounts = new ArrayList<>();
        AtomicInteger callCount = new AtomicInteger();
        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenAnswer(inv -> {
                rowCounts.add(inv.getArgument(4));
                if (callCount.incrementAndGet() == 1) {
                    throw new MaxStorageException("boom");
                }
                return okResponse();
            });

        try (VectorSchemaRoot b = makeBatch(10)) {
            writer.writeBatch(b);
        }

        assertThrows(MaxStorageException.class, () -> writer.flush());
        writer.flush();

        List<Long> expectedRowCounts = new ArrayList<>();
        expectedRowCounts.add(10L);
        expectedRowCounts.add(10L);
        assertEquals(expectedRowCounts, rowCounts,
            "sync flush should preserve row count and buffered batches after a failure");
    }

    @Test
    void syncFlushAfterAsyncWaitsForInFlightUpload() throws Exception {
        writer = newWriter(64L * 1024 * 1024);
        CountDownLatch asyncStart = new CountDownLatch(1);
        CountDownLatch asyncRelease = new CountDownLatch(1);
        AtomicInteger order = new AtomicInteger();
        AtomicReference<Integer> asyncOrder = new AtomicReference<>();
        AtomicReference<Integer> syncOrder = new AtomicReference<>();

        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenAnswer(inv -> {
                if (asyncOrder.get() == null) {
                    asyncStart.countDown();
                    asyncRelease.await();
                    asyncOrder.set(order.incrementAndGet());
                } else {
                    syncOrder.set(order.incrementAndGet());
                }
                return okResponse();
            });

        try (VectorSchemaRoot b1 = makeBatch(10)) {
            writer.writeBatch(b1);
        }
        writer.flushAsync();
        assertTrue(asyncStart.await(2, TimeUnit.SECONDS));

        try (VectorSchemaRoot b2 = makeBatch(5)) {
            writer.writeBatch(b2);
        }

        Thread caller = new Thread(() -> writer.flush(), "sync-flush-caller");
        caller.start();
        Thread.sleep(100);
        assertTrue(caller.isAlive(), "sync flush must wait for in-flight async");

        asyncRelease.countDown();
        caller.join(5_000);
        assertFalse(caller.isAlive());

        assertEquals(Integer.valueOf(1), asyncOrder.get(),
            "async upload must complete first");
        assertEquals(Integer.valueOf(2), syncOrder.get(),
            "sync upload must run second");
    }

    @Test
    void writeBatchAutoFlushTriggersUploadAtBufferSize() throws Exception {
        writer = newWriter(1L);  // any non-empty batch triggers flush

        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenReturn(okResponse());

        try (VectorSchemaRoot batch = makeBatch(10)) {
            writer.writeBatch(batch);
        }

        assertEquals(0L, writer.getCachedSize());

        writer.flush();

        verify(stub, times(1)).writeTable(any(), any(), any(), anyLong(), anyLong(),
                                           any(RequestBody.class), any(), any(), any(), anyLong(), any(), any());
    }

    // ---- Multi-buffer tests ----

    @Test
    void multiBuffer_writerDoesNotBlockWhenPermitsAvailable() throws Exception {
        writer = newWriter(64L * 1024 * 1024, 3);
        CountDownLatch blockFlush = new CountDownLatch(1);

        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenAnswer(inv -> {
                blockFlush.await();
                return okResponse();
            });

        for (int i = 0; i < 3; i++) {
            try (VectorSchemaRoot b = makeBatch(10)) {
                writer.writeBatch(b);
            }
            writer.flushAsync();
        }

        assertEquals(0L, writer.getCachedSize(),
            "all 3 flushAsync calls should return without blocking");

        blockFlush.countDown();
        writer.flush();

        verify(stub, times(3)).writeTable(any(), any(), any(), anyLong(), anyLong(),
                                           any(RequestBody.class), any(), any(), any(), anyLong(), any(), any());
    }

    @Test
    void multiBuffer_backpressureBlocksWhenAllPermitsUsed() throws Exception {
        writer = newWriter(64L * 1024 * 1024, 2);
        CountDownLatch blockFlush = new CountDownLatch(1);

        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenAnswer(inv -> {
                blockFlush.await();
                return okResponse();
            });

        for (int i = 0; i < 2; i++) {
            try (VectorSchemaRoot b = makeBatch(10)) {
                writer.writeBatch(b);
            }
            writer.flushAsync();
        }

        try (VectorSchemaRoot b = makeBatch(10)) {
            writer.writeBatch(b);
        }

        Thread caller = new Thread(() -> writer.flushAsync(), "blocked-flush");
        caller.start();
        Thread.sleep(300);
        assertTrue(caller.isAlive(),
            "3rd flushAsync must block when all 2 permits are used");

        blockFlush.countDown();
        caller.join(5_000);
        assertFalse(caller.isAlive(), "caller should finish after permits freed");

        writer.flush();
        verify(stub, times(3)).writeTable(any(), any(), any(), anyLong(), anyLong(),
                                           any(RequestBody.class), any(), any(), any(), anyLong(), any(), any());
    }

    @Test
    void multiBuffer_flushesExecuteInSubmissionOrder() throws Exception {
        writer = newWriter(64L * 1024 * 1024, 3);
        List<Long> flushOrder = Collections.synchronizedList(new ArrayList<>());

        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenAnswer(inv -> {
                long rowCount = inv.getArgument(4);
                flushOrder.add(rowCount);
                return okResponse();
            });

        int[] rowCounts = {10, 20, 30};
        for (int rc : rowCounts) {
            try (VectorSchemaRoot b = makeBatch(rc)) {
                writer.writeBatch(b);
            }
            writer.flushAsync();
        }

        writer.flush();
        assertEquals(3, flushOrder.size());
        assertEquals(10L, flushOrder.get(0));
        assertEquals(20L, flushOrder.get(1));
        assertEquals(30L, flushOrder.get(2));
    }

    @Test
    void multiBuffer_closeWaitsForAllPendingBuffers() throws Exception {
        writer = newWriter(64L * 1024 * 1024, 3);
        AtomicInteger completedFlushes = new AtomicInteger(0);

        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenAnswer(inv -> {
                Thread.sleep(50);
                completedFlushes.incrementAndGet();
                return okResponse();
            });

        for (int i = 0; i < 3; i++) {
            try (VectorSchemaRoot b = makeBatch(10)) {
                writer.writeBatch(b);
            }
            writer.flushAsync();
        }

        writer.close();
        assertEquals(3, completedFlushes.get(),
            "all pending flushes must complete before close returns");
    }

    @Test
    void multiBuffer_errorInMiddleFlushSurfacesOnWriter() throws Exception {
        writer = newWriter(64L * 1024 * 1024, 3);
        CountDownLatch blockAll = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger();

        when(stub.writeTable(any(), any(), any(), anyLong(), anyLong(),
                              any(RequestBody.class), any(), any(), any(), anyLong(), any(), any()))
            .thenAnswer(inv -> {
                blockAll.await();
                if (callCount.incrementAndGet() == 2) {
                    throw new MaxStorageException("boom on 2nd flush");
                }
                return okResponse();
            });

        @SuppressWarnings("unchecked")
        Future<Void>[] futures = new Future[3];
        for (int i = 0; i < 3; i++) {
            try (VectorSchemaRoot b = makeBatch(10)) {
                writer.writeBatch(b);
            }
            futures[i] = writer.flushAsync();
        }

        blockAll.countDown();

        for (Future<Void> f : futures) {
            try { f.get(2, TimeUnit.SECONDS); } catch (Exception ignored) { }
        }

        assertThrows(MaxStorageException.class, () -> writer.flush());
    }
}
