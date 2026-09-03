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

import com.aliyun.odps.table.configuration.DiskSpillBufferOptions;
import com.aliyun.odps.table.metrics.Counter;
import com.aliyun.odps.table.metrics.Gauge;
import com.aliyun.odps.table.metrics.MetricNames;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.utils.ArrowUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A decorator around {@link SplitReader} that continuously drains the delegate on a background
 * thread. Batches are retained in a bounded in-memory queue or written to bounded Arrow IPC spill
 * files, so downstream backpressure does not leave the network connection idle.
 *
 * <p>While active, the producer is the sole owner of calls to the delegate and of the spill writer.
 * After the producer stops, serialized cleanup may retry an incomplete delegate close. The
 * consumer is the sole owner of the active spill reader. During close, neither side's Arrow
 * resources are released until the producer has stopped accessing them.
 */
public class DiskSpillBufferedSplitReader implements SplitReader<VectorSchemaRoot> {

    private static final Logger LOG = LoggerFactory.getLogger(
            DiskSpillBufferedSplitReader.class);
    private static final long MIN_PRODUCER_CLOSE_TIMEOUT_MILLIS =
            TimeUnit.SECONDS.toMillis(5);
    private static final long CANCELLATION_GRACE_MILLIS =
            TimeUnit.SECONDS.toMillis(5);
    private static final AtomicLong READER_ID = new AtomicLong(0);

    private final SplitReader<VectorSchemaRoot> delegate;
    private final DiskSpillBufferOptions options;
    private final boolean reuseBatch;
    private final boolean delegateReuseBatch;
    private final long producerCloseTimeoutMillis;
    private final Metrics metrics;
    private final DiskSpillWorkspace spillWorkspace;

    /**
     * Child allocator used only by spill writer and spill reader internals. It is created from a
     * source vector's allocator so Arrow transfers always share the same root allocator.
     */
    private volatile BufferAllocator allocator;

    /**
     * Allocator used for consumer-visible roots. It is deliberately not the spill child allocator:
     * consumer-owned roots may outlive this reader when {@code reuseBatch} is false.
     */
    private volatile BufferAllocator consumerAllocator;
    private final Object allocatorLock = new Object();

    private final LinkedBlockingDeque<BufferedBatch> orderedQueue =
            new LinkedBlockingDeque<>();
    private final Semaphore memorySlots;
    private final ReentrantLock consumerLock = new ReentrantLock();
    private final Thread producerThread;
    // Signals that delegate/writer finalization is complete; local cleanup may still be running.
    private final CountDownLatch producerFinished = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object closeLock = new Object();
    private final Object cleanupLock = new Object();

    private volatile IOException cancellationFailure;
    private volatile IOException delegateCloseFailure;
    private volatile IOException cleanupFailure;
    private volatile boolean delegateCloseSucceeded;
    private boolean allocatorCloseSucceeded;

    private final AtomicLong spillBatchCount = new AtomicLong(0);
    private final AtomicLong spillBytesWritten = new AtomicLong(0);
    private final AtomicLong spillBytesInUse = new AtomicLong(0);
    private final AtomicLong peakSpillBytesInUse = new AtomicLong(0);
    private final AtomicLong spillFileCount = new AtomicLong(0);
    private final AtomicLong memoryBytesInUse = new AtomicLong(0);
    private final AtomicLong producerCompleted = new AtomicLong(0);
    private final Map<File, SpillFileHandle> activeSpillFiles =
            new ConcurrentHashMap<>();
    // Invalid writer states retained until writer/output/root/file cleanup all succeed.
    private final List<SpillWriterState> retainedSpillWriters = new ArrayList<>();
    // Files whose creation succeeded but whose handle registration did not complete.
    private final List<File> retainedUntrackedSpillFiles = new ArrayList<>();
    // Arrow roots whose first close failed. Entries stay reachable until a later close succeeds.
    private final ConcurrentLinkedQueue<VectorSchemaRoot> retainedArrowRoots =
            new ConcurrentLinkedQueue<>();

    // Consumer state. Access is serialized by consumerLock.
    private ArrowStreamReader currentSpillReader;
    private FileInputStream currentSpillInput;
    private SpillFileHandle currentSpillReaderHandle;
    private boolean currentSpillCleanupOnly;
    private volatile VectorSchemaRoot currentBatch;
    private boolean currentBatchHandedOut;
    private volatile boolean finished;

    /**
     * Creates a buffered reader whose returned batches are owned by the consumer.
     *
     * <p>This constructor preserves the original direct-construction behavior: delegate batches
     * are treated as borrowed/reused and are not closed by this wrapper. Production callers should
     * use the overload that explicitly supplies the reader's reuse setting.
     */
    public DiskSpillBufferedSplitReader(
            SplitReader<VectorSchemaRoot> delegate,
            DiskSpillBufferOptions options) {
        this(delegate, options, false, true, null);
    }

    /**
     * Creates a buffered reader.
     *
     * @param reuseBatch if true, a returned batch is valid only until the next {@link #hasNext()}
     *                   call or close; if false, the consumer owns and must close every returned
     *                   batch. The delegate must use the same reuse setting.
     */
    public DiskSpillBufferedSplitReader(
            SplitReader<VectorSchemaRoot> delegate,
            DiskSpillBufferOptions options,
            boolean reuseBatch) {
        this(delegate, options, reuseBatch, reuseBatch, null);
    }

    /**
     * Internal production path that takes ownership of a workspace prepared before the remote
     * split connection is opened. Ownership transfers only when construction returns
     * successfully; cleanup on a construction failure is best-effort and the caller remains
     * responsible for retrying both the workspace and delegate cleanup.
     */
    DiskSpillBufferedSplitReader(
            SplitReader<VectorSchemaRoot> delegate,
            DiskSpillBufferOptions options,
            boolean reuseBatch,
            DiskSpillWorkspace preparedWorkspace) {
        this(delegate, options, reuseBatch, reuseBatch, preparedWorkspace);
    }

    private DiskSpillBufferedSplitReader(
            SplitReader<VectorSchemaRoot> delegate,
            DiskSpillBufferOptions options,
            boolean reuseBatch,
            boolean delegateReuseBatch,
            DiskSpillWorkspace preparedWorkspace) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate reader must not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("Disk spill options must not be null");
        }
        this.delegate = delegate;
        this.options = options;
        this.reuseBatch = reuseBatch;
        this.delegateReuseBatch = delegateReuseBatch;
        this.producerCloseTimeoutMillis = producerCloseTimeoutMillis(delegate);
        Metrics resolvedMetrics;
        Semaphore resolvedMemorySlots;
        DiskSpillWorkspace resolvedWorkspace = preparedWorkspace;
        try {
            Metrics delegateMetrics = delegate.currentMetricsValues();
            resolvedMetrics = delegateMetrics == null ? new Metrics() : delegateMetrics;
            resolvedMemorySlots = new Semaphore(options.getMemoryBufferCapacity());
            if (resolvedWorkspace == null) {
                resolvedWorkspace = DiskSpillWorkspace.create(options.getSpillDirectory());
            }
        } catch (Throwable constructionFailure) {
            if (resolvedWorkspace != null) {
                addSuppressed(constructionFailure, resolvedWorkspace.cleanup());
            }
            try {
                delegate.close();
            } catch (Throwable closeFailure) {
                addSuppressed(constructionFailure, closeFailure);
            }
            throw constructionException(constructionFailure);
        }
        this.metrics = resolvedMetrics;
        this.memorySlots = resolvedMemorySlots;
        this.spillWorkspace = resolvedWorkspace;

        Thread thread;
        try {
            thread = new Thread(
                    this::producerLoop,
                    "odps-disk-spill-reader-" + READER_ID.incrementAndGet());
        } catch (Throwable constructionFailure) {
            closeDelegateAfterConstructionFailure(constructionFailure);
            throw constructionException(constructionFailure);
        }
        this.producerThread = thread;

        try {
            registerMetrics();
            this.producerThread.setDaemon(true);
            this.producerThread.start();
        } catch (Throwable constructionFailure) {
            closeDelegateAfterConstructionFailure(constructionFailure);
            throw constructionException(constructionFailure);
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        consumerLock.lock();
        try {
            releasePreviousBatch();
            if (finished || closed.get()) {
                return false;
            }

            while (true) {
                if (currentSpillCleanupOnly) {
                    IOException closeFailure = closeCurrentSpillReader(true);
                    if (closeFailure != null) {
                        throw closeFailure;
                    }
                    continue;
                }

                if (currentSpillReader != null) {
                    try {
                        if (currentSpillReader.loadNextBatch()) {
                            currentBatch = copyBatch(
                                    currentSpillReader.getVectorSchemaRoot(),
                                    consumerAllocator(
                                            currentSpillReader.getVectorSchemaRoot()));
                            currentBatchHandedOut = false;
                            return true;
                        }
                    } catch (Throwable e) {
                        finished = true;
                        currentSpillCleanupOnly = true;
                        IOException closeFailure = closeCurrentSpillReader(true);
                        addSuppressed(e, closeFailure);
                        throw propagateAsIOException(
                                "Failed to read disk spill batch", e);
                    }

                    LOG.debug(
                            "Finished reading spill file: {}",
                            currentSpillReaderHandle.file.getName());
                    currentSpillCleanupOnly = true;
                    IOException closeFailure = closeCurrentSpillReader(true);
                    if (closeFailure != null) {
                        throw closeFailure;
                    }
                    continue;
                }

                BufferedBatch item;
                try {
                    item = orderedQueue.takeFirst();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for next batch", e);
                }

                switch (item.type) {
                    case IN_MEMORY:
                        currentBatch = item.root;
                        currentBatchHandedOut = false;
                        memorySlots.release();
                        releaseMemoryBytes(item.memoryBytes);
                        return true;
                    case ON_DISK:
                        return openSpillFile(item.spillFile);
                    case DONE:
                        finished = true;
                        return false;
                    case ERROR:
                        finished = true;
                        throw new IOException("Background reader failed", item.error);
                    case CLOSED:
                        finished = true;
                        return false;
                    default:
                        throw new IllegalStateException("Unknown batch type: " + item.type);
                }
            }
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public VectorSchemaRoot get() {
        consumerLock.lock();
        try {
            if (currentBatch != null) {
                currentBatchHandedOut = true;
            }
            return currentBatch;
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public Metrics currentMetricsValues() {
        return metrics;
    }

    @Override
    public void close() throws IOException {
        synchronized (closeLock) {
            closeInternal();
        }
    }

    private void closeInternal() throws IOException {
        if (closed.compareAndSet(false, true)) {
            finished = true;
            orderedQueue.offerFirst(BufferedBatch.closed());
            producerThread.interrupt();
            if (producerFinished.getCount() != 0) {
                requestDelegateCancellation();
            }
        }

        boolean interrupted = false;
        boolean producerStopped = false;
        long waitStarted = System.nanoTime();
        while (!producerStopped) {
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - waitStarted);
            long remainingMillis = producerCloseTimeoutMillis - elapsedMillis;
            if (remainingMillis <= 0) {
                break;
            }
            try {
                producerStopped = producerFinished.await(
                        remainingMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        IOException failure = null;
        if (!producerStopped) {
            failure = new IOException(
                    "Background reader did not stop within "
                            + producerCloseTimeoutMillis
                            + " ms; Arrow and spill resources were intentionally retained "
                            + "until the producer exits");
            failure = appendFailure(failure, cancellationFailure);
        } else {
            synchronized (cleanupLock) {
                cleanupResourcesLocked();
                failure = appendFailure(failure, cleanupFailure);
                cleanupFailure = null;
                failure = appendFailure(failure, delegateCloseFailure);
                delegateCloseFailure = null;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
            failure = appendFailure(
                    failure,
                    new IOException("Interrupted while waiting for background reader to stop"));
        }
        if (failure != null) {
            throw failure;
        }
    }

    // ---- Producer thread ----

    private void producerLoop() {
        SpillWriterState pendingSpill = null;
        Throwable failure = null;

        try {
            while (!closed.get() && !Thread.currentThread().isInterrupted()) {
                if (!delegate.hasNext()) {
                    break;
                }

                VectorSchemaRoot source = delegate.get();
                Throwable iterationFailure = null;
                try {
                    if (closed.get()) {
                        break;
                    }

                    // Pin the consumer allocator while the remote source root is still available.
                    // Otherwise an all-spill reader could derive it from the spill child allocator,
                    // making consumer-owned batches unsafe after this reader closes.
                    consumerAllocator(source);
                    long batchBytes = estimateBufferBytes(source);
                    boolean slotHeld = memorySlots.tryAcquire();
                    boolean memoryBytesHeld = false;
                    VectorSchemaRoot owned = null;
                    boolean handedOff = false;
                    Throwable processingFailure = null;
                    try {
                        if (slotHeld) {
                            memoryBytesHeld = tryReserveMemoryBytes(batchBytes);
                            if (!memoryBytesHeld) {
                                memorySlots.release();
                                slotHeld = false;
                            }
                        }

                        if (slotHeld) {
                            if (pendingSpill != null) {
                                SpillWriterState writer = pendingSpill;
                                pendingSpill = null;
                                enqueueFinishedSpill(writer);
                            }
                            if (closed.get()) {
                                break;
                            }

                            owned = copyBatch(source, consumerAllocator(source));
                            orderedQueue.addLast(
                                    BufferedBatch.inMemory(owned, batchBytes));
                            handedOff = true;
                            owned = null;
                            slotHeld = false;
                            memoryBytesHeld = false;
                        } else {
                            if (pendingSpill == null) {
                                pendingSpill = new SpillWriterState(source);
                            }
                            pendingSpill.writeBatch(source);
                            if (pendingSpill.bytesWritten()
                                    >= options.getSpillFileTargetBytes()) {
                                SpillWriterState writer = pendingSpill;
                                pendingSpill = null;
                                enqueueFinishedSpill(writer);
                            }
                        }
                    } catch (Throwable t) {
                        processingFailure = t;
                        throw t;
                    } finally {
                        Throwable ownedCloseFailure = null;
                        if (!handedOff && owned != null) {
                            try {
                                owned.close();
                            } catch (Throwable closeError) {
                                retainedArrowRoots.add(owned);
                                ownedCloseFailure = closeError;
                            }
                        }
                        if (slotHeld) {
                            memorySlots.release();
                        }
                        if (memoryBytesHeld) {
                            releaseMemoryBytes(batchBytes);
                        }
                        if (ownedCloseFailure != null) {
                            if (processingFailure == null) {
                                throw ownedCloseFailure;
                            }
                            addSuppressed(processingFailure, ownedCloseFailure);
                        }
                    }
                } catch (Throwable t) {
                    iterationFailure = t;
                    throw t;
                } finally {
                    if (!delegateReuseBatch && source != null) {
                        try {
                            source.close();
                        } catch (Throwable closeError) {
                            retainedArrowRoots.add(source);
                            if (iterationFailure == null) {
                                throw closeError;
                            }
                            addSuppressed(iterationFailure, closeError);
                        }
                    }
                }
            }

            if (pendingSpill != null) {
                SpillWriterState writer = pendingSpill;
                pendingSpill = null;
                if (closed.get()) {
                    IOException abortFailure = writer.abort();
                    if (abortFailure != null) {
                        throw abortFailure;
                    }
                } else {
                    enqueueFinishedSpill(writer);
                }
            }
        } catch (Throwable t) {
            failure = t;
        } finally {
            boolean cleanupAfterProducerStop = false;
            try {
                if (pendingSpill != null) {
                    try {
                        failure = appendThrowable(failure, pendingSpill.abort());
                    } catch (Throwable abortError) {
                        failure = appendThrowable(failure, abortError);
                    }
                }

                Throwable closeError = closeDelegateOnProducerThread();
                failure = appendThrowable(failure, closeError);

                if (!closed.get()) {
                    if (failure == null) {
                        orderedQueue.offerLast(BufferedBatch.done());
                    } else {
                        LOG.error("Background reader failed", failure);
                        orderedQueue.offerLast(BufferedBatch.error(failure));
                    }
                } else if (failure != null) {
                    LOG.debug("Background reader stopped during close", failure);
                }
            } catch (Throwable finalizationError) {
                IOException unexpected = toIOException(
                        "Unexpected failure while finalizing disk spill producer",
                        finalizationError);
                recordCleanupFailure(unexpected);
                if (!closed.get()) {
                    orderedQueue.offerLast(BufferedBatch.error(unexpected));
                }
                LOG.warn("Disk spill producer finalization failed", finalizationError);
            } finally {
                cleanupAfterProducerStop = closed.get();
                producerCompleted.set(1);
                producerFinished.countDown();
            }

            // After publishing producer completion, this thread may only enter the same
            // serialized, idempotent cleanup path used by close().
            if (cleanupAfterProducerStop) {
                // Thread interruption is only a producer-cancellation signal. Leaving it set
                // makes Java NIO file operations fail immediately with
                // ClosedByInterruptException and can prevent workspace cleanup.
                boolean restoreInterrupt = Thread.interrupted();
                try {
                    cleanupResourcesAfterProducerStop();
                } finally {
                    if (restoreInterrupt) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private void enqueueFinishedSpill(SpillWriterState writer) throws IOException {
        SpillFileHandle handle = writer.finish();
        boolean handedOff = false;
        try {
            if (closed.get()) {
                IOException deleteFailure = deleteSpillFile(handle);
                if (deleteFailure != null) {
                    recordCleanupFailure(deleteFailure);
                    throw deleteFailure;
                }
                return;
            }
            orderedQueue.addLast(BufferedBatch.onDisk(handle));
            handedOff = true;
        } finally {
            if (!handedOff && !handle.released.get()) {
                IOException deleteFailure = deleteSpillFile(handle);
                if (deleteFailure != null) {
                    recordCleanupFailure(deleteFailure);
                    LOG.warn("Failed to delete unqueued spill file {}", handle.file, deleteFailure);
                }
            }
        }
    }

    private Throwable closeDelegateOnProducerThread() {
        try {
            delegate.close();
            delegateCloseSucceeded = true;
            return null;
        } catch (Throwable t) {
            delegateCloseFailure = appendFailure(
                    delegateCloseFailure,
                    toIOException("Failed to close delegate reader", t));
            return t;
        }
    }

    private IOException retryDelegateCloseLocked() {
        if (delegateCloseSucceeded) {
            return null;
        }
        try {
            delegate.close();
            delegateCloseSucceeded = true;
            return null;
        } catch (Throwable t) {
            IOException failure = toIOException("Failed to close delegate reader", t);
            delegateCloseFailure = appendFailure(delegateCloseFailure, failure);
            return failure;
        }
    }

    private void requestDelegateCancellation() {
        if (!(delegate instanceof CancellableSplitReader)) {
            return;
        }
        try {
            ((CancellableSplitReader) delegate).cancelRead();
        } catch (Throwable t) {
            cancellationFailure = toIOException(
                    "Failed to request delegate read cancellation", t);
            LOG.debug("Delegate cancellation request failed", t);
        }
    }

    private void closeDelegateAfterConstructionFailure(Throwable failure) {
        addSuppressed(failure, spillWorkspace.cleanup());
        try {
            delegate.close();
        } catch (Throwable closeFailure) {
            addSuppressed(failure, closeFailure);
        }
    }

    private static RuntimeException constructionException(Throwable failure) {
        if (failure instanceof RuntimeException) {
            return (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        return new IllegalStateException(
                "Failed to initialize disk spill reader", failure);
    }

    private static long producerCloseTimeoutMillis(
            SplitReader<VectorSchemaRoot> delegate) {
        if (!(delegate instanceof CancellableSplitReader)) {
            return MIN_PRODUCER_CLOSE_TIMEOUT_MILLIS;
        }
        long transportTimeout =
                ((CancellableSplitReader) delegate).cancellationTimeoutMillis();
        if (transportTimeout <= 0) {
            return MIN_PRODUCER_CLOSE_TIMEOUT_MILLIS;
        }
        if (transportTimeout > Long.MAX_VALUE - CANCELLATION_GRACE_MILLIS) {
            return Long.MAX_VALUE;
        }
        return Math.max(
                MIN_PRODUCER_CLOSE_TIMEOUT_MILLIS,
                transportTimeout + CANCELLATION_GRACE_MILLIS);
    }

    // ---- Spill writer and quota accounting ----

    private final class SpillWriterState {

        private SpillFileHandle handle;
        private FileOutputStream outputStream;
        private QuotaWritableByteChannel channel;
        private VectorSchemaRoot writerRoot;
        private ArrowStreamWriter writer;
        private boolean closedState;
        private boolean cleanupRegistered;

        private SpillWriterState(VectorSchemaRoot sample) throws IOException {
            Throwable failure = null;
            try {
                handle = createSpillFile();
                outputStream = new FileOutputStream(handle.file);
                channel = new QuotaWritableByteChannel(
                        outputStream.getChannel(), handle);
                writerRoot = VectorSchemaRoot.create(
                        sample.getSchema(), allocator(sample));
                writer = new ArrowStreamWriter(writerRoot, null, channel);
                writer.start();
                LOG.debug("Created spill file: {}", handle.file.getName());
            } catch (Throwable t) {
                failure = t;
            }

            if (failure != null) {
                IOException abortFailure = abort();
                addSuppressed(failure, abortFailure);
                throw propagateAsIOException("Failed to create spill writer", failure);
            }
        }

        private void writeBatch(VectorSchemaRoot source) throws IOException {
            for (int i = 0; i < source.getFieldVectors().size(); i++) {
                source.getFieldVectors().get(i)
                        .makeTransferPair(writerRoot.getFieldVectors().get(i))
                        .transfer();
            }
            writerRoot.setRowCount(source.getRowCount());
            writer.writeBatch();
            spillBatchCount.incrementAndGet();
        }

        private long bytesWritten() {
            return handle == null ? 0 : handle.accountedBytes.get();
        }

        private SpillFileHandle finish() throws IOException {
            if (closedState) {
                throw new IOException("Spill writer is already closed");
            }
            closedState = true;

            Throwable failure = null;
            try {
                writer.end();
            } catch (Throwable t) {
                failure = appendThrowable(failure, t);
            }
            failure = appendThrowable(failure, closeWriterResources());

            if (failure != null) {
                IOException deleteFailure = deleteInvalidFileIfReady();
                addSuppressed(failure, deleteFailure);
                IOException reported = toIOException(
                        "Failed to finalize spill file", failure);
                recordSpillWriterFailure(this, reported);
                throw propagateAsIOException("Failed to finalize spill file", failure);
            }

            SpillFileHandle completed = handle;
            handle = null;
            LOG.debug(
                    "Finalized spill file: {} ({} bytes)",
                    completed.file.getName(),
                    completed.accountedBytes.get());
            return completed;
        }

        private IOException abort() {
            if (closedState && cleanupComplete()) {
                return null;
            }
            closedState = true;
            IOException failure = cleanupInvalidFile();
            if (failure != null) {
                recordSpillWriterFailure(this, failure);
            }
            return failure;
        }

        private IOException cleanupInvalidFile() {
            IOException failure = closeWriterResources();
            failure = appendFailure(failure, deleteInvalidFileIfReady());
            if (failure == null && !cleanupComplete()) {
                failure = new IOException(
                        "Spill writer cleanup did not release every owned resource");
            }
            return failure;
        }

        private IOException closeWriterResources() {
            IOException failure = null;
            if (writer != null) {
                try {
                    writer.close();
                    writer = null;
                } catch (Throwable t) {
                    failure = appendFailure(
                            failure, toIOException("Failed to close spill writer", t));
                }
            }
            // ArrowWriter marks start/end before performing their I/O. Closing the underlying
            // stream after writer.close() fails therefore does not prevent a later close retry,
            // and lets us release the file descriptor independently.
            if (outputStream != null) {
                try {
                    outputStream.close();
                    outputStream = null;
                } catch (Throwable t) {
                    failure = appendFailure(
                            failure, toIOException("Failed to close spill output", t));
                }
            }
            if (outputStream == null) {
                channel = null;
            }
            if (writerRoot != null) {
                try {
                    writerRoot.close();
                    writerRoot = null;
                } catch (Throwable t) {
                    failure = appendFailure(
                            failure, toIOException("Failed to close spill writer root", t));
                }
            }
            return failure;
        }

        private IOException deleteInvalidFileIfReady() {
            if (writer == null
                    && outputStream == null
                    && channel == null
                    && writerRoot == null
                    && handle != null) {
                IOException failure = deleteSpillFile(handle);
                if (failure == null) {
                    handle = null;
                }
                return failure;
            }
            return null;
        }

        private boolean cleanupComplete() {
            return writer == null
                    && outputStream == null
                    && channel == null
                    && writerRoot == null
                    && handle == null;
        }

        private void cleanupRegistrationCleared() {
            cleanupRegistered = false;
        }

        private void ensureCleanupRegistered() {
            if (!cleanupRegistered) {
                retainedSpillWriters.add(this);
                cleanupRegistered = true;
            }
        }
    }

    private void recordSpillWriterFailure(
            SpillWriterState state,
            IOException failure) {
        synchronized (cleanupLock) {
            if (!state.cleanupComplete()) {
                state.ensureCleanupRegistered();
            }
        }
        LOG.debug("Spill writer requires cleanup retry", failure);
    }

    private IOException cleanupRetainedSpillWritersLocked() {
        IOException failure = null;
        int index = 0;
        while (index < retainedSpillWriters.size()) {
            SpillWriterState state = retainedSpillWriters.get(index);
            IOException stateFailure = state.cleanupInvalidFile();
            failure = appendFailure(failure, stateFailure);
            if (state.cleanupComplete()) {
                retainedSpillWriters.remove(index);
                state.cleanupRegistrationCleared();
            } else {
                index++;
            }
        }
        return failure;
    }

    private IOException cleanupRetainedArrowRootsLocked() {
        IOException failure = null;
        List<VectorSchemaRoot> roots = new ArrayList<>(retainedArrowRoots);
        for (VectorSchemaRoot root : roots) {
            try {
                root.close();
            } catch (Throwable t) {
                failure = appendFailure(
                        failure,
                        toIOException("Failed to close retained Arrow batch", t));
                continue;
            }
            if (!retainedArrowRoots.remove(root)) {
                failure = appendFailure(
                        failure,
                        new IOException(
                                "Retained Arrow batch queue changed unexpectedly"));
            }
        }
        return failure;
    }

    private final class QuotaWritableByteChannel implements WritableByteChannel {

        private final WritableByteChannel delegateChannel;
        private final SpillFileHandle handle;

        private QuotaWritableByteChannel(
                WritableByteChannel delegateChannel,
                SpillFileHandle handle) {
            this.delegateChannel = delegateChannel;
            this.handle = handle;
        }

        @Override
        public int write(ByteBuffer source) throws IOException {
            int requested = source.remaining();
            if (requested == 0) {
                return 0;
            }

            long total = spillBytesWritten.get();
            if (requested > options.getMaxSpillBytes() - total) {
                throw new IOException(
                        "Cumulative disk spill limit exceeded: requested "
                                + requested
                                + " bytes with "
                                + total
                                + " bytes already written; limit is "
                                + options.getMaxSpillBytes());
            }

            reserveSpillBytes(handle, requested);
            int startPosition = source.position();
            try {
                return delegateChannel.write(source);
            } finally {
                int written = Math.max(0, source.position() - startPosition);
                spillBytesWritten.addAndGet(written);
                int unused = requested - written;
                if (unused > 0) {
                    releaseSpillBytes(handle, unused);
                }
            }
        }

        @Override
        public boolean isOpen() {
            return delegateChannel.isOpen();
        }

        @Override
        public void close() throws IOException {
            delegateChannel.close();
        }
    }

    private SpillFileHandle createSpillFile() throws IOException {
        File file = null;
        SpillFileHandle handle = null;
        boolean registered = false;
        boolean counted = false;
        boolean conflictingRegistration = false;
        Throwable failure = null;
        try {
            file = spillWorkspace.createSpillFile();
            handle = new SpillFileHandle(file);
            SpillFileHandle existing = activeSpillFiles.putIfAbsent(file, handle);
            if (existing != null) {
                conflictingRegistration = true;
                throw new IOException("Duplicate spill file registration: " + file);
            }
            registered = true;
            spillFileCount.incrementAndGet();
            counted = true;
            return handle;
        } catch (Throwable t) {
            failure = t;
        }

        if (!registered
                && handle != null
                && activeSpillFiles.get(file) == handle) {
            registered = true;
        }
        IOException cleanupFailure;
        if (registered && counted) {
            cleanupFailure = deleteSpillFile(handle);
        } else if (conflictingRegistration) {
            // The path is already owned by the existing handle. Deleting or retaining it as an
            // untracked file would corrupt that active spill.
            cleanupFailure = null;
        } else {
            if (registered) {
                activeSpillFiles.remove(file, handle);
            }
            cleanupFailure = deleteUntrackedSpillFile(file);
            if (cleanupFailure != null) {
                retainUntrackedSpillFile(file);
            }
        }
        addSuppressed(failure, cleanupFailure);
        throw propagateAsIOException("Failed to register spill file", failure);
    }

    private IOException deleteUntrackedSpillFile(File file) {
        if (file == null) {
            return null;
        }
        try {
            Files.deleteIfExists(file.toPath());
            return null;
        } catch (IOException | RuntimeException e) {
            return new IOException("Failed to delete untracked spill file: " + file, e);
        }
    }

    private void retainUntrackedSpillFile(File file) {
        synchronized (cleanupLock) {
            if (!retainedUntrackedSpillFiles.contains(file)) {
                retainedUntrackedSpillFiles.add(file);
            }
        }
    }

    private IOException cleanupRetainedUntrackedSpillFilesLocked() {
        IOException failure = null;
        int index = 0;
        while (index < retainedUntrackedSpillFiles.size()) {
            File file = retainedUntrackedSpillFiles.get(index);
            IOException deleteFailure = deleteUntrackedSpillFile(file);
            failure = appendFailure(failure, deleteFailure);
            if (deleteFailure == null) {
                retainedUntrackedSpillFiles.remove(index);
            } else {
                index++;
            }
        }
        return failure;
    }

    private void reserveSpillBytes(
            SpillFileHandle handle,
            long requested) throws IOException {
        while (true) {
            long current = spillBytesInUse.get();
            if (requested > options.getMaxSpillBytesInUse() - current) {
                throw new IOException(
                        "Live disk spill limit exceeded: requested "
                                + requested
                                + " bytes with "
                                + current
                                + " bytes in use; limit is "
                                + options.getMaxSpillBytesInUse());
            }
            long updated = current + requested;
            if (spillBytesInUse.compareAndSet(current, updated)) {
                handle.accountedBytes.addAndGet(requested);
                updatePeakSpillBytes(updated);
                return;
            }
        }
    }

    private void releaseSpillBytes(SpillFileHandle handle, long bytes) {
        if (bytes <= 0) {
            return;
        }
        handle.accountedBytes.addAndGet(-bytes);
        spillBytesInUse.addAndGet(-bytes);
    }

    private void updatePeakSpillBytes(long candidate) {
        long current = peakSpillBytesInUse.get();
        while (candidate > current
                && !peakSpillBytesInUse.compareAndSet(current, candidate)) {
            current = peakSpillBytesInUse.get();
        }
    }

    private IOException deleteSpillFile(SpillFileHandle handle) {
        if (handle == null || handle.released.get()) {
            return null;
        }
        try {
            Files.deleteIfExists(handle.file.toPath());
        } catch (IOException | RuntimeException e) {
            return new IOException("Failed to delete spill file: " + handle.file, e);
        }

        if (handle.released.compareAndSet(false, true)) {
            activeSpillFiles.remove(handle.file, handle);
            long bytes = handle.accountedBytes.getAndSet(0);
            spillBytesInUse.addAndGet(-bytes);
            spillFileCount.decrementAndGet();
        }
        return null;
    }

    // ---- Consumer and cleanup ----

    private boolean openSpillFile(SpillFileHandle handle) throws IOException {
        Throwable failure = null;
        try {
            if (currentSpillReader != null
                    || currentSpillInput != null
                    || currentSpillReaderHandle != null) {
                throw new IOException("Previous spill file has not been fully released");
            }
            LOG.debug("Reading spill file: {}", handle.file.getName());
            currentSpillReaderHandle = handle;
            currentSpillCleanupOnly = false;
            currentSpillInput = new FileInputStream(handle.file);
            currentSpillReader = new ArrowStreamReader(
                    currentSpillInput, allocator(null));

            if (!currentSpillReader.loadNextBatch()) {
                throw new IOException("Spill file contains no batch: " + handle.file);
            }
            currentBatch = copyBatch(
                    currentSpillReader.getVectorSchemaRoot(),
                    consumerAllocator(currentSpillReader.getVectorSchemaRoot()));
            currentBatchHandedOut = false;
            return true;
        } catch (Throwable t) {
            failure = t;
        }

        finished = true;
        currentSpillCleanupOnly = true;
        failure = appendThrowable(failure, closeCurrentSpillReader(true));
        throw propagateAsIOException("Failed to open spill file", failure);
    }

    private void releasePreviousBatch() throws IOException {
        VectorSchemaRoot previous = currentBatch;
        boolean handedOut = currentBatchHandedOut;
        if (previous == null) {
            currentBatchHandedOut = false;
            return;
        }
        if (!reuseBatch && handedOut) {
            currentBatch = null;
            currentBatchHandedOut = false;
            return;
        }

        try {
            previous.close();
            currentBatch = null;
            currentBatchHandedOut = false;
        } catch (RuntimeException e) {
            throw new IOException("Failed to release previous Arrow batch", e);
        }
    }

    private IOException closeCurrentSpillReader(boolean deleteFile) {
        IOException failure = null;
        if (currentSpillReader != null
                || currentSpillInput != null
                || currentSpillReaderHandle != null) {
            currentSpillCleanupOnly = true;
        }

        if (currentSpillReader != null) {
            try {
                currentSpillReader.close();
                currentSpillReader = null;
            } catch (Throwable t) {
                failure = appendFailure(
                        failure, toIOException("Failed to close spill reader", t));
            }
        }

        if (currentSpillInput != null) {
            try {
                currentSpillInput.close();
                currentSpillInput = null;
            } catch (Throwable t) {
                failure = appendFailure(
                        failure, toIOException("Failed to close spill input", t));
            }
        }

        if (deleteFile
                && currentSpillReader == null
                && currentSpillInput == null
                && currentSpillReaderHandle != null) {
            IOException deleteFailure = deleteSpillFile(currentSpillReaderHandle);
            failure = appendFailure(failure, deleteFailure);
            if (deleteFailure == null) {
                currentSpillReaderHandle = null;
            }
        }

        if (currentSpillReader == null
                && currentSpillInput == null
                && currentSpillReaderHandle == null) {
            currentSpillCleanupOnly = false;
        }
        return failure;
    }

    private void cleanupResourcesAfterProducerStop() {
        synchronized (cleanupLock) {
            IOException failure = cleanupResourcesLocked();
            if (failure != null) {
                LOG.warn("Failed to fully clean disk spill resources", failure);
            }
        }
    }

    private IOException cleanupResourcesLocked() {
        IOException attemptFailure = retryDelegateCloseLocked();
        IOException localFailure = cleanupRetainedSpillWritersLocked();
        localFailure = appendFailure(
                localFailure, cleanupRetainedUntrackedSpillFilesLocked());
        consumerLock.lock();
        try {
            localFailure = appendFailure(
                    localFailure, cleanupRetainedArrowRootsLocked());
            localFailure = appendFailure(
                    localFailure, closeCurrentSpillReader(true));
            localFailure = appendFailure(
                    localFailure, cleanupCurrentBatch());
            localFailure = appendFailure(
                    localFailure, cleanupOrderedQueue());

            if (currentSpillReader == null
                    && currentSpillInput == null
                    && currentSpillReaderHandle == null
                    && retainedArrowRoots.isEmpty()
                    && retainedSpillWriters.isEmpty()
                    && retainedUntrackedSpillFiles.isEmpty()
                    && orderedQueue.isEmpty()) {
                List<SpillFileHandle> remainingFiles =
                        new ArrayList<>(activeSpillFiles.values());
                for (SpillFileHandle handle : remainingFiles) {
                    localFailure = appendFailure(
                            localFailure, deleteSpillFile(handle));
                }
            }

            if (!allocatorCloseSucceeded
                    && allocator != null
                    && spillResourcesReadyForAllocatorCleanup()) {
                try {
                    allocator.close();
                    allocatorCloseSucceeded = true;
                } catch (Throwable t) {
                    localFailure = appendFailure(
                            localFailure,
                            toIOException("Failed to close spill allocator", t));
                }
            }
            if (resourcesReadyForWorkspaceCleanup()) {
                localFailure = appendFailure(
                        localFailure, spillWorkspace.cleanup());
            }
        } finally {
            consumerLock.unlock();
        }

        if (localFailure == null && !cleanupComplete()) {
            localFailure = new IOException(
                    "Disk spill cleanup did not release every owned resource");
        }
        if (localFailure != null) {
            cleanupFailure = appendFailure(cleanupFailure, localFailure);
        }
        if (attemptFailure == null) {
            return localFailure;
        }
        if (localFailure == null) {
            return attemptFailure;
        }
        IOException combined = new IOException("Failed to fully clean disk spill reader");
        addSuppressed(combined, attemptFailure);
        addSuppressed(combined, localFailure);
        return combined;
    }

    private IOException cleanupCurrentBatch() {
        VectorSchemaRoot batch = currentBatch;
        if (batch == null) {
            currentBatchHandedOut = false;
            return null;
        }
        if (!reuseBatch && currentBatchHandedOut) {
            currentBatch = null;
            currentBatchHandedOut = false;
            return null;
        }

        try {
            batch.close();
            currentBatch = null;
            currentBatchHandedOut = false;
            return null;
        } catch (Throwable t) {
            return toIOException("Failed to close current Arrow batch", t);
        }
    }

    private IOException cleanupOrderedQueue() {
        IOException failure = null;
        List<BufferedBatch> items = new ArrayList<>(orderedQueue);
        for (BufferedBatch item : items) {
            IOException itemFailure = null;
            if (item.type == BufferedBatch.Type.IN_MEMORY) {
                try {
                    item.root.close();
                } catch (Throwable t) {
                    itemFailure = toIOException(
                            "Failed to close queued Arrow batch", t);
                }
            } else if (item.type == BufferedBatch.Type.ON_DISK) {
                itemFailure = deleteSpillFile(item.spillFile);
            }
            if (itemFailure != null) {
                failure = appendFailure(failure, itemFailure);
                continue;
            }

            if (!orderedQueue.remove(item)) {
                failure = appendFailure(
                        failure,
                        new IOException(
                                "Disk spill cleanup queue changed unexpectedly"));
                continue;
            }
            if (item.type == BufferedBatch.Type.IN_MEMORY) {
                memorySlots.release();
                releaseMemoryBytes(item.memoryBytes);
            }
        }
        return failure;
    }

    private void recordCleanupFailure(IOException failure) {
        synchronized (cleanupLock) {
            cleanupFailure = appendFailure(cleanupFailure, failure);
        }
    }

    private boolean cleanupComplete() {
        return currentSpillReader == null
                && currentSpillInput == null
                && currentSpillReaderHandle == null
                && !currentSpillCleanupOnly
                && currentBatch == null
                && retainedArrowRoots.isEmpty()
                && retainedSpillWriters.isEmpty()
                && retainedUntrackedSpillFiles.isEmpty()
                && orderedQueue.isEmpty()
                && activeSpillFiles.isEmpty()
                && spillBytesInUse.get() == 0
                && spillFileCount.get() == 0
                && memoryBytesInUse.get() == 0
                && (allocator == null || allocatorCloseSucceeded)
                && spillWorkspace.isDeleted();
    }

    private boolean spillResourcesReadyForAllocatorCleanup() {
        return currentSpillReader == null
                && currentSpillInput == null
                && currentSpillReaderHandle == null
                && retainedArrowRoots.isEmpty()
                && retainedSpillWriters.isEmpty()
                && retainedUntrackedSpillFiles.isEmpty()
                && orderedQueue.isEmpty()
                && activeSpillFiles.isEmpty()
                && spillBytesInUse.get() == 0
                && spillFileCount.get() == 0
                && memoryBytesInUse.get() == 0;
    }

    private boolean resourcesReadyForWorkspaceCleanup() {
        return spillResourcesReadyForAllocatorCleanup()
                && (allocator == null || allocatorCloseSucceeded);
    }

    // ---- Allocator and memory accounting ----

    private BufferAllocator allocator(VectorSchemaRoot sample) {
        BufferAllocator local = allocator;
        if (local != null) {
            return local;
        }
        synchronized (allocatorLock) {
            if (allocator == null) {
                allocator = parentAllocatorFor(sample)
                        .newChildAllocator("disk-spill-buffer", 0, Long.MAX_VALUE);
            }
            return allocator;
        }
    }

    private BufferAllocator consumerAllocator(VectorSchemaRoot sample) {
        BufferAllocator local = consumerAllocator;
        if (local != null) {
            return local;
        }
        synchronized (allocatorLock) {
            if (consumerAllocator == null) {
                consumerAllocator = parentAllocatorFor(sample);
            }
            return consumerAllocator;
        }
    }

    private BufferAllocator parentAllocatorFor(VectorSchemaRoot sample) {
        if (sample != null) {
            for (FieldVector vector : sample.getFieldVectors()) {
                if (vector != null && vector.getAllocator() != null) {
                    return vector.getAllocator();
                }
            }
        }
        return options.getAllocator() != null
                ? options.getAllocator()
                : ArrowUtils.getDefaultRootAllocator();
    }

    private boolean tryReserveMemoryBytes(long bytes) {
        while (true) {
            long current = memoryBytesInUse.get();
            if (bytes > options.getMaxMemoryBufferBytes() - current) {
                return false;
            }
            if (memoryBytesInUse.compareAndSet(current, current + bytes)) {
                return true;
            }
        }
    }

    private void releaseMemoryBytes(long bytes) {
        if (bytes > 0) {
            memoryBytesInUse.addAndGet(-bytes);
        }
    }

    private static long estimateBufferBytes(VectorSchemaRoot root) {
        long bytes = 0;
        for (FieldVector vector : root.getFieldVectors()) {
            long vectorBytes = vector.getBufferSize();
            if (Long.MAX_VALUE - bytes < vectorBytes) {
                return Long.MAX_VALUE;
            }
            bytes += vectorBytes;
        }
        return bytes;
    }

    private VectorSchemaRoot copyBatch(
            VectorSchemaRoot source,
            BufferAllocator targetAllocator) {
        VectorSchemaRoot target = VectorSchemaRoot.create(
                source.getSchema(), targetAllocator);
        try {
            for (int i = 0; i < source.getFieldVectors().size(); i++) {
                source.getFieldVectors().get(i)
                        .makeTransferPair(target.getFieldVectors().get(i))
                        .transfer();
            }
            target.setRowCount(source.getRowCount());
            return target;
        } catch (RuntimeException | Error e) {
            try {
                target.close();
            } catch (RuntimeException | Error closeError) {
                retainedArrowRoots.add(target);
                addSuppressed(e, closeError);
            }
            throw e;
        }
    }

    // ---- Metrics and failure helpers ----

    private void registerMetrics() {
        metrics.register(new ReadOnlyCounterGauge(
                MetricNames.DISK_SPILL_BATCH_COUNT, spillBatchCount));
        metrics.register(new ReadOnlyCounterGauge(
                MetricNames.DISK_SPILL_BYTES_WRITTEN, spillBytesWritten));
        metrics.register(new ReadOnlyGauge(
                MetricNames.DISK_SPILL_BYTES_IN_USE, spillBytesInUse));
        metrics.register(new ReadOnlyGauge(
                MetricNames.DISK_SPILL_PEAK_BYTES_IN_USE, peakSpillBytesInUse));
        metrics.register(new ReadOnlyGauge(
                MetricNames.DISK_SPILL_FILE_COUNT, spillFileCount));
        metrics.register(new ReadOnlyGauge(
                MetricNames.DISK_SPILL_PRODUCER_FINISHED, producerCompleted));
    }

    private static IOException appendFailure(
            IOException current,
            IOException next) {
        if (next == null) {
            return current;
        }
        if (current == null) {
            return next;
        }
        addSuppressed(current, next);
        return current;
    }

    private static Throwable appendThrowable(Throwable current, Throwable next) {
        if (next == null) {
            return current;
        }
        if (current == null) {
            return next;
        }
        addSuppressed(current, next);
        return current;
    }

    private static void addSuppressed(Throwable primary, Throwable secondary) {
        if (primary != null && secondary != null && primary != secondary) {
            primary.addSuppressed(secondary);
        }
    }

    private static IOException toIOException(String message, Throwable failure) {
        if (failure instanceof IOException) {
            return (IOException) failure;
        }
        return new IOException(message, failure);
    }

    private static IOException propagateAsIOException(
            String message,
            Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        return toIOException(message, failure);
    }

    private static class ReadOnlyGauge implements Gauge<Long> {

        private final String name;
        private final AtomicLong value;

        private ReadOnlyGauge(String name, AtomicLong value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void setValue(Long newValue) {
            throw new UnsupportedOperationException("Metric is read-only: " + name);
        }

        @Override
        public Long getValue() {
            return value.get();
        }
    }

    private static final class ReadOnlyCounterGauge
            extends ReadOnlyGauge implements Counter {

        private ReadOnlyCounterGauge(String name, AtomicLong value) {
            super(name, value);
        }

        @Override
        public void inc() {
            throw readOnly();
        }

        @Override
        public void inc(long n) {
            throw readOnly();
        }

        @Override
        public void dec() {
            throw readOnly();
        }

        @Override
        public void dec(long n) {
            throw readOnly();
        }

        @Override
        public long getCount() {
            return getValue();
        }

        private UnsupportedOperationException readOnly() {
            return new UnsupportedOperationException("Metric is read-only: " + name());
        }
    }

    private static final class SpillFileHandle {

        private final File file;
        private final AtomicLong accountedBytes = new AtomicLong(0);
        private final AtomicBoolean released = new AtomicBoolean(false);

        private SpillFileHandle(File file) {
            this.file = file;
        }
    }

    static final class BufferedBatch {

        enum Type {
            IN_MEMORY,
            ON_DISK,
            DONE,
            ERROR,
            CLOSED
        }

        private final Type type;
        private final VectorSchemaRoot root;
        private final SpillFileHandle spillFile;
        private final Throwable error;
        private final long memoryBytes;

        private BufferedBatch(
                Type type,
                VectorSchemaRoot root,
                SpillFileHandle spillFile,
                Throwable error,
                long memoryBytes) {
            this.type = type;
            this.root = root;
            this.spillFile = spillFile;
            this.error = error;
            this.memoryBytes = memoryBytes;
        }

        static BufferedBatch inMemory(VectorSchemaRoot root, long memoryBytes) {
            return new BufferedBatch(
                    Type.IN_MEMORY, root, null, null, memoryBytes);
        }

        static BufferedBatch onDisk(SpillFileHandle file) {
            return new BufferedBatch(Type.ON_DISK, null, file, null, 0);
        }

        static BufferedBatch done() {
            return new BufferedBatch(Type.DONE, null, null, null, 0);
        }

        static BufferedBatch error(Throwable error) {
            return new BufferedBatch(Type.ERROR, null, null, error, 0);
        }

        static BufferedBatch closed() {
            return new BufferedBatch(Type.CLOSED, null, null, null, 0);
        }
    }
}
