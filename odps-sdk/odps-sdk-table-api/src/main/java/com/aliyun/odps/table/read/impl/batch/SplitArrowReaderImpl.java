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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.arrow.ArrowReaderBuilder;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.metrics.count.RateLimitCost;
import com.aliyun.odps.table.metrics.count.ServerProcessCost;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitWithIndex;
import com.aliyun.odps.table.read.split.InputSplitWithRowRange;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.table.utils.HttpUtils;
import com.aliyun.odps.table.utils.TableRetryHandler;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitArrowReaderImpl
        implements SplitReader<VectorSchemaRoot>, CancellableSplitReader {

    private static final Logger logger =
            LoggerFactory.getLogger(SplitArrowReaderImpl.class.getName());

    private final ReaderOptions readerOptions;
    private final InputStream in;

    private ArrowReader reader;
    private Connection connection;
    private volatile boolean isClosed;
    private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
    private final Object closeLock = new Object();
    private volatile boolean readerClosed;
    private volatile boolean transportDisconnected;
    private volatile long cancellationTimeoutMillis =
            TimeUnit.SECONDS.toMillis(RestClient.DEFAULT_READ_TIMEOUT);
    private Metrics metrics;
    private BytesCount bytesCount;
    private RecordCount recordCount;
    private RateLimitCost rateLimitCost;
    private ServerProcessCost serverProcessCost;

    private String requestId;
    private long bytesRead;
    private boolean extendedArrowIPCEnabled;
    private int streamTag;

    public SplitArrowReaderImpl(TableIdentifier identifier,
                                InputSplit split,
                                ReaderOptions options) throws IOException {
        this.readerOptions = options;
        InputStream input = null;
        ArrowReader arrowReader = null;
        Throwable failure = null;
        try {
            openReaderConnection(identifier, split, options);
            initMetrics();
            input = new CancellationAwareInputStream(
                    connection.getInputStream(), cancelRequested);
            arrowReader = ArrowReaderBuilder.newBuilder(
                            input, options.getBufferAllocator())
                    .withReuseBatch(options.isReuseBatch())
                    .withCompression(options.getCompressionCodec())
                    .withAsync(options.isAsync())
                    .withAsyncQueue(options.getAsyncQueue())
                    .build();
        } catch (Throwable t) {
            failure = t;
        }

        if (failure != null) {
            Throwable cleanupFailure =
                    cleanupFailedConstruction(arrowReader, input);
            failure = appendFailure(failure, cleanupFailure);
            throwConstructionFailure(failure);
        }

        this.in = input;
        this.reader = arrowReader;
        this.isClosed = false;
        this.bytesRead = 0;
        this.streamTag = -1;
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            if (!extendedArrowIPCEnabled) {
                return reader.nextBatch();
            }

            while (true) {
                if (streamTag == -1) {
                    streamTag = in.read();
                    if (streamTag == -1) {
                        return false;
                    }
                }

                if (!reader.nextBatch()) {
                    if (streamTag == 2) {
                        loadServerMetrics();
                    }
                    bytesRead += reader.bytesRead();
                    reader.close(false);
                    if (cancelRequested.get()) {
                        return false;
                    }
                    reader = ArrowReaderBuilder.newBuilder(in,
                                    readerOptions.getBufferAllocator())
                            .withReuseBatch(readerOptions.isReuseBatch())
                            .withCompression(readerOptions.getCompressionCodec())
                            .withAsync(readerOptions.isAsync())
                            .withAsyncQueue(readerOptions.getAsyncQueue())
                            .build();
                    streamTag = -1;
                    continue;
                }
                return true;
            }
        } catch (IOException e) {
            if (cancelRequested.get()) {
                logger.debug(
                        "Arrow split read cancelled, requestId=" + requestId, e);
            } else {
                logger.error(
                        "Get next record batch failed, requestId=" + requestId, e);
            }
            throw e;
        }
    }

    @Override
    public VectorSchemaRoot get() {
        VectorSchemaRoot root = reader.getCurrentValue();
        recordCount.inc(root.getRowCount());
        bytesCount.setValue(bytesRead + reader.bytesRead());
        return root;
    }

    @Override
    public void close() throws IOException {
        synchronized (closeLock) {
            if (isClosed) {
                return;
            }

            Throwable failure = null;
            if (!readerClosed) {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                    readerClosed = true;
                } catch (Throwable t) {
                    failure = appendFailure(failure, t);
                }
            }

            try {
                disconnectTransport();
            } catch (Throwable t) {
                failure = appendFailure(failure, t);
            }
            isClosed = readerClosed && transportDisconnected;
            throwCloseFailure(failure);
        }
    }

    @Override
    public void cancelRead() throws IOException {
        if (isClosed) {
            return;
        }
        cancelRequested.set(true);
    }

    @Override
    public long cancellationTimeoutMillis() {
        return cancellationTimeoutMillis;
    }

    @Override
    public Metrics currentMetricsValues() {
        return this.metrics;
    }

    private void initMetrics() {
        this.bytesCount = new BytesCount();
        this.recordCount = new RecordCount();
        this.metrics = new Metrics();
        metrics.register(bytesCount);
        metrics.register(recordCount);

        if (extendedArrowIPCEnabled) {
            this.rateLimitCost = new RateLimitCost();
            this.serverProcessCost = new ServerProcessCost();
            metrics.register(rateLimitCost);
            metrics.register(serverProcessCost);
        }
    }

    private void openReaderConnection(TableIdentifier identifier,
                                      InputSplit split,
                                      ReaderOptions options) throws IOException {
        RestClient restClient = ExecutionEnvironment.create(options.getSettings())
                .createHttpClient(identifier.getProject());
        int readTimeoutSeconds = restClient.getReadTimeout();
        if (options.getDiskSpillBufferOptions() != null && readTimeoutSeconds <= 0) {
            throw new IllegalArgumentException(
                    "Disk spill buffering requires a finite positive HTTP read timeout");
        }
        cancellationTimeoutMillis = TimeUnit.SECONDS.toMillis(readTimeoutSeconds);
        restClient.setRetryLogger(new RestClient.RetryLogger() {
            @Override
            public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
                logger.warn(String.format("Reader retry for session: %s, " +
                                "retryCount: %d, will retry in %d seconds.",
                        split.getSessionId(), retryCount, retrySleepTime / 1000), e);
            }
        });

        TunnelRetryHandler retryHandler = new TableRetryHandler(restClient);

        Map<String, String> headers = HttpUtils.createCommonHeader(options.getSettings());
        if (options.getCompressionCodec().equals(CompressionCodec.ZSTD)) {
            headers.put(Headers.ACCEPT_ENCODING, CompressionCodec.ZSTD.toString());
        } else if (options.getCompressionCodec().equals(CompressionCodec.LZ4_FRAME)) {
            headers.put(Headers.ACCEPT_ENCODING, CompressionCodec.LZ4_FRAME.toString());
        }

        Map<String, String> params = HttpUtils.createCommonParams(options.getSettings());
        params.put(ConfigConstants.SESSION_ID, split.getSessionId());

        if (split instanceof InputSplitWithRowRange) {
            InputSplitWithRowRange rowRangeInputSplit = (InputSplitWithRowRange) split;
            params.put(ConfigConstants.ROW_INDEX,
                    String.valueOf(rowRangeInputSplit.getRowRange().getStartIndex()));
            params.put(ConfigConstants.ROW_COUNT,
                    String.valueOf(rowRangeInputSplit.getRowRange().getNumRecord()));
        } else if (split instanceof InputSplitWithIndex) {
            InputSplitWithIndex indexedInputSplit = (InputSplitWithIndex) split;
            params.put(ConfigConstants.SPLIT_INDEX,
                    String.valueOf(indexedInputSplit.getSplitIndex()));
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }

        params.put(ConfigConstants.MAX_BATCH_ROWS,
                String.valueOf(options.getBatchRowCount()));
        if (options.getBatchRawSize() != 0L) {
            params.put(
                    ConfigConstants.MAX_BATCH_RAW_SIZE,
                    String.valueOf(options.getBatchRawSize()));
        }
        params.put(ConfigConstants.DATA_FORMAT_TYPE,
                options.getDataFormat().getType().toString());
        params.put(ConfigConstants.DATA_FORMAT_VERSION,
                options.getDataFormat().getVersion().toString());
        try {
            String resource = ResourceBuilder.buildTableDataResource(
                    ConfigConstants.VERSION_1,
                    identifier.getProject(),
                    identifier.getSchema(),
                    identifier.getTable());

            final Exception[] pendingDisconnectPrimary = new Exception[1];
            retryHandler.executeWithRetry(ctx -> {
                Exception pendingFailure = pendingDisconnectPrimary[0];
                if (pendingFailure != null) {
                    disconnectAfterFailure(pendingFailure);
                    if (!transportDisconnected) {
                        throw pendingFailure;
                    }
                    pendingDisconnectPrimary[0] = null;
                }

                try {
                    Map<String, String> requestHeaders = new HashMap<>(headers);
                    ctx.injectHeaders(requestHeaders);
                    this.connection = restClient.connect(
                            resource, "GET", params, requestHeaders);
                    this.transportDisconnected = false;
                    Response resp = connection.getResponse();
                    this.requestId = resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID);
                    this.extendedArrowIPCEnabled = "true".equals(
                            resp.getHeader(HttpHeaders.HEADER_EXTENDED_ARROW_IPC_ENABLED));

                    if (!resp.isOK()) {
                        throw new TunnelException(requestId, connection.getInputStream(),
                                resp.getStatus());
                    }
                } catch (Exception e) {
                    disconnectAfterFailure(e);
                    if (connection != null && !transportDisconnected) {
                        pendingDisconnectPrimary[0] = e;
                    }
                    throw e;
                }
                return null;
            });
        } catch (Exception e) {
            disconnectAfterFailure(e);
            logger.error("Open split reader failed", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    private void disconnectTransport() throws IOException {
        if (!transportDisconnected && connection != null) {
            connection.disconnect();
            transportDisconnected = true;
        }
    }

    private Throwable cleanupFailedConstruction(
            ArrowReader arrowReader,
            InputStream input) {
        Throwable failure = null;
        if (arrowReader != null) {
            try {
                arrowReader.close();
            } catch (Throwable t) {
                failure = appendFailure(failure, t);
            }
        }
        if (input != null) {
            try {
                input.close();
            } catch (Throwable t) {
                failure = appendFailure(failure, t);
            }
        }
        try {
            disconnectTransport();
        } catch (Throwable t) {
            failure = appendFailure(failure, t);
        }
        return failure;
    }

    private void disconnectAfterFailure(Throwable primary) {
        try {
            disconnectTransport();
        } catch (Throwable disconnectFailure) {
            appendFailure(primary, disconnectFailure);
        }
    }

    private static Throwable appendFailure(
            Throwable primary,
            Throwable secondary) {
        if (secondary == null) {
            return primary;
        }
        if (primary == null) {
            return secondary;
        }
        if (primary != secondary) {
            try {
                primary.addSuppressed(secondary);
            } catch (Throwable ignored) {
                // Preserve the primary failure even if suppression itself cannot be recorded.
            }
        }
        return primary;
    }

    private static void throwConstructionFailure(Throwable failure)
            throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("Failed to initialize Arrow split reader", failure);
    }

    private static void throwCloseFailure(Throwable failure)
            throws IOException {
        if (failure == null) {
            return;
        }
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("Failed to close Arrow split reader", failure);
    }

    private void loadServerMetrics() {
        Schema schema = reader.getSchema();
        if (schema != null) {
            schema.getCustomMetadata().forEach((key, value) -> {
                if (key.equals("server_processing_duration_us")) {
                    serverProcessCost.inc(Long.parseLong(value));
                } else if (key.equals("rate_limit_duration_us")) {
                    rateLimitCost.inc(Long.parseLong(value));
                }
            });
        }
    }

    /**
     * Makes cancellation observable even when the server keeps trickling bytes. A blocked
     * underlying read is still bounded by the positive RestClient read timeout required for disk
     * spill mode.
     */
    private static final class CancellationAwareInputStream extends FilterInputStream {

        private final AtomicBoolean cancelled;

        private CancellationAwareInputStream(
                InputStream input,
                AtomicBoolean cancelled) {
            super(input);
            this.cancelled = cancelled;
        }

        @Override
        public int read() throws IOException {
            checkCancelled();
            int value = super.read();
            checkCancelled();
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            checkCancelled();
            int count = super.read(buffer, offset, length);
            checkCancelled();
            return count;
        }

        @Override
        public long skip(long count) throws IOException {
            checkCancelled();
            long skipped = super.skip(count);
            checkCancelled();
            return skipped;
        }

        private void checkCancelled() throws IOException {
            if (cancelled.get()) {
                throw new IOException("Arrow split read cancelled");
            }
        }
    }
}
