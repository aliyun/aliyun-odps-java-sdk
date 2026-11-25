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

import com.aliyun.odps.table.impl.v2.V2ClientHolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.arrow.compression.OdpsCompressionFactory;
import com.aliyun.odps.table.arrow.readers.ArrowBatchReusedReader;
import com.aliyun.odps.table.arrow.readers.ArrowBatchNonReusedReader;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RateLimitCost;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.metrics.count.ServerProcessCost;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.split.InputSplit;
import org.apache.arrow.vector.VectorSchemaRoot;
import com.aliyun.odps.table.arrow.readers.ExtendedArrowReader;

/**
 * Arrow split reader over the Storage API v2 data plane.
 *
 * <p>The stream comes from {@code StorageStub.createTableReadStream}; Arrow IPC buffers are
 * decoded here, preferring native LZ4F when available. When the session negotiated extended IPC, the
 * stream is framed as {@code [tag][arrow-stream]} segments and the trailing metrics frame is
 * consumed here, matching the v1 reader's observable behavior.
 */
class SplitArrowReaderV2Impl implements SplitReader<VectorSchemaRoot> {

    private final ReaderOptions readerOptions;
    private final org.apache.arrow.vector.compression.CompressionCodec.Factory compressionFactory;

    private final InputStream in;

    private final V2ClientHolder ownedClient;

    private final boolean extendedArrowIPCEnabled;

    private ArrowReader reader;

    private Metrics metrics;

    private BytesCount bytesCount;

    private RecordCount recordCount;

    private RateLimitCost rateLimitCost;

    private ServerProcessCost serverProcessCost;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    SplitArrowReaderV2Impl(V2ClientHolder holder, TableIdentifier identifier, InputSplit split,
                           ReaderOptions options, boolean extendedArrowIPCEnabled)
            throws IOException {
        this.readerOptions = options;
        this.compressionFactory = options.getCompressionCodec() == CompressionCodec.LZ4_FRAME_NATIVE
                ? OdpsCompressionFactory.NATIVE_LZ4_INSTANCE : OdpsCompressionFactory.V2_READ_INSTANCE;
        this.extendedArrowIPCEnabled = extendedArrowIPCEnabled;
        V2ClientHolder dataClient = holder;
        this.ownedClient = holder;
        InputStream input = null;
        try {
            input = V2NormalSession.openStream(dataClient, identifier, split, options);
            this.reader = extendedArrowIPCEnabled
                    ? new ExtendedArrowReader(input, options.getBufferAllocator(),
                            compressionFactory, options.isAsync(), options.getAsyncQueue(),
                            options.isReuseBatch())
                    : newReader(input);
            this.in = input;
        } catch (IOException | RuntimeException | Error e) {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            if (ownedClient != null) {
                ownedClient.close();
            }
            throw e;
        }
        initMetrics();
    }

    private ArrowReader newReader(InputStream input) {
        if (!readerOptions.isReuseBatch()) {
            return new ArrowBatchNonReusedReader(input, readerOptions.getBufferAllocator(),
                    compressionFactory);
        }
        return new ArrowBatchReusedReader(input, readerOptions.getBufferAllocator(),
                compressionFactory, readerOptions.isAsync(),
                readerOptions.getAsyncQueue());
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

    @Override
    public boolean hasNext() throws IOException {
        boolean hasBatch = reader.nextBatch();
        if (extendedArrowIPCEnabled) {
            loadServerMetrics();
        }
        bytesCount.setValue(reader.bytesRead());
        return hasBatch;
    }

    @Override
    public VectorSchemaRoot get() {
        VectorSchemaRoot root = reader.getCurrentValue();
        recordCount.inc(root.getRowCount());
        bytesCount.setValue(reader.bytesRead());
        return root;
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            reader.close();
        } finally {
            try {
                in.close();
            } finally {
                if (ownedClient != null) {
                    ownedClient.close();
                }
            }
        }
    }

    @Override
    public Metrics currentMetricsValues() {
        return this.metrics;
    }

    private void loadServerMetrics() {
        ((ExtendedArrowReader) reader).getMetricsMetadata().forEach((key, value) -> {
            if (key.equals("server_processing_duration_us")) {
                serverProcessCost.inc(Long.parseLong(value) - serverProcessCost.getCount());
            } else if (key.equals("rate_limit_duration_us")) {
                rateLimitCost.inc(Long.parseLong(value) - rateLimitCost.getCount());
            }
        });
    }
}
