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

package com.aliyun.odps.table.arrow.readers;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.aliyun.odps.table.arrow.ArrowReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.types.pojo.Schema;

/** Reads tagged Arrow substreams, keeping metrics frames separate from table rows. */
public final class ExtendedArrowReader implements ArrowReader {
    private final InputStream input;
    private final BufferAllocator allocator;
    private final CompressionCodec.Factory compressionFactory;
    private final boolean async;
    private final boolean reuseBatch;
    private final BlockingQueue<Object> asyncQueue;
    private final Map<String, String> metricsMetadata = new HashMap<>();
    private ArrowReader reader;
    private Schema schema;
    private int streamTag;
    private long completedBytes;
    private boolean exhausted;
    private boolean closed;

    public ExtendedArrowReader(InputStream input, BufferAllocator allocator,
                               CompressionCodec.Factory compressionFactory,
                               boolean async, BlockingQueue<Object> asyncQueue) {
        this(input, allocator, compressionFactory, async, asyncQueue, true);
    }

    public ExtendedArrowReader(InputStream input, BufferAllocator allocator,
                               CompressionCodec.Factory compressionFactory,
                               boolean async, BlockingQueue<Object> asyncQueue,
                               boolean reuseBatch) {
        this.reuseBatch = reuseBatch;
        this.input = input;
        this.allocator = allocator;
        this.compressionFactory = compressionFactory;
        this.async = async;
        this.asyncQueue = asyncQueue;
    }

    @Override
    public boolean nextBatch() throws IOException {
        if (closed || exhausted) {
            return false;
        }
        while (true) {
            if (reader == null) {
                streamTag = input.read();
                if (streamTag == -1) {
                    exhausted = true;
                    return false;
                }
                completedBytes++;
                // Read the tag before starting an asynchronous Arrow reader.
                // Metrics batches stay reader-owned even when data batches transfer ownership.
                reader = reuseBatch || streamTag == 2
                        ? new ArrowBatchReusedReader(input, allocator, compressionFactory,
                                async, asyncQueue)
                        : new ArrowBatchNonReusedReader(input, allocator, compressionFactory);
            }
            boolean hasBatch = reader.nextBatch();
            if (streamTag == 2) {
                Schema metricsSchema = reader.getSchema();
                if (!hasBatch && metricsSchema != null) {
                    metricsSchema.getCustomMetadata().forEach((key, value) -> {
                        if (key.equals("server_processing_duration_us")
                                || key.equals("rate_limit_duration_us")) {
                            metricsMetadata.merge(key, value, (previous, current) ->
                                    Long.toString(Long.parseLong(previous) + Long.parseLong(current)));
                        } else {
                            metricsMetadata.put(key, value);
                        }
                    });
                }
                // A metrics stream must never become a table batch.
            } else if (hasBatch) {
                schema = reader.getSchema();
                return true;
            }
            if (!hasBatch) {
                completedBytes += reader.bytesRead();
                reader.close(false);
                reader = null;
            }
        }
    }

    @Override
    public VectorSchemaRoot getCurrentValue() {
        return reader == null || streamTag == 2 ? null : reader.getCurrentValue();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    public Map<String, String> getMetricsMetadata() {
        return Collections.unmodifiableMap(metricsMetadata);
    }

    @Override
    public long bytesRead() {
        return completedBytes + (reader == null ? 0 : reader.bytesRead());
    }

    @Override
    public void close() throws IOException {
        close(true);
    }

    @Override
    public void close(boolean closeReadSource) throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (closeReadSource) {
                // Closing the source first unblocks an asynchronous network read.
                input.close();
            }
        } finally {
            if (reader != null) {
                completedBytes += reader.bytesRead();
                reader.close(false);
                reader = null;
            }
        }
    }
}
