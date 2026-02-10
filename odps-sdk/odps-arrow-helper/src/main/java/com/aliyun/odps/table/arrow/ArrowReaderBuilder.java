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

package com.aliyun.odps.table.arrow;

import com.aliyun.odps.table.arrow.readers.ArrowBatchNonReusedReader;
import com.aliyun.odps.table.arrow.readers.ArrowBatchReusedReader;
import com.aliyun.odps.table.configuration.CompressionCodec;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.NoCompressionCodec;

import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * A factory used to create {@link ArrowReader} instances.
 */
public class ArrowReaderBuilder {

    public static ArrowReaderBuilder newBuilder(InputStream is,
                                                BufferAllocator allocator) {
        return new ArrowReaderBuilder(is, allocator);
    }

    // Required parameters
    private final InputStream inputStream;
    private final BufferAllocator allocator;

    // Optional parameters with default values
    private boolean reuseBatch = false;
    private org.apache.arrow.vector.compression.CompressionCodec.Factory compressionFactory = CommonsCompressionFactory.INSTANCE;
    private boolean isAsync = false;
    private BlockingQueue<Object> asyncQueue = null;

    /**
     * Private constructor to be called by the factory.
     *
     * @param inputStream The input stream to read from (required).
     * @param allocator   The Arrow buffer allocator (required).
     */
    private ArrowReaderBuilder(InputStream inputStream, BufferAllocator allocator) {
        this.inputStream = Objects.requireNonNull(inputStream, "InputStream cannot be null.");
        this.allocator = Objects.requireNonNull(allocator, "BufferAllocator cannot be null.");
    }

    public ArrowReaderBuilder withReuseBatch(boolean reuseBatch) {
        this.reuseBatch = reuseBatch;
        return this;
    }

    public ArrowReaderBuilder withCompression(CompressionCodec compression) {
        this.compressionFactory =
          compression.equals(CompressionCodec.NO_COMPRESSION) ?
          NoCompressionCodec.Factory.INSTANCE : CommonsCompressionFactory.INSTANCE;
        return this;
    }


    public ArrowReaderBuilder withAsync(boolean isAsync) {
        this.isAsync = isAsync;
        return this;
    }

    /**
     * Enables asynchronous reading mode with a specific queue.
     *
     * @param asyncQueue The queue to use for holding asynchronously read batches.
     * @return this builder instance for chaining.
     */
    public ArrowReaderBuilder withAsyncQueue(BlockingQueue<Object> asyncQueue) {
        this.asyncQueue = asyncQueue;
        return this;
    }

    /**
     * Constructs the final {@link ArrowReader} instance based on the configured options.
     *
     * @return An instance of {@link ArrowReader}.
     * @throws IllegalStateException if async mode is enabled without a queue (though the builder tries to prevent this).
     */
    public ArrowReader build() {
        if (reuseBatch) {
            return new ArrowBatchReusedReader(inputStream, allocator, compressionFactory, isAsync, asyncQueue);
        } else {
            return new ArrowBatchNonReusedReader(inputStream, allocator, compressionFactory);
        }
    }


}
