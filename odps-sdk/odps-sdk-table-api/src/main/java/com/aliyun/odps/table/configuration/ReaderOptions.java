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

package com.aliyun.odps.table.configuration;

import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.utils.ArrowUtils;
import com.aliyun.odps.table.utils.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.concurrent.BlockingQueue;

import static com.aliyun.odps.table.utils.ConfigConstants.DEFAULT_BUFFERED_ROW_COUNT;

public class ReaderOptions {

    private int batchRowCount;
    private long batchRawSize;
    private BufferAllocator bufferAllocator;
    private boolean reuseBatch;
    private boolean async;
    private BlockingQueue<Object> asyncQueue;
    private EnvironmentSettings settings;
    private CompressionCodec compressionCodec;
    private DataFormat dataFormat;
    private DiskSpillBufferOptions diskSpillBufferOptions;

    private ReaderOptions() {
        this.batchRowCount = DEFAULT_BUFFERED_ROW_COUNT;
        this.bufferAllocator = ArrowUtils.getDefaultRootAllocator();
        this.reuseBatch = true;
        this.compressionCodec = CompressionCodec.NO_COMPRESSION;
        this.dataFormat = ArrowUtils.getDefaultDataFormat();
    }

    public int getBatchRowCount() {
        return batchRowCount;
    }

    public long getBatchRawSize() {
        return batchRawSize;
    }

    public BufferAllocator getBufferAllocator() {
        return bufferAllocator;
    }

    public boolean isReuseBatch() {
        return reuseBatch;
    }

    public boolean isAsync() {
        return async;
    }
    public BlockingQueue<Object> getAsyncQueue() {
        return asyncQueue;
    }

    public EnvironmentSettings getSettings() {
        return settings;
    }

    public DataFormat getDataFormat() {
        return dataFormat;
    }

    public CompressionCodec getCompressionCodec() {
        return compressionCodec;
    }

    public DiskSpillBufferOptions getDiskSpillBufferOptions() {
        return diskSpillBufferOptions;
    }

    public static ReaderOptions.Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private final ReaderOptions readerOptions = new ReaderOptions();

        public Builder withMaxBatchRowCount(int maxBatchRowCount) {
            Preconditions.checkInteger(maxBatchRowCount, 1, "Batch row count");
            this.readerOptions.batchRowCount = maxBatchRowCount;
            return this;
        }

        public Builder withBufferAllocator(BufferAllocator allocator) {
            Preconditions.checkNotNull(allocator, "Buffer allocator");
            this.readerOptions.bufferAllocator = allocator;
            return this;
        }

        /**
         * Configures ownership of {@link VectorSchemaRoot} batches returned by
         * {@code TableBatchReadSession.createArrowReader}.
         *
         * <p>When true, a batch remains valid only until the next {@code hasNext()} call or reader
         * close, and the caller must not close it. When false, every returned batch is owned by the
         * caller, may outlive later reader advances and reader close, and must be closed by the
         * caller.
         */
        public Builder withReuseBatch(boolean reuseBatch) {
            this.readerOptions.reuseBatch = reuseBatch;
            return this;
        }

        public Builder withCompressionCodec(CompressionCodec codec) {
            this.readerOptions.compressionCodec = codec;
            return this;
        }

        public Builder withSettings(EnvironmentSettings settings) {
            this.readerOptions.settings = settings;
            return this;
        }

        public Builder withDataFormat(DataFormat dataFormat) {
            Preconditions.checkNotNull(dataFormat, "Data format");
            this.readerOptions.dataFormat = dataFormat;
            return this;
        }

        public Builder withMaxBatchRawSize(long batchRawSize) {
            this.readerOptions.batchRawSize = batchRawSize;
            return this;
        }

        public Builder withAsync(boolean async) {
            this.readerOptions.async = async;
            return this;
        }
        public Builder withAsyncQueue(BlockingQueue<Object> asyncQueue) {
            this.readerOptions.asyncQueue = asyncQueue;
            return this;
        }

        /**
         * Enables bounded background buffering with local-disk spill.
         *
         * <p>Disk spill buffering already drains the network asynchronously and therefore cannot
         * be combined with {@link #withAsync(boolean)}.
         */
        public Builder withDiskSpillBuffer(DiskSpillBufferOptions options) {
            this.readerOptions.diskSpillBufferOptions = options;
            return this;
        }

        public ReaderOptions build() {
            Preconditions.checkNotNull(readerOptions.settings,
                    "Environment settings", "required");
            return this.readerOptions;
        }
    }
}
