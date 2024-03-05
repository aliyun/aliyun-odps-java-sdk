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

import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.utils.ArrowUtils;
import com.aliyun.odps.table.utils.Preconditions;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Optional;

import static com.aliyun.odps.table.utils.ConfigConstants.DEFAULT_BUFFERED_ROW_COUNT;
import static com.aliyun.odps.table.utils.ConfigConstants.DEFAULT_CHUNK_SIZE;

public class WriterOptions {

    private RetryStrategy retryStrategy;
    private int bufferedRowCount; // for buffered writer
    private BufferAllocator bufferAllocator;
    private EnvironmentSettings settings;
    private int chunkSize;
    private CompressionCodec compressionCodec;
    private DataFormat dataFormat;
    private long maxBlockNumber;

    public WriterOptions() {
        this.retryStrategy = new RetryStrategy();
        this.bufferedRowCount = DEFAULT_BUFFERED_ROW_COUNT;
        this.bufferAllocator = ArrowUtils.getDefaultRootAllocator();
        this.chunkSize = DEFAULT_CHUNK_SIZE;
        this.compressionCodec = CompressionCodec.NO_COMPRESSION;
        this.dataFormat = ArrowUtils.getDefaultDataFormat();
        this.maxBlockNumber = -1L;
    }

    public int getBufferedRowCount() {
        return bufferedRowCount;
    }

    public BufferAllocator getBufferAllocator() {
        return bufferAllocator;
    }

    public RetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    public EnvironmentSettings getSettings() {
        return settings;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public DataFormat getDataFormat() {
        return dataFormat;
    }

    public CompressionCodec getCompressionCodec() {
        return compressionCodec;
    }

    public Optional<Long> maxBlockNumber() {
        return maxBlockNumber > 0 ?
                Optional.of(maxBlockNumber) : Optional.empty();
    }

    public static WriterOptions.Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private final WriterOptions writerOptions = new WriterOptions();

        public WriterOptions.Builder withBufferedRowCount(int bufferSize) {
            Preconditions.checkInteger(bufferSize, 1, "Buffered row count");
            this.writerOptions.bufferedRowCount = bufferSize;
            return this;
        }

        public WriterOptions.Builder withRetryStrategy(RetryStrategy retryStrategy) {
            Preconditions.checkNotNull(retryStrategy, "Retry strategy");
            this.writerOptions.retryStrategy = retryStrategy;
            return this;
        }

        public WriterOptions.Builder withBufferAllocator(BufferAllocator allocator) {
            Preconditions.checkNotNull(allocator, "Buffer allocator");
            this.writerOptions.bufferAllocator = allocator;
            return this;
        }

        public WriterOptions.Builder withSettings(EnvironmentSettings settings) {
            this.writerOptions.settings = settings;
            return this;
        }

        public WriterOptions.Builder withChunkSize(int chunkSize) {
            Preconditions.checkInteger(chunkSize, 1500 - 4, "Chunk size");
            this.writerOptions.chunkSize = chunkSize;
            return this;
        }

        public WriterOptions.Builder withDataFormat(DataFormat dataFormat) {
            Preconditions.checkNotNull(dataFormat, "Data format");
            this.writerOptions.dataFormat = dataFormat;
            return this;
        }

        public WriterOptions.Builder withCompressionCodec(CompressionCodec codec) {
            this.writerOptions.compressionCodec = codec;
            return this;
        }

        public WriterOptions.Builder withMaxBlockNumber(long maxBlockNumber) {
            Preconditions.checkLong(maxBlockNumber, 1, "MaxBlockNumber");
            this.writerOptions.maxBlockNumber = maxBlockNumber;
            return this;
        }

        public WriterOptions build() {
            Preconditions.checkNotNull(writerOptions.settings,
                    "Environment settings", "required");
            return this.writerOptions;
        }
    }
}