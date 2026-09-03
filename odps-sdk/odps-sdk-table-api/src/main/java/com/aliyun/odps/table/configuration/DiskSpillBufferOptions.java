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

import com.aliyun.odps.table.utils.Preconditions;
import org.apache.arrow.memory.BufferAllocator;

import java.io.File;

public class DiskSpillBufferOptions {

    private static final int DEFAULT_MEMORY_BUFFER_CAPACITY = 64;
    private static final long DEFAULT_MAX_MEMORY_BUFFER_BYTES = 256L * 1024 * 1024;
    private static final long DEFAULT_MAX_SPILL_BYTES = 10L * 1024 * 1024 * 1024;
    private static final long DEFAULT_MAX_SPILL_BYTES_IN_USE = 10L * 1024 * 1024 * 1024;
    private static final long DEFAULT_SPILL_FILE_TARGET_BYTES = 4L * 1024 * 1024;

    private int memoryBufferCapacity;
    private long maxMemoryBufferBytes;
    private File spillDirectory;
    private long maxSpillBytes;
    private long maxSpillBytesInUse;
    private long spillFileTargetBytes;
    private BufferAllocator allocator;

    private DiskSpillBufferOptions() {
        this.memoryBufferCapacity = DEFAULT_MEMORY_BUFFER_CAPACITY;
        this.maxMemoryBufferBytes = DEFAULT_MAX_MEMORY_BUFFER_BYTES;
        this.spillDirectory = new File(System.getProperty("java.io.tmpdir"));
        this.maxSpillBytes = DEFAULT_MAX_SPILL_BYTES;
        this.maxSpillBytesInUse = DEFAULT_MAX_SPILL_BYTES_IN_USE;
        this.spillFileTargetBytes = DEFAULT_SPILL_FILE_TARGET_BYTES;
    }

    public int getMemoryBufferCapacity() {
        return memoryBufferCapacity;
    }

    /**
     * Returns the maximum estimated Arrow buffer bytes retained in the in-memory queue.
     *
     * <p>This is a per-reader queue budget, not a total reader memory limit. It does not include
     * batches already handed to the consumer or the single batch being materialized from a spill
     * file.
     */
    public long getMaxMemoryBufferBytes() {
        return maxMemoryBufferBytes;
    }

    public File getSpillDirectory() {
        return spillDirectory;
    }

    /**
     * Returns the maximum cumulative Arrow IPC bytes written by one reader. This quota is not
     * released when files are consumed.
     */
    public long getMaxSpillBytes() {
        return maxSpillBytes;
    }

    /**
     * Returns the maximum Arrow IPC bytes that may exist on disk for this reader at once.
     * Successfully deleted spill files release this quota.
     */
    public long getMaxSpillBytesInUse() {
        return maxSpillBytesInUse;
    }

    public long getSpillFileTargetBytes() {
        return spillFileTargetBytes;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private final DiskSpillBufferOptions options = new DiskSpillBufferOptions();

        public Builder withMemoryBufferCapacity(int capacity) {
            Preconditions.checkInteger(capacity, 0, "Memory buffer capacity");
            this.options.memoryBufferCapacity = capacity;
            return this;
        }

        /**
         * Limits estimated Arrow buffer bytes retained in one reader's in-memory queue. A batch
         * that would exceed this limit is spilled even when a batch-count slot is available. This
         * does not cap consumer-owned batches or the batch currently read back from a spill file.
         */
        public Builder withMaxMemoryBufferBytes(long maxBytes) {
            Preconditions.checkLong(maxBytes, 0, "Max memory buffer bytes");
            this.options.maxMemoryBufferBytes = maxBytes;
            return this;
        }

        /**
         * Sets the local directory used for spill files.
         *
         * <p>The directory must be a dedicated writable local directory. Each reader only creates
         * and deletes its own isolated workspace. The SDK does not scan or reclaim workspaces left
         * by abnormal process termination; the application that owns this directory must clean
         * such leftovers only when no reader is using it.
         */
        public Builder withSpillDirectory(File directory) {
            Preconditions.checkNotNull(directory, "Spill directory");
            this.options.spillDirectory = directory;
            return this;
        }

        /**
         * Limits cumulative Arrow IPC bytes written by one reader, including schema, padding and
         * end-of-stream metadata. Consuming or deleting a file does not return this quota.
         */
        public Builder withMaxSpillBytes(long maxBytes) {
            Preconditions.checkLong(maxBytes, 0, "Max spill bytes");
            this.options.maxSpillBytes = maxBytes;
            return this;
        }

        /**
         * Limits current spill-file bytes for one reader. Unlike {@link #withMaxSpillBytes(long)},
         * quota is returned when a spill file is successfully deleted.
         */
        public Builder withMaxSpillBytesInUse(long maxBytes) {
            Preconditions.checkLong(maxBytes, 0, "Max spill bytes in use");
            this.options.maxSpillBytesInUse = maxBytes;
            return this;
        }

        public Builder withSpillFileTargetBytes(long bytes) {
            Preconditions.checkLong(bytes, 1, "Spill file target bytes");
            this.options.spillFileTargetBytes = bytes;
            return this;
        }

        public Builder withAllocator(BufferAllocator allocator) {
            Preconditions.checkNotNull(allocator, "Buffer allocator");
            this.options.allocator = allocator;
            return this;
        }

        public DiskSpillBufferOptions build() {
            if (!options.spillDirectory.exists()) {
                options.spillDirectory.mkdirs();
            }
            if (!options.spillDirectory.isDirectory() || !options.spillDirectory.canWrite()) {
                throw new IllegalArgumentException(
                        "Spill directory is not writable: " + options.spillDirectory);
            }
            return options;
        }
    }
}
