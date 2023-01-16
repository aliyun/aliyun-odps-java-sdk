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

public class SplitOptions {

    private static final long DEFAULT_SPLIT_SIZE = 256 * 1024L * 1024L;
    private static final boolean DEFAULT_CROSS_PARTITION = true;
    private static final SplitMode DEFAULT_SPLIT_MODE = SplitMode.SIZE;
    private static final SplitUnit DEFAULT_SPLIT_SIZE_MODE = SplitUnit.BYTE_SIZE;

    private SplitMode splitMode;
    private SplitUnit splitUnit;
    private long splitNumber;
    private boolean crossPartition;

    private SplitOptions() {
        this.splitMode = DEFAULT_SPLIT_MODE;
        this.splitUnit = DEFAULT_SPLIT_SIZE_MODE;
        this.splitNumber = DEFAULT_SPLIT_SIZE;
        this.crossPartition = DEFAULT_CROSS_PARTITION;
    }

    public static SplitOptions.Builder newBuilder() {
        return new Builder();
    }

    public static SplitOptions createDefault() {
        return new Builder().SplitByByteSize(DEFAULT_SPLIT_SIZE).build();
    }

    public boolean isCrossPartition() {
        return crossPartition;
    }

    public long getSplitNumber() {
        return splitNumber;
    }

    public SplitUnit getSplitUnit() {
        return splitUnit;
    }

    public SplitMode getSplitMode() {
        return splitMode;
    }

    public static class Builder {

        private SplitOptions splitOptions;

        public SplitOptions.Builder SplitByByteSize(long splitByteSize) {
            Preconditions.checkLong(splitByteSize, 1024L, "splitByteSize");
            this.splitOptions = new SplitOptions();
            this.splitOptions.splitNumber = splitByteSize;
            this.splitOptions.splitUnit = SplitUnit.BYTE_SIZE;
            this.splitOptions.splitMode = SplitMode.SIZE;
            return this;
        }

        public SplitOptions.Builder SplitByRowCount(long rowCount) {
            Preconditions.checkLong(rowCount, 1, "rowCount");
            this.splitOptions = new SplitOptions();
            this.splitOptions.splitNumber = rowCount;
            this.splitOptions.splitUnit = SplitUnit.ROW_COUNT;
            this.splitOptions.splitMode = SplitMode.SIZE;
            return this;
        }

        public SplitOptions.Builder SplitByParallelism(long splitParallelism) {
            return SplitByParallelism(splitParallelism, SplitUnit.BYTE_SIZE);
        }

        public SplitOptions.Builder SplitByParallelism(long splitParallelism, SplitUnit mode) {
            Preconditions.checkLong(splitParallelism, 1, "splitParallelism");
            this.splitOptions = new SplitOptions();
            this.splitOptions.splitNumber = splitParallelism;
            this.splitOptions.splitUnit = mode;
            this.splitOptions.splitMode = SplitMode.PARALLELISM;
            return this;
        }

        public SplitOptions.Builder SplitByRowOffset() {
            this.splitOptions = new SplitOptions();
            this.splitOptions.splitMode = SplitMode.ROW_OFFSET;
            return this;
        }

        public SplitOptions.Builder SplitByBucket() {
            this.splitOptions = new SplitOptions();
            this.splitOptions.splitMode = SplitMode.BUCKET;
            return this;
        }

        public SplitOptions.Builder withCrossPartition(boolean crossPartition) {
            Preconditions.checkNotNull(this.splitOptions, "Split option");
            this.splitOptions.crossPartition = crossPartition;
            return this;
        }

        public SplitOptions build() {
            return this.splitOptions;
        }
    }

    public enum SplitMode {
        SIZE,
        PARALLELISM,
        ROW_OFFSET,
        BUCKET;

        @Override
        public String toString() {
            switch (this) {
                case SIZE:
                    return "Size";
                case PARALLELISM:
                    return "Parallelism";
                case ROW_OFFSET:
                    return "RowOffset";
                case BUCKET:
                    return "Bucket";
                default:
                    throw new IllegalArgumentException("Unexpected split mode");
            }
        }
    }

    public enum SplitUnit {
        BYTE_SIZE,
        ROW_COUNT;

        @Override
        public String toString() {
            switch (this) {
                case BYTE_SIZE:
                    return "ByteSize";
                case ROW_COUNT:
                    return "RowCount";
                default:
                    throw new IllegalArgumentException("Unexpected split unit");
            }
        }
    }
}
