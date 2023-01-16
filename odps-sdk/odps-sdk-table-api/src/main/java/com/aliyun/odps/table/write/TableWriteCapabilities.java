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

package com.aliyun.odps.table.write;

import java.io.Serializable;

public final class TableWriteCapabilities implements Serializable {

    private boolean supportHashBuckets;
    private boolean supportRangeBuckets;
    private boolean supportDynamicPartition;

    private TableWriteCapabilities() {
        this.supportHashBuckets = false;
        this.supportRangeBuckets = false;
        this.supportDynamicPartition = true;
    }

    public boolean supportHashBuckets() {
        return supportHashBuckets;
    }

    public boolean supportRangeBuckets() {
        return supportRangeBuckets;
    }

    public boolean supportDynamicPartition() {
        return supportDynamicPartition;
    }

    public static TableWriteCapabilities.Builder newBuilder() {
        return new TableWriteCapabilities.Builder();
    }

    public static TableWriteCapabilities createDefault() {
        return newBuilder().build();
    }

    public static class Builder {

        private final TableWriteCapabilities writeCapabilities;

        public Builder() {
            this.writeCapabilities = new TableWriteCapabilities();
        }

        public TableWriteCapabilities.Builder supportHashBuckets(boolean supportHashBuckets) {
            this.writeCapabilities.supportHashBuckets = supportHashBuckets;
            return this;
        }

        public TableWriteCapabilities.Builder supportRangeBuckets(boolean supportRangeBuckets) {
            this.writeCapabilities.supportRangeBuckets = supportRangeBuckets;
            return this;
        }

        public TableWriteCapabilities.Builder supportDynamicPartition(boolean supportDynamicPartition) {
            this.writeCapabilities.supportDynamicPartition = supportDynamicPartition;
            return this;
        }

        public TableWriteCapabilities build() {
            return this.writeCapabilities;
        }
    }
}
