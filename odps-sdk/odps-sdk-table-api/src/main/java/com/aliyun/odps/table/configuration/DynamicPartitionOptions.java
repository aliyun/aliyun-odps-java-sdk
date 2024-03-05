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

import java.io.Serializable;

public class DynamicPartitionOptions implements Serializable {

    public static final int DEFAULT_DYNAMIC_PARTITION_LIMIT = 512;
    public static final int DEFAULT_INVALID_LIMIT = 100;
    public static final InvalidStrategy DEFAULT_INVALID_STRATEGY = InvalidStrategy.EXCEPTION;

    private InvalidStrategy invalidStrategy;
    private int invalidLimit;
    private int dynamicPartitionLimit;

    private DynamicPartitionOptions() {
        this.dynamicPartitionLimit = DEFAULT_DYNAMIC_PARTITION_LIMIT;
        this.invalidLimit = DEFAULT_INVALID_LIMIT;
        this.invalidStrategy = DEFAULT_INVALID_STRATEGY;
    }

    public int getDynamicPartitionLimit() {
        return dynamicPartitionLimit;
    }

    public long getInvalidLimit() {
        return invalidLimit;
    }

    public InvalidStrategy getInvalidStrategy() {
        return invalidStrategy;
    }

    public static DynamicPartitionOptions.Builder newBuilder() {
        return new DynamicPartitionOptions.Builder();
    }

    public static DynamicPartitionOptions createDefault() {
        return new DynamicPartitionOptions.Builder().build();
    }

    public static class Builder {

        private final DynamicPartitionOptions options = new DynamicPartitionOptions();

        public DynamicPartitionOptions.Builder withInvalidStrategy(InvalidStrategy invalidStrategy) {
            this.options.invalidStrategy = invalidStrategy;
            return this;
        }

        public DynamicPartitionOptions.Builder withInvalidLimit(int invalidLimit) {
            this.options.invalidLimit = invalidLimit;
            return this;
        }

        public DynamicPartitionOptions.Builder withDynamicPartitionLimit(int dynamicPartitionLimit) {
            this.options.dynamicPartitionLimit = dynamicPartitionLimit;
            return this;
        }

        public DynamicPartitionOptions build() {
            return options;
        }
    }

    public enum InvalidStrategy {
        DROP,
        LIMIT,
        EXCEPTION;

        @Override
        public String toString() {
            switch (this) {
                case DROP:
                    return "Drop";
                case LIMIT:
                    return "Limit";
                case EXCEPTION:
                    return "Exception";
                default:
                    throw new IllegalArgumentException("Unexpected invalid strategy");
            }
        }
    }

}
