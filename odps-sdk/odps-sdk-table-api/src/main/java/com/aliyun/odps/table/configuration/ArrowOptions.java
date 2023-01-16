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

public class ArrowOptions implements Serializable {

    public static final TimestampUnit DEFAULT_TIMESTAMP_UNIT = TimestampUnit.NANO;
    public static final TimestampUnit DEFAULT_DATETIME_UNIT = TimestampUnit.MILLI;

    private TimestampUnit timestampUnit;
    private TimestampUnit dateTimeUnit;
    // TODO: arrow extension mode
    // TODO: arrow data version

    private ArrowOptions() {
        this.timestampUnit = DEFAULT_TIMESTAMP_UNIT;
        this.dateTimeUnit = DEFAULT_DATETIME_UNIT;
    }

    public TimestampUnit getTimestampUnit() {
        return timestampUnit;
    }

    public TimestampUnit getDateTimeUnit() {
        return dateTimeUnit;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static ArrowOptions createDefault() {
        return newBuilder().build();
    }

    public static class Builder {

        private final ArrowOptions arrowOptions;

        public Builder() {
            this.arrowOptions = new ArrowOptions();
        }

        public Builder withTimestampUnit(TimestampUnit unit) {
            this.arrowOptions.timestampUnit = unit;
            return this;
        }

        public Builder withDatetimeUnit(TimestampUnit unit) {
            this.arrowOptions.dateTimeUnit = unit;
            return this;
        }

        public ArrowOptions build() {
            return this.arrowOptions;
        }
    }

    public enum TimestampUnit {
        SECOND,
        MILLI,
        MICRO,
        NANO;

        @Override
        public String toString() {
            switch (this) {
                case SECOND:
                    return "second";
                case MILLI:
                    return "milli";
                case MICRO:
                    return "micro";
                case NANO:
                    return "nano";
                default:
                    throw new IllegalArgumentException("Unexpected time stamp unit");
            }
        }
    }
}
