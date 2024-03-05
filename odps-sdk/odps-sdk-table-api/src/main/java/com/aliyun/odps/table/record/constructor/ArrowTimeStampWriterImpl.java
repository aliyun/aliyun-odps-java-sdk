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

package com.aliyun.odps.table.record.constructor;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.table.arrow.constructor.ArrowTimeStampWriter;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.TimeUnit;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static com.aliyun.odps.table.utils.DateTimeConstants.MICROS_PER_SECOND;
import static com.aliyun.odps.table.utils.DateTimeConstants.NANOS_PER_SECOND;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ArrowTimeStampWriterImpl {

    private static long transformToEpochTime(Object timestamp, TimeUnit unit) {
        Instant instant;
        if (timestamp instanceof Instant) {
            instant = (Instant) timestamp;
        } else {
            instant = ((Timestamp) timestamp).toInstant();
        }
        return instantToEpochTime(instant, unit);
    }

    public static final class RecordTimeStampWriter extends ArrowTimeStampWriter<ArrayRecord> {

        RecordTimeStampWriter(TimeStampVector timeStampVector) {
            super(timeStampVector);
        }

        @Override
        protected long readEpochTime(ArrayRecord row, int ordinal) {
            return transformToEpochTime(row.get(ordinal), getType().getUnit());
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }
    }

    public static final class ListTimeStampWriter extends ArrowTimeStampWriter<List<Object>> {

        ListTimeStampWriter(TimeStampVector timeStampVector) {
            super(timeStampVector);
        }

        @Override
        protected long readEpochTime(List<Object> row, int ordinal) {
            return transformToEpochTime(row.get(ordinal), getType().getUnit());
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }
    }

    public static final class StructTimeStampWriter extends ArrowTimeStampWriter<Struct> {

        StructTimeStampWriter(TimeStampVector timeStampVector) {
            super(timeStampVector);
        }

        @Override
        protected long readEpochTime(Struct row, int ordinal) {
            return transformToEpochTime(row.getFieldValue(ordinal), getType().getUnit());
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }
    }

    public static long instantToNanos(Instant instant) {
        long secs = instant.getEpochSecond();
        long nanos = instant.getNano();
        if (secs < 0 && nanos > 0) {
            long us = Math.multiplyExact(secs + 1, NANOS_PER_SECOND);
            return Math.addExact(us, nanos - NANOS_PER_SECOND);
        } else {
            long us = Math.multiplyExact(secs, NANOS_PER_SECOND);
            return Math.addExact(us, nanos);
        }
    }

    public static long instantToMicros(Instant instant) {
        long secs = instant.getEpochSecond();
        long nanos = instant.getNano();
        if (secs < 0 && nanos > 0) {
            long us = Math.multiplyExact(secs + 1, MICROS_PER_SECOND);
            return Math.addExact(us, NANOSECONDS.toMicros(nanos) - MICROS_PER_SECOND);
        } else {
            long us = Math.multiplyExact(secs, MICROS_PER_SECOND);
            return Math.addExact(us, NANOSECONDS.toMicros(nanos));
        }
    }

    public static long instantToEpochTime(Instant instant, TimeUnit unit) {
        switch (unit) {
            case SECOND:
                return instant.getEpochSecond();
            case MILLISECOND:
                return instant.toEpochMilli();
            case MICROSECOND:
                return instantToMicros(instant);
            case NANOSECOND:
                return instantToNanos(instant);
            default:
                throw new UnsupportedOperationException("Unit not supported: " + unit);
        }
    }
}
