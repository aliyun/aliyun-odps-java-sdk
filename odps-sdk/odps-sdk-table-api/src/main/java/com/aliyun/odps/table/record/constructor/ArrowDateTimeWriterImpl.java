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
import com.google.common.base.Preconditions;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.TimeUnit;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

public class ArrowDateTimeWriterImpl {

    private static long transformToEpochTime(Object datetime) {
        long longValue;
        if (datetime instanceof ZonedDateTime) {
            longValue = ((ZonedDateTime) datetime).toInstant().toEpochMilli();
        } else {
            longValue = ((Date) datetime).getTime();
        }
        return longValue;
    }

    public static final class RecordDateTimeWriter extends ArrowTimeStampWriter<ArrayRecord> {

        RecordDateTimeWriter(TimeStampVector timeStampVector) {
            super(timeStampVector);
            Preconditions.checkState(getType().getUnit().equals(TimeUnit.MILLISECOND));
        }

        @Override
        protected long readEpochTime(ArrayRecord row, int ordinal) {
            return transformToEpochTime(row.get(ordinal));
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }
    }

    public static final class ListDateTimeWriter extends ArrowTimeStampWriter<List<Object>> {

        ListDateTimeWriter(TimeStampVector timeStampVector) {
            super(timeStampVector);
        }

        @Override
        protected long readEpochTime(List<Object> row, int ordinal) {
            return transformToEpochTime(row.get(ordinal));
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }
    }

    public static final class StructDateTimeWriter extends ArrowTimeStampWriter<Struct> {

        StructDateTimeWriter(TimeStampVector timeStampVector) {
            super(timeStampVector);
        }

        @Override
        protected long readEpochTime(Struct row, int ordinal) {
            return transformToEpochTime(row.getFieldValue(ordinal));
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }
    }
}
