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
import com.aliyun.odps.data.OdpsTypeTransformer;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.table.arrow.constructor.ArrowDateDayWriter;
import org.apache.arrow.vector.DateDayVector;

import java.time.LocalDate;
import java.util.List;

import static com.aliyun.odps.data.ArrayRecord.DEFAULT_CALENDAR;

public class ArrowDateDayWriterImpl {

    private static int transformToEpochDay(Object date) {
        LocalDate localDate;
        if (date instanceof LocalDate) {
            localDate = (LocalDate) date;
        } else {
            localDate = OdpsTypeTransformer.dateToLocalDate((java.sql.Date)date, DEFAULT_CALENDAR);
        }
        return Math.toIntExact(localDate.toEpochDay());
    }

    public static final class RecordDateWriter extends ArrowDateDayWriter<ArrayRecord> {

        RecordDateWriter(DateDayVector dateDayVector) {
            super(dateDayVector);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }

        @Override
        protected int readEpochDay(ArrayRecord row, int ordinal) {
            return transformToEpochDay(row.get(ordinal));
        }
    }

    public static final class ListDateWriter extends ArrowDateDayWriter<List<Object>> {

        ListDateWriter(DateDayVector DateVector) {
            super(DateVector);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected int readEpochDay(List<Object> in, int ordinal) {
            return transformToEpochDay(in.get(ordinal));
        }
    }

    public static final class StructDateWriter extends ArrowDateDayWriter<Struct> {

        StructDateWriter(DateDayVector dateDayVector) {
            super(dateDayVector);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected int readEpochDay(Struct in, int ordinal) {
            return transformToEpochDay(in.getFieldValue(ordinal));
        }
    }
}
