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
import com.aliyun.odps.table.arrow.constructor.ArrowSmallIntWriter;
import org.apache.arrow.vector.SmallIntVector;

import java.util.List;

public class ArrowSmallIntWriterImpl {

    public static final class RecordSmallIntWriter extends ArrowSmallIntWriter<ArrayRecord> {

        RecordSmallIntWriter(SmallIntVector SmallIntVector) {
            super(SmallIntVector);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }

        @Override
        protected short readShort(ArrayRecord row, int ordinal) {
            return row.getSmallint(ordinal);
        }
    }

    public static final class ListSmallIntWriter extends ArrowSmallIntWriter<List<Object>> {

        ListSmallIntWriter(SmallIntVector SmallIntVector) {
            super(SmallIntVector);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected short readShort(List<Object> in, int ordinal) {
            return (short) in.get(ordinal);
        }
    }

    public static final class StructSmallIntWriter extends ArrowSmallIntWriter<Struct> {

        StructSmallIntWriter(SmallIntVector SmallIntVector) {
            super(SmallIntVector);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected short readShort(Struct in, int ordinal) {
            return (short) in.getFieldValue(ordinal);
        }
    }
}
