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
import com.aliyun.odps.table.arrow.constructor.ArrowTinyIntWriter;
import org.apache.arrow.vector.TinyIntVector;

import java.util.List;

public class ArrowTinyIntWriterImpl {

    public static final class RecordTinyIntWriter extends ArrowTinyIntWriter<ArrayRecord> {

        RecordTinyIntWriter(TinyIntVector tinyIntVector) {
            super(tinyIntVector);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }

        @Override
        protected byte readByte(ArrayRecord row, int ordinal) {
            return row.getTinyint(ordinal);
        }
    }

    public static final class ListTinyIntWriter extends ArrowTinyIntWriter<List<Object>> {

        ListTinyIntWriter(TinyIntVector tinyIntVector) {
            super(tinyIntVector);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected byte readByte(List<Object> in, int ordinal) {
            return (byte) in.get(ordinal);
        }
    }

    public static final class StructTinyIntWriter extends ArrowTinyIntWriter<Struct> {

        StructTinyIntWriter(TinyIntVector tinyIntVector) {
            super(tinyIntVector);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected byte readByte(Struct in, int ordinal) {
            return (byte) in.getFieldValue(ordinal);
        }
    }
}
