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
import com.aliyun.odps.table.arrow.constructor.ArrowIntWriter;
import org.apache.arrow.vector.IntVector;

import java.util.List;

public class ArrowIntWriterImpl {

    public static final class RecordIntWriter extends ArrowIntWriter<ArrayRecord> {

        RecordIntWriter(IntVector intVector) {
            super(intVector);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }

        @Override
        protected int readInt(ArrayRecord row, int ordinal) {
            return row.getInt(ordinal);
        }
    }

    public static final class ListIntWriter extends ArrowIntWriter<List<Object>> {

        ListIntWriter(IntVector intVector) {
            super(intVector);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected int readInt(List<Object> in, int ordinal) {
            return (int) in.get(ordinal);
        }
    }

    public static final class StructIntWriter extends ArrowIntWriter<Struct> {

        StructIntWriter(IntVector intVector) {
            super(intVector);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected int readInt(Struct in, int ordinal) {
            return (int) in.getFieldValue(ordinal);
        }
    }
}
