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
import com.aliyun.odps.table.arrow.constructor.ArrowBitWriter;
import org.apache.arrow.vector.BitVector;

import java.util.List;

public class ArrowBitWriterImpl {

    public static final class RecordBitWriter extends ArrowBitWriter<ArrayRecord> {

        RecordBitWriter(BitVector bitVector) {
            super(bitVector);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }

        @Override
        protected boolean readBoolean(ArrayRecord row, int ordinal) {
            return row.getBoolean(ordinal);
        }
    }

    public static final class ListBitWriter extends ArrowBitWriter<List<Object>> {

        ListBitWriter(BitVector bitVector) {
            super(bitVector);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected boolean readBoolean(List<Object> in, int ordinal) {
            return (boolean) in.get(ordinal);
        }
    }

    public static final class StructBitWriter extends ArrowBitWriter<Struct> {

        StructBitWriter(BitVector bitVector) {
            super(bitVector);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected boolean readBoolean(Struct in, int ordinal) {
            return (boolean) in.getFieldValue(ordinal);
        }
    }
}
