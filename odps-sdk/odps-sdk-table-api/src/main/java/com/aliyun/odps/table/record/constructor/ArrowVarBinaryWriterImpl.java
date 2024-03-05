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
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.table.arrow.constructor.ArrowVarBinaryWriter;
import org.apache.arrow.vector.VarBinaryVector;

import java.util.List;

public class ArrowVarBinaryWriterImpl {

    public static abstract class ArrowVarBinaryWriterBase<IN> extends ArrowVarBinaryWriter<IN> {

        ArrowVarBinaryWriterBase(VarBinaryVector varBinaryVector) {
            super(varBinaryVector);
        }

        protected abstract byte[] readBytes(IN in, int ordinal);

        @Override
        protected void setValue(IN in, int ordinal) {
            ((VarBinaryVector) valueVector).setSafe(count, readBytes(in, ordinal));
        }
    }

    public static final class RecordVarBinaryWriter extends ArrowVarBinaryWriterBase<ArrayRecord> {

        RecordVarBinaryWriter(VarBinaryVector VarBinaryVector) {
            super(VarBinaryVector);
        }

        @Override
        protected boolean isNullAt(ArrayRecord in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected byte[] readBytes(ArrayRecord in, int ordinal) {
            return in.getBinary(ordinal).data();
        }
    }

    public static final class ListVarBinaryWriter extends ArrowVarBinaryWriterBase<List<Object>> {

        ListVarBinaryWriter(VarBinaryVector varBinaryVector) {
            super(varBinaryVector);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected byte[] readBytes(List<Object> in, int ordinal) {
            return ((Binary) in.get(ordinal)).data();
        }
    }

    public static final class StructVarBinaryWriter extends ArrowVarBinaryWriterBase<Struct> {

        StructVarBinaryWriter(VarBinaryVector VarBinaryVector) {
            super(VarBinaryVector);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected byte[] readBytes(Struct in, int ordinal) {
            return ((Binary) in.getFieldValue(ordinal)).data();
        }
    }
}
