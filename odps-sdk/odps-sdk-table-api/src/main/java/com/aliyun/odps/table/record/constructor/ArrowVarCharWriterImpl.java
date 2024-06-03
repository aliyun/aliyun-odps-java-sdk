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

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.SimpleJsonValue;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.table.arrow.constructor.ArrowVarCharWriter;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.vector.VarCharVector;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ArrowVarCharWriterImpl {

    public static abstract class ArrowVarCharWriterBase<IN> extends ArrowVarCharWriter<IN> {

        ArrowVarCharWriterBase(VarCharVector varCharVector) {
            super(varCharVector);
        }

        abstract protected byte[] readBytes(IN in, int ordinal);

        @Override
        protected void setValue(IN in, int ordinal) {
            ((VarCharVector) valueVector).setSafe(count, readBytes(in, ordinal));
        }
    }

    public static final class RecordVarCharWriter extends ArrowVarCharWriterBase<ArrayRecord> {

        RecordVarCharWriter(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        protected byte[] readBytes(ArrayRecord record, int ordinal) {
            return record.getBytes(ordinal);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }
    }

    public static final class ListVarCharWriter extends ArrowVarCharWriterBase<List<Object>> {

        ListVarCharWriter(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected byte[] readBytes(List<Object> row, int ordinal) {
            Object v = row.get(ordinal);
            if (!(v instanceof byte[])) {
                return stringToBytes(v.toString());
            }
            return (byte[]) v;
        }
    }

    public static final class StructVarCharWriter extends ArrowVarCharWriterBase<Struct> {

        StructVarCharWriter(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected byte[] readBytes(Struct in, int ordinal) {
            Object v = in.getFieldValue(ordinal);
            if (!(v instanceof byte[])) {
                return stringToBytes(v.toString());
            }
            return (byte[]) v;
        }
    }

    private static byte[] stringToBytes(String string) {
        try {
            return string.getBytes(ConfigConstants.DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
