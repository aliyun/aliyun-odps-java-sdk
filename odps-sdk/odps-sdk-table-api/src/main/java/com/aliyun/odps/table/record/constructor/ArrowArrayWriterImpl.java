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
import com.aliyun.odps.table.arrow.constructor.ArrowArrayWriter;
import com.aliyun.odps.table.arrow.constructor.ArrowFieldWriter;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.List;

public class ArrowArrayWriterImpl {

    public static abstract class ArrowArrayWriterBase<IN> extends ArrowArrayWriter<IN, List<Object>> {

        public ArrowArrayWriterBase(ListVector listVector, TypeInfo typeInfo) {
            super(listVector, createArrowFieldWriterForList(listVector.getDataVector(), ((ArrayTypeInfo) typeInfo).getElementTypeInfo()));
        }

        @Override
        protected int numElements(List<Object> arrayData) {
            return arrayData.size();
        }
    }

    public static final class RecordArrayWriter extends ArrowArrayWriterBase<ArrayRecord> {

        RecordArrayWriter(ListVector listVector, TypeInfo typeInfo) {
            super(listVector, typeInfo);
        }

        @Override
        protected List<Object> readArray(ArrayRecord record, int ordinal) {
            return (List<Object>) record.getArray(ordinal);
        }

        @Override
        protected boolean isNullAt(ArrayRecord record, int ordinal) {
            return record.isNull(ordinal);
        }
    }

    public static final class ListArrayWriter extends ArrowArrayWriterBase<List<Object>> {

        ListArrayWriter(ListVector listVector, TypeInfo typeInfo) {
            super(listVector, typeInfo);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected List<Object> readArray(List<Object> in, int ordinal) {
            return (List<Object>) in.get(ordinal);
        }
    }

    public static final class StructArrayWriter extends ArrowArrayWriterBase<Struct> {

        StructArrayWriter(ListVector listVector, TypeInfo typeInfo) {
            super(listVector, typeInfo);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected List<Object> readArray(Struct in, int ordinal) {
            return (List<Object>) in.getFieldValue(ordinal);
        }
    }

    public static ArrowFieldWriter<List<Object>> createArrowFieldWriterForList(
            ValueVector vector,
            TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN:
                return new ArrowBitWriterImpl.ListBitWriter((BitVector) vector);
            case TINYINT:
                return new ArrowTinyIntWriterImpl.ListTinyIntWriter((TinyIntVector) vector);
            case SMALLINT:
                return new ArrowSmallIntWriterImpl.ListSmallIntWriter((SmallIntVector) vector);
            case INT:
                return new ArrowIntWriterImpl.ListIntWriter((IntVector) vector);
            case BIGINT:
                return new ArrowBigIntWriterImpl.ListBigIntWriter((BigIntVector) vector);
            case FLOAT:
                return new ArrowFloat4WriterImpl.ListFloat4Writer((Float4Vector) vector);
            case DOUBLE:
                return new ArrowFloat8WriterImpl.ListFloat8Writer((Float8Vector) vector);
            case DECIMAL:
                return new ArrowDecimalWriterImpl.ListDecimalWriter((DecimalVector) vector, typeInfo);
            case STRING:
            case VARCHAR:
            case CHAR:
                return new ArrowVarCharWriterImpl.ListVarCharWriter((VarCharVector) vector);
            case BINARY:
                return new ArrowVarBinaryWriterImpl.ListVarBinaryWriter((VarBinaryVector) vector);
            case DATE:
                return new ArrowDateDayWriterImpl.ListDateWriter((DateDayVector) vector);
            case DATETIME:
                return new ArrowDateTimeWriterImpl.ListDateTimeWriter((TimeStampVector) vector);
            case TIMESTAMP:
                return new ArrowTimeStampWriterImpl.ListTimeStampWriter((TimeStampVector) vector);
            case ARRAY:
                return new ArrowArrayWriterImpl.ListArrayWriter((ListVector) vector, typeInfo);
            case MAP:
                return new ArrowMapWriterImpl.ListMapWriter((MapVector) vector, typeInfo);
            case STRUCT:
                return new ArrowStructWriterImpl.ListStructWriter((StructVector) vector, typeInfo);
            default:
                throw new UnsupportedOperationException("Datatype not supported: " + typeInfo.getTypeName());
        }
    }
}
