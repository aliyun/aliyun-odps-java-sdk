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
import com.aliyun.odps.table.arrow.constructor.ArrowFieldWriter;
import com.aliyun.odps.table.arrow.constructor.ArrowStructWriter;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.List;

public class ArrowStructWriterImpl {

    public abstract static class ArrowStructWriterBase<IN> extends ArrowStructWriter<IN, Struct> {

        ArrowStructWriterBase(StructVector structVector,
                                      TypeInfo typeInfo) {
            super(structVector, createArrowFieldWritersForStruct(structVector, (StructTypeInfo) typeInfo));
        }

        protected abstract Struct readStruct(IN in, int ordinal);
    }

    public static final class RecordStructWriter extends ArrowStructWriterBase<ArrayRecord> {

        RecordStructWriter(StructVector structVector,
                                        TypeInfo typeInfo) {
            super(structVector, typeInfo);
        }

        @Override
        protected Struct readStruct(ArrayRecord record, int ordinal) {
            return record.getStruct(ordinal);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }
    }

    public static final class ListStructWriter extends ArrowStructWriterBase<List<Object>> {

        ListStructWriter(StructVector structVector,
                                      TypeInfo typeInfo) {
            super(structVector, typeInfo);
        }

        @Override
        protected Struct readStruct(List<Object> in, int ordinal) {
            return (Struct) in.get(ordinal);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }
    }

    public static final class StructArrowStructWriter extends ArrowStructWriterBase<Struct> {

        StructArrowStructWriter(StructVector structVector,
                                        TypeInfo typeInfo) {
            super(structVector, typeInfo);
        }

        @Override
        protected Struct readStruct(Struct struct, int ordinal) {
            return (Struct) struct.getFieldValue(ordinal);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }
    }

    public static ArrowFieldWriter<Struct>[] createArrowFieldWritersForStruct(StructVector structVector, StructTypeInfo typeInfo) {
        if (structVector.getChildFieldNames().size() != typeInfo.getFieldCount()) {
            throw new RuntimeException();
        }
        ArrowFieldWriter<Struct>[] fieldsWriters = new ArrowFieldWriter[structVector.getChildFieldNames().size()];
        for (int i = 0; i < structVector.getChildFieldNames().size(); i++) {
            if (!structVector.getChildFieldNames().get(i).equals(typeInfo.getFieldNames().get(i))) {
                throw new RuntimeException();
            }
            fieldsWriters[i] = createArrowFieldWriterForStruct(structVector.getChildByOrdinal(i),
                    typeInfo.getFieldTypeInfos().get(i));
        }
        return fieldsWriters;
    }

    public static ArrowFieldWriter<Struct> createArrowFieldWriterForStruct(
            ValueVector vector,
            TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN:
                return new ArrowBitWriterImpl.StructBitWriter((BitVector) vector);
            case TINYINT:
                return new ArrowTinyIntWriterImpl.StructTinyIntWriter((TinyIntVector) vector);
            case SMALLINT:
                return new ArrowSmallIntWriterImpl.StructSmallIntWriter((SmallIntVector) vector);
            case INT:
                return new ArrowIntWriterImpl.StructIntWriter((IntVector) vector);
            case BIGINT:
                return new ArrowBigIntWriterImpl.StructBigIntWriter((BigIntVector) vector);
            case FLOAT:
                return new ArrowFloat4WriterImpl.StructFloat4Writer((Float4Vector) vector);
            case DOUBLE:
                return new ArrowFloat8WriterImpl.StructFloat8Writer((Float8Vector) vector);
            case DECIMAL:
                return new ArrowDecimalWriterImpl.StructDecimalWriter((DecimalVector) vector, typeInfo);
            case STRING:
            case VARCHAR:
            case CHAR:
            case JSON:
                return new ArrowVarCharWriterImpl.StructVarCharWriter((VarCharVector) vector);
            case BINARY:
                return new ArrowVarBinaryWriterImpl.StructVarBinaryWriter((VarBinaryVector) vector);
            case DATE:
                return new ArrowDateDayWriterImpl.StructDateWriter((DateDayVector) vector);
            case DATETIME:
                return new ArrowDateTimeWriterImpl.StructDateTimeWriter((TimeStampVector) vector);
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
                return new ArrowTimeStampWriterImpl.StructTimeStampWriter((TimeStampVector) vector, typeInfo);
            case ARRAY:
                return new ArrowArrayWriterImpl.StructArrayWriter((ListVector) vector, typeInfo);
            case MAP:
                return new ArrowMapWriterImpl.StructMapWriter((MapVector) vector, typeInfo);
            case STRUCT:
                return new ArrowStructWriterImpl.StructArrowStructWriter((StructVector) vector, typeInfo);
            default:
                throw new UnsupportedOperationException("Datatype not supported: " + typeInfo.getTypeName());
        }
    }
}