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

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.table.arrow.constructor.ArrowBatchConstructor;
import com.aliyun.odps.table.arrow.constructor.ArrowFieldWriter;
import com.aliyun.odps.table.utils.SchemaUtils;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public class RecordToArrowConverter {

    public static ArrowBatchConstructor<ArrayRecord> createRecordArrowBatchConstructor(
            List<Column> dataColumns, BufferAllocator allocator) {
        Schema arrowSchema = SchemaUtils.toArrowSchema(dataColumns);
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        return createRecordArrowBatchConstructor(dataColumns, root);
    }

    /**
     * Creates an {@link ArrowBatchConstructor} for the specified {@link VectorSchemaRoot}.
     */
    public static ArrowBatchConstructor<ArrayRecord> createRecordArrowBatchConstructor(List<Column> dataColumns,
                                                                                       VectorSchemaRoot root) {
        ArrowFieldWriter<ArrayRecord>[] fieldWriters =
                new ArrowFieldWriter[root.getFieldVectors().size()];
        List<FieldVector> vectors = root.getFieldVectors();
        if (vectors.size() != dataColumns.size()) {
            throw new RuntimeException();
        }
        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            vector.allocateNew();
            fieldWriters[i] = createArrowFieldWriterForRecord(vector, dataColumns.get(i).getTypeInfo());
        }
        return new ArrowBatchConstructor<>(root, fieldWriters);
    }

    public static ArrowFieldWriter<ArrayRecord> createArrowFieldWriterForRecord(
            ValueVector vector,
            TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN:
                return new ArrowBitWriterImpl.RecordBitWriter((BitVector) vector);
            case TINYINT:
                return new ArrowTinyIntWriterImpl.RecordTinyIntWriter((TinyIntVector) vector);
            case SMALLINT:
                return new ArrowSmallIntWriterImpl.RecordSmallIntWriter((SmallIntVector) vector);
            case INT:
                return new ArrowIntWriterImpl.RecordIntWriter((IntVector) vector);
            case BIGINT:
                return new ArrowBigIntWriterImpl.RecordBigIntWriter((BigIntVector) vector);
            case FLOAT:
                return new ArrowFloat4WriterImpl.RecordFloat4Writer((Float4Vector) vector);
            case DOUBLE:
                return new ArrowFloat8WriterImpl.RecordFloat8Writer((Float8Vector) vector);
            case DECIMAL:
                return new ArrowDecimalWriterImpl.RecordDecimalWriter((DecimalVector) vector, typeInfo);
            case STRING:
            case VARCHAR:
            case CHAR:
                return new ArrowVarCharWriterImpl.RecordVarCharWriter((VarCharVector) vector);
            case BINARY:
                return new ArrowVarBinaryWriterImpl.RecordVarBinaryWriter((VarBinaryVector) vector);
            case DATE:
                return new ArrowDateDayWriterImpl.RecordDateWriter((DateDayVector) vector);
            case DATETIME:
                return new ArrowDateTimeWriterImpl.RecordDateTimeWriter((TimeStampVector) vector);
            case TIMESTAMP:
                return new ArrowTimeStampWriterImpl.RecordTimeStampWriter((TimeStampVector) vector);
            case ARRAY:
                return new ArrowArrayWriterImpl.RecordArrayWriter((ListVector) vector, typeInfo);
            case MAP:
                return new ArrowMapWriterImpl.RecordMapWriter((MapVector) vector, typeInfo);
            case STRUCT:
                return new ArrowStructWriterImpl.RecordStructWriter((StructVector) vector, typeInfo);
            default:
                throw new UnsupportedOperationException("Datatype not supported: " + typeInfo.getTypeName());
        }
    }
}