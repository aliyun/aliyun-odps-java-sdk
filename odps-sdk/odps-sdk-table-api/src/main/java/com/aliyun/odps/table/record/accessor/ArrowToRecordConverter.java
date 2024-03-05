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

package com.aliyun.odps.table.record.accessor;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.table.arrow.accessor.*;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import static com.aliyun.odps.table.utils.DateTimeConstants.*;

public class ArrowToRecordConverter {

    public static ArrowVectorAccessor createColumnVectorAccessor(ValueVector vector,
                                                                 TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN:
                return new ArrowBitAccessor((BitVector) vector);
            case TINYINT:
                return new ArrowTinyIntAccessor((TinyIntVector) vector);
            case SMALLINT:
                return new ArrowSmallIntAccessor((SmallIntVector) vector);
            case INT:
                return new ArrowIntAccessor((IntVector) vector);
            case BIGINT:
                return new ArrowBigIntAccessor((BigIntVector) vector);
            case FLOAT:
                return new ArrowFloat4Accessor((Float4Vector) vector);
            case DOUBLE:
                return new ArrowFloat8Accessor((Float8Vector) vector);
            case DECIMAL:
                return new ArrowDecimalAccessor((DecimalVector) vector);
            case STRING:
            case VARCHAR:
            case CHAR:
                return new ArrowVarCharAccessor((VarCharVector) vector);
            case BINARY:
                return new ArrowVarBinaryAccessor((VarBinaryVector) vector);
            case DATE:
                return new ArrowDateDayAccessor((DateDayVector) vector);
            case DATETIME:
            case TIMESTAMP:
                return new ArrowTimestampAccessor((TimeStampVector) vector);
            case ARRAY:
                return new ArrowArrayAccessorImpl.ArrowArrayAccessorForRecord((ListVector) vector, typeInfo);
            case MAP:
                return new ArrowMapAccessorImpl.ArrowMapAccessorForRecord((MapVector) vector, typeInfo);
            case STRUCT:
                return new ArrowStructAccessorImpl.ArrowStructAccessorForRecord((StructVector) vector, typeInfo);
            default:
                throw new UnsupportedOperationException("Datatype not supported: " + typeInfo.getTypeName());
        }
    }

    public static Object getData(ArrowVectorAccessor dataAccessor,
                                 TypeInfo typeInfo,
                                 int rowId) throws IOException {
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN:
                return ((ArrowBitAccessor) dataAccessor).getBoolean(rowId);
            case TINYINT:
                return ((ArrowTinyIntAccessor) dataAccessor).getByte(rowId);
            case SMALLINT:
                return ((ArrowSmallIntAccessor) dataAccessor).getShort(rowId);
            case INT:
                return ((ArrowIntAccessor) dataAccessor).getInt(rowId);
            case BIGINT:
                return ((ArrowBigIntAccessor) dataAccessor).getLong(rowId);
            case FLOAT:
                return ((ArrowFloat4Accessor) dataAccessor).getFloat(rowId);
            case DOUBLE:
                return ((ArrowFloat8Accessor) dataAccessor).getDouble(rowId);
            case DECIMAL:
                return ((ArrowDecimalAccessor) dataAccessor).getDecimal(rowId);
            case STRING:
                return ((ArrowVarCharAccessor) dataAccessor).getBytes(rowId);
            case VARCHAR:
                return new Varchar(new String(((ArrowVarCharAccessor) dataAccessor).getBytes(rowId), ConfigConstants.DEFAULT_CHARSET));
            case CHAR:
                return new Char(new String(((ArrowVarCharAccessor) dataAccessor).getBytes(rowId), ConfigConstants.DEFAULT_CHARSET));
            case BINARY:
                return new Binary(((ArrowVarBinaryAccessor) dataAccessor).getBinary(rowId));
            case DATE:
                return LocalDate.ofEpochDay(((ArrowDateDayAccessor) dataAccessor).getEpochDay(rowId));
            case DATETIME:
                return convertToTimeStamp(((ArrowTimestampAccessor) dataAccessor).getType(),
                            ((ArrowTimestampAccessor) dataAccessor).getEpochTime(rowId))
                            .atZone(ZoneId.systemDefault());
            case TIMESTAMP:
                return convertToTimeStamp(((ArrowTimestampAccessor) dataAccessor).getType(),
                        ((ArrowTimestampAccessor) dataAccessor).getEpochTime(rowId));
            case ARRAY:
                return (((ArrowArrayAccessorImpl.ArrowArrayAccessorForRecord) dataAccessor)).getArray(rowId);
            case MAP:
                return (((ArrowMapAccessorImpl.ArrowMapAccessorForRecord) dataAccessor)).getMap(rowId);
            case STRUCT:
                return (((ArrowStructAccessorImpl.ArrowStructAccessorForRecord) dataAccessor)).getStruct(rowId);
            default:
                throw new UnsupportedOperationException("Datatype not supported: " + typeInfo.getTypeName());
        }
    }

    private static Instant convertToTimeStamp(ArrowType.Timestamp timestampType, long epochTime) {
        switch (timestampType.getUnit()) {
            case SECOND:
                return Instant.ofEpochSecond(epochTime);
            case MILLISECOND:
                return Instant.ofEpochMilli(epochTime);
            case MICROSECOND:
                return microsToInstant(epochTime);
            case NANOSECOND:
                return nanosToInstant(epochTime);
            default:
                throw new UnsupportedOperationException(
                        "Unit not supported: " + timestampType.getUnit());
        }
    }

    private static Instant microsToInstant(long micros) {
        long secs = Math.floorDiv(micros, MICROS_PER_SECOND);
        long mos = micros - secs * MICROS_PER_SECOND;
        return Instant.ofEpochSecond(secs, mos * NANOS_PER_MICROS);
    }

    private static Instant nanosToInstant(long nanos) {
        long secs = Math.floorDiv(nanos, NANOS_PER_SECOND);
        long nos = nanos - secs * NANOS_PER_SECOND;
        return Instant.ofEpochSecond(secs, nos);
    }
}
