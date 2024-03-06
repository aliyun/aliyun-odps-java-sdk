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

import static com.aliyun.odps.table.utils.DateTimeConstants.MICROS_PER_SECOND;
import static com.aliyun.odps.table.utils.DateTimeConstants.NANOS_PER_MICROS;
import static com.aliyun.odps.table.utils.DateTimeConstants.NANOS_PER_SECOND;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.SimpleJsonValue;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.table.arrow.accessor.ArrowBigIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowBitAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowDateDayAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowDecimalAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowDecimalExtensionAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowFloat4Accessor;
import com.aliyun.odps.table.arrow.accessor.ArrowFloat8Accessor;
import com.aliyun.odps.table.arrow.accessor.ArrowIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowSmallIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowTimestampAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowTimestampExtensionAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowTinyIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVarBinaryAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVarCharAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.type.TypeInfo;

public class ArrowToRecordConverter {

  public static ArrowVectorAccessor createColumnVectorAccessor(ValueVector vector,
                                                               TypeInfo typeInfo) {
    return createColumnVectorAccessor(vector, typeInfo, false);
  }

  /**
   * @param isExtension 是否使用ODPS_EXTENSION模式, 默认为false, 即ARROW_COMPATIBLE模式
   *                    EXTENSION模式在DECIMAL_V1, TIMESTAMP类型上支持更多的精度和范围, 但需要更高的性能和成本.
   */
  public static ArrowVectorAccessor createColumnVectorAccessor(ValueVector vector,
                                                               TypeInfo typeInfo,
                                                               boolean isExtension) {
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
        if (isExtension && (vector instanceof FixedSizeBinaryVector)) {
          return new ArrowDecimalExtensionAccessor((FixedSizeBinaryVector) vector);
        } else {
          return new ArrowDecimalAccessor((DecimalVector) vector);
        }
      case STRING:
      case VARCHAR:
      case CHAR:
      case JSON:
        return new ArrowVarCharAccessor((VarCharVector) vector);
      case BINARY:
        return new ArrowVarBinaryAccessor((VarBinaryVector) vector);
      case DATE:
        return new ArrowDateDayAccessor((DateDayVector) vector);
      case DATETIME:
        return new ArrowTimestampAccessor((TimeStampVector) vector);
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        if (!isExtension) {
          return new ArrowTimestampAccessor((TimeStampVector) vector);
        } else {
          return new ArrowTimestampExtensionAccessor((StructVector) vector);
        }
      case ARRAY:
        return new ArrowArrayAccessorImpl.ArrowArrayAccessorForRecord((ListVector) vector,
                                                                      typeInfo, isExtension);
      case MAP:
        return new ArrowMapAccessorImpl.ArrowMapAccessorForRecord((MapVector) vector, typeInfo,
                                                                  isExtension);
      case STRUCT:
        return new ArrowStructAccessorImpl.ArrowStructAccessorForRecord((StructVector) vector,
                                                                        typeInfo, isExtension);
      default:
        throw new UnsupportedOperationException(
            "Datatype not supported: " + typeInfo.getTypeName());
    }
  }

  public static Object getData(ArrowVectorAccessor dataAccessor, TypeInfo typeInfo, int rowId)
      throws IOException {
    return getData(dataAccessor, typeInfo, rowId, false);
  }

  public static Object getData(ArrowVectorAccessor dataAccessor, TypeInfo typeInfo, int rowId,
                               boolean isExtension)
      throws IOException {
    if (dataAccessor.isNullAt(rowId)) {
      return null;
    }
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
        if (isExtension && (dataAccessor instanceof ArrowDecimalExtensionAccessor)) {
          return ((ArrowDecimalExtensionAccessor) dataAccessor).getDecimal(rowId);
        } else {
          return ((ArrowDecimalAccessor) dataAccessor).getDecimal(rowId);
        }
      case STRING:
        return ((ArrowVarCharAccessor) dataAccessor).getBytes(rowId);
      case VARCHAR:
        return new Varchar(new String(((ArrowVarCharAccessor) dataAccessor).getBytes(rowId),
                                      ConfigConstants.DEFAULT_CHARSET));
      case CHAR:
        return new Char(new String(((ArrowVarCharAccessor) dataAccessor).getBytes(rowId),
                                   ConfigConstants.DEFAULT_CHARSET));
      case BINARY:
        return new Binary(((ArrowVarBinaryAccessor) dataAccessor).getBinary(rowId));
      case DATE:
        return LocalDate.ofEpochDay(((ArrowDateDayAccessor) dataAccessor).getEpochDay(rowId));
      case DATETIME:
        return convertToTimeStamp(((ArrowTimestampAccessor) dataAccessor).getType(),
                                  ((ArrowTimestampAccessor) dataAccessor).getEpochTime(
                                      rowId)).atZone(ZoneId.systemDefault());
      case TIMESTAMP:
        if (!isExtension) {
          return convertToTimeStamp(((ArrowTimestampAccessor) dataAccessor).getType(),
                                    ((ArrowTimestampAccessor) dataAccessor).getEpochTime(rowId));
        } else {
          return ((ArrowTimestampExtensionAccessor) dataAccessor).getTimestamp(rowId);
        }
      case TIMESTAMP_NTZ:
        if (!isExtension) {
          return convertToTimeStampNtz(((ArrowTimestampAccessor) dataAccessor).getType(),
                                       ((ArrowTimestampAccessor) dataAccessor).getEpochTime(rowId));
        } else {
          return ((ArrowTimestampExtensionAccessor) dataAccessor).getTimestampNtz(rowId);
        }
      case JSON:
        return new SimpleJsonValue(
            new String(((ArrowVarCharAccessor) dataAccessor).getBytes(rowId)));
      case ARRAY:
        return (((ArrowArrayAccessorImpl.ArrowArrayAccessorForRecord) dataAccessor)).getArray(
            rowId);
      case MAP:
        return (((ArrowMapAccessorImpl.ArrowMapAccessorForRecord) dataAccessor)).getMap(rowId);
      case STRUCT:
        return (((ArrowStructAccessorImpl.ArrowStructAccessorForRecord) dataAccessor)).getStruct(
            rowId);
      default:
        throw new UnsupportedOperationException(
            "Datatype not supported: " + typeInfo.getTypeName());
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
        throw new UnsupportedOperationException("Unit not supported: " + timestampType.getUnit());
    }
  }

  private static LocalDateTime convertToTimeStampNtz(ArrowType.Timestamp timestampType,
                                                     long epochTime) {
    return LocalDateTime.ofInstant(convertToTimeStamp(timestampType, epochTime), ZoneOffset.UTC);
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
