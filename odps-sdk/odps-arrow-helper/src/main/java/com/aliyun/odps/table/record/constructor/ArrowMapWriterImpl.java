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
import com.aliyun.odps.table.arrow.constructor.ArrowMapWriter;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.aliyun.odps.table.record.constructor.ArrowArrayWriterImpl.createArrowFieldWriterForList;

public class ArrowMapWriterImpl {

    public abstract static class ArrowMapWriterBase<IN>
            extends ArrowMapWriter<IN, Map<Object, Object>, List<Object>, List<Object>> {

        ArrowMapWriterBase(MapVector mapVector,
                                  TypeInfo typeInfo) {
            super(mapVector,
                    createArrowFieldWriterForList((
                            (StructVector) mapVector.getDataVector()).getChild(MapVector.KEY_NAME), ((MapTypeInfo) typeInfo).getKeyTypeInfo()),
                    createArrowFieldWriterForList((
                            (StructVector) mapVector.getDataVector()).getChild(MapVector.VALUE_NAME), ((MapTypeInfo) typeInfo).getValueTypeInfo()));
        }

        @Override
        protected int numElements(Map<Object, Object> arrayData) {
            return arrayData.size();
        }

        @Override
        protected List<Object> readKeyArray(Map<Object, Object> mapData) {
            List<Object> keyArray = new ArrayList<>();
            mapData.forEach((key, value) -> {
                keyArray.add(key);
            });
            return keyArray;
        }

        @Override
        protected List<Object> readValueArray(Map<Object, Object> mapData) {
            List<Object> valueArray = new ArrayList<>();
            mapData.forEach((key, value) -> {
                valueArray.add(value);
            });
            return valueArray;
        }
    }

    public static final class RecordMapWriter extends ArrowMapWriterBase<ArrayRecord> {

        RecordMapWriter(MapVector mapVector, TypeInfo typeInfo) {
            super(mapVector, typeInfo);
        }

        @Override
        protected Map<Object, Object> readMap(ArrayRecord record, int ordinal) {
            return record.getMap(ordinal);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }
    }

    public static final class ListMapWriter extends ArrowMapWriterBase<List<Object>> {

        ListMapWriter(MapVector mapVector, TypeInfo typeInfo) {
            super(mapVector, typeInfo);
        }

        @Override
        protected Map<Object, Object> readMap(List<Object> in, int ordinal) {
            return (Map) in.get(ordinal);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }
    }

    public static final class StructMapWriter extends ArrowMapWriterBase<Struct> {

        StructMapWriter(MapVector mapVector, TypeInfo typeInfo) {
            super(mapVector, typeInfo);
        }

        @Override
        protected Map<Object, Object> readMap(Struct struct, int ordinal) {
            return (Map) struct.getFieldValue(ordinal);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }
    }
}