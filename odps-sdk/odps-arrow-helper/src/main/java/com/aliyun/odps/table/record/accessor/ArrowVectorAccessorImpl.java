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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.table.record.accessor;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.complex.ListVector;

import com.aliyun.odps.data.Vector;
import com.aliyun.odps.exceptions.SchemaMismatchRuntimeException;
import com.aliyun.odps.table.arrow.accessor.ArrowArrayAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.VectorTypeInfo;

public class ArrowVectorAccessorImpl {

    public static class ArrowVectorAccessorForRecord extends ArrowArrayAccessor<Vector> {

        private final boolean isExtension;
        private final boolean encodeString;
        private final TypeInfo elementTypeInfo;
        private final int expectedDimension;
        private final ArrowVectorAccessor dataAccessor;

        public ArrowVectorAccessorForRecord(ListVector vector, TypeInfo typeInfo,
                                            boolean isExtension, boolean encodeString) {
            super(vector);
            this.isExtension = isExtension;
            this.encodeString = encodeString;
            if (typeInfo instanceof VectorTypeInfo) {
                VectorTypeInfo vectorType = (VectorTypeInfo) typeInfo;
                this.elementTypeInfo = vectorType.getElementTypeInfo();
                this.expectedDimension = vectorType.getDimension();
            } else {
                this.elementTypeInfo = TypeInfoFactory.FLOAT;
                this.expectedDimension = 0;
            }
            this.dataAccessor = ArrowToRecordConverter.createColumnVectorAccessor(
                vector.getDataVector(), elementTypeInfo, isExtension, encodeString);
        }

        public Vector getVector(int rowId) {
            return getArray(rowId);
        }

        @Override
        protected Vector getArrayData(int offset, int length) {
            if (expectedDimension > 0 && length != expectedDimension) {
                throw new SchemaMismatchRuntimeException(
                    "Vector dimension mismatch: expected " + expectedDimension + ", but got " + length);
            }
            List<Number> values = new ArrayList<>(length);
            try {
                for (int i = 0; i < length; i++) {
                    Object value = ArrowToRecordConverter.getData(dataAccessor,
                        elementTypeInfo, offset + i, isExtension, encodeString);
                    if (!(value instanceof Number)) {
                        throw new IllegalArgumentException(
                            "Vector element must be a number, but was: " + value);
                    }
                    values.add((Number) value);
                }
                return Vector.fromList(values, elementTypeInfo.getOdpsType());
            } catch (Exception e) {
                throw new RuntimeException("Could not get the vector", e);
            }
        }
    }
}
