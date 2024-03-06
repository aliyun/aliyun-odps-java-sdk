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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.complex.ListVector;

import com.aliyun.odps.table.arrow.accessor.ArrowArrayAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.TypeInfo;

public class ArrowArrayAccessorImpl {

    public static class ArrowArrayAccessorForRecord extends ArrowArrayAccessor<List<Object>> {

        private final boolean isExtension;
        private final TypeInfo elementTypeInfo;
        private final ArrowVectorAccessor dataAccessor;

        public ArrowArrayAccessorForRecord(ListVector vector, TypeInfo typeInfo, boolean isExtension) {
            super(vector);
            this.isExtension = isExtension;
            this.elementTypeInfo = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
            this.dataAccessor = ArrowToRecordConverter.
                    createColumnVectorAccessor(vector.getDataVector(), elementTypeInfo, isExtension);
        }

        @Override
        protected List<Object> getArrayData(int offset, int length) {
            List<Object> list = new ArrayList<>();
            try {
                for (int i = 0; i < length; i++) {
                    list.add(ArrowToRecordConverter.getData(dataAccessor,
                            elementTypeInfo, offset + i, isExtension));
                }
                return list;
            } catch (Exception e) {
                throw new RuntimeException("Could not get the array", e);
            }
        }
    }
}
