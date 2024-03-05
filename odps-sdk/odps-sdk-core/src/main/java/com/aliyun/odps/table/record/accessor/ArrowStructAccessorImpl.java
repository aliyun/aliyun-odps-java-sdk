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

import org.apache.arrow.vector.complex.StructVector;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.table.arrow.accessor.ArrowStructAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;

public class ArrowStructAccessorImpl {

    public static class ArrowStructAccessorForRecord extends ArrowStructAccessor<Struct> {

        private final boolean isExtension;
        private final ArrowVectorAccessor[] childAccessors;
        private final TypeInfo structTypeInfo;
        private final List<TypeInfo> childTypeInfos;

        public ArrowStructAccessorForRecord(StructVector structVector,
                                            TypeInfo typeInfo, boolean isExtension) {
            super(structVector);
            this.isExtension = isExtension;
            this.structTypeInfo = typeInfo;
            this.childTypeInfos = ((StructTypeInfo) typeInfo).getFieldTypeInfos();
            this.childAccessors = new ArrowVectorAccessor[structVector.size()];
            for (int i = 0; i < childAccessors.length; i++) {
                this.childAccessors[i] = ArrowToRecordConverter.createColumnVectorAccessor(
                        structVector.getVectorById(i), childTypeInfos.get(i), isExtension);
            }
        }

        @Override
        public Struct getStruct(int rowId) {
            List<Object> values = new ArrayList<>();
            try {
                for (int i = 0; i < childAccessors.length; i++) {
                    values.add(ArrowToRecordConverter.getData(childAccessors[i], childTypeInfos.get(i), rowId, isExtension));
                }
                return new SimpleStruct((StructTypeInfo) structTypeInfo, values);
            } catch (Exception e) {
                throw new RuntimeException("Could not get the struct", e);
            }
        }
    }
}
