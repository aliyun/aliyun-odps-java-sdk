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

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import com.aliyun.odps.table.arrow.accessor.ArrowMapAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.TypeInfo;

public class ArrowMapAccessorImpl {

    public static class ArrowMapAccessorForRecord extends ArrowMapAccessor<Map<Object, Object>> {
        private final boolean isExtension;
        private final TypeInfo keyTypeInfo;
        private final TypeInfo valueTypeInfo;
        private final ArrowVectorAccessor keyAccessor;
        private final ArrowVectorAccessor valueAccessor;

        public ArrowMapAccessorForRecord(MapVector mapVector, TypeInfo typeInfo, boolean isExtension) {
            super(mapVector);
            this.isExtension = isExtension;
            this.keyTypeInfo = ((MapTypeInfo) typeInfo).getKeyTypeInfo();
            this.valueTypeInfo = ((MapTypeInfo) typeInfo).getValueTypeInfo();
            StructVector entries = (StructVector) mapVector.getDataVector();
            this.keyAccessor = ArrowToRecordConverter.createColumnVectorAccessor(
                    entries.getChild(MapVector.KEY_NAME), keyTypeInfo, isExtension);
            this.valueAccessor = ArrowToRecordConverter.createColumnVectorAccessor(
                    entries.getChild(MapVector.VALUE_NAME), valueTypeInfo, isExtension);
        }

        @Override
        protected Map<Object, Object> getMapData(int offset, int numElements) {
            Map<Object, Object> map = new HashMap<>();
            try {
                for (int i = 0; i < numElements; i++) {
                    map.put(ArrowToRecordConverter.getData(keyAccessor, keyTypeInfo, offset + i, isExtension),
                            ArrowToRecordConverter.getData(valueAccessor, valueTypeInfo, offset + i, isExtension));
                }
                return map;
            } catch (Exception e) {
                throw new RuntimeException("Could not get the map", e);
            }
        }
    }
}
