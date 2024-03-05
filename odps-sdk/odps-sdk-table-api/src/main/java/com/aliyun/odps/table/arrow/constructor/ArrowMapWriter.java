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

package com.aliyun.odps.table.arrow.constructor;

import com.google.common.base.Preconditions;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

/**
 * {@link ArrowFieldWriter} for Map.
 */
public abstract class ArrowMapWriter<IN, TYPE_MAP, TYPE_KEY_ARRAY, TYPE_VALUE_ARRAY> extends ArrowFieldWriter<IN> {

    protected StructVector structVector;
    protected ArrowFieldWriter<TYPE_KEY_ARRAY> keyWriter;
    protected ArrowFieldWriter<TYPE_VALUE_ARRAY> valueWriter;

    public ArrowMapWriter(MapVector mapVector,
                          ArrowFieldWriter<TYPE_KEY_ARRAY> keyWriter,
                          ArrowFieldWriter<TYPE_VALUE_ARRAY> valueWriter) {
        super(mapVector);
        this.structVector = (StructVector) mapVector.getDataVector();
        this.keyWriter = Preconditions.checkNotNull(keyWriter);
        this.valueWriter = Preconditions.checkNotNull(valueWriter);
    }

    protected abstract TYPE_MAP readMap(IN in, int ordinal);

    protected abstract TYPE_KEY_ARRAY readKeyArray(TYPE_MAP mapData);

    protected abstract TYPE_VALUE_ARRAY readValueArray(TYPE_MAP mapData);

    protected abstract int numElements(TYPE_MAP arrayData);

    @Override
    protected void setValue(IN in, int ordinal) {
        ((MapVector) valueVector).startNewValue(getCount());
        TYPE_MAP map = readMap(in, ordinal);
        TYPE_KEY_ARRAY keyArray = readKeyArray(map);
        TYPE_VALUE_ARRAY valueArray = readValueArray(map);
        int mapSize = numElements(map);
        for (int i = 0; i < mapSize; i++) {
            structVector.setIndexDefined(keyWriter.getCount());
            keyWriter.write(keyArray, i);
            valueWriter.write(valueArray, i);
        }
        ((MapVector) valueVector).endValue(getCount(), mapSize);
    }

    @Override
    protected void setNull() {
    }

    @Override
    public void finish() {
        super.finish();
        keyWriter.finish();
        valueWriter.finish();
    }

    @Override
    public void reset() {
        super.reset();
        keyWriter.reset();
        valueWriter.reset();
    }
}