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

package com.aliyun.odps.table.arrow.accessor;

import org.apache.arrow.vector.complex.MapVector;

/**
 * Arrow column vector accessor for map.
 */
public abstract class ArrowMapAccessor<TYPE_MAP> extends ArrowVectorAccessor {

    protected final MapVector mapVector;

    public ArrowMapAccessor(MapVector vector) {
        super(vector);
        this.mapVector = vector;
    }

    public TYPE_MAP getMap(int rowId) {
        int index = rowId * MapVector.OFFSET_WIDTH;
        int offset = mapVector.getOffsetBuffer().getInt(index);
        int length = mapVector.getInnerValueCountAt(rowId);
        return getMapData(offset, length);
    }

    abstract protected TYPE_MAP getMapData(int offset, int numElements);
}
