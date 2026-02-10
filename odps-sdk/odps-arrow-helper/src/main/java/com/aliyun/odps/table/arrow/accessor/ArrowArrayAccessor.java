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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.ListVector;

/**
 * Arrow column vector accessor for array.
 */
public abstract class ArrowArrayAccessor<TYPE_ARRAY> extends ArrowVectorAccessor {

    protected final ListVector listVector;

    public ArrowArrayAccessor(ListVector vector) {
        super(vector);
        this.listVector = vector;
    }

    public TYPE_ARRAY getArray(int rowId) {
        ArrowBuf offsets = listVector.getOffsetBuffer();
        int index = rowId * ListVector.OFFSET_WIDTH;
        int start = offsets.getInt(index);
        int end = offsets.getInt(index + ListVector.OFFSET_WIDTH);
        return getArrayData(start, end - start);
    }

    abstract protected TYPE_ARRAY getArrayData(int offset, int numElements);
}
