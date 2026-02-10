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

import com.aliyun.odps.table.utils.Preconditions;
import org.apache.arrow.vector.complex.ListVector;

/**
 * {@link ArrowFieldWriter} for Array.
 */
public abstract class ArrowArrayWriter<IN, TYPE_ARRAY> extends ArrowFieldWriter<IN> {

    protected final ListVector listVector;
    protected ArrowFieldWriter<TYPE_ARRAY> elementWriter;

    public ArrowArrayWriter(ListVector listVector,
                            ArrowFieldWriter<TYPE_ARRAY> elementWriter) {
        super(listVector);
        this.listVector = listVector;
        this.elementWriter = Preconditions.checkNotNull(elementWriter, "Array element writer");
    }

    protected abstract TYPE_ARRAY readArray(IN in, int ordinal);

    protected abstract int numElements(TYPE_ARRAY arrayData);

    @Override
    protected void setValue(IN in, int ordinal) {
        ((ListVector) valueVector).startNewValue(getCount());
        TYPE_ARRAY array = readArray(in, ordinal);
        int size = numElements(array);
        for (int i = 0; i < size; i++) {
            elementWriter.write(array, i);
        }
        ((ListVector) valueVector).endValue(getCount(), size);
    }

    @Override
    protected void setNull() {
    }

    @Override
    public void finish() {
        super.finish();
        elementWriter.finish();
    }

    @Override
    public void reset() {
        super.reset();
        elementWriter.reset();
    }
}
