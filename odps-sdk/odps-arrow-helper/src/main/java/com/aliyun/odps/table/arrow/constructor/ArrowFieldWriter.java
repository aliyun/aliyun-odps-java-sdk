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
import org.apache.arrow.vector.ValueVector;

public abstract class ArrowFieldWriter<IN> {

    protected final ValueVector valueVector;
    protected int count = 0;

    public ArrowFieldWriter(ValueVector valueVector) {
        this.valueVector = Preconditions.checkNotNull(valueVector, "Value vector");
    }

    public int getCount() {
        return count;
    }

    public String getName() {
        return valueVector.getField().getName();
    }

    public void write(IN row, int ordinal) {
        if (isNullAt(row, ordinal)) {
            setNull();
        } else {
            setValue(row, ordinal);
        }
        count += 1;
    }

    public void finish() {
        valueVector.setValueCount(count);
    }

    public void reset() {
        valueVector.reset();
        count = 0;
    }

    protected abstract boolean isNullAt(IN row, int ordinal);

    protected abstract void setValue(IN row, int ordinal);

    protected abstract void setNull();
}
