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
import org.apache.arrow.vector.complex.StructVector;

/**
 * {@link ArrowFieldWriter} for Struct.
 */
public abstract class ArrowStructWriter<IN, TYPE_STRUCT> extends ArrowFieldWriter<IN> {

    protected ArrowFieldWriter<TYPE_STRUCT>[] fieldsWriters;

    public ArrowStructWriter(StructVector valueVector,
                             ArrowFieldWriter<TYPE_STRUCT>[] fieldsWriters) {
        super(valueVector);
        this.fieldsWriters = Preconditions.checkNotNull(fieldsWriters, "Struct field writer");
    }

    protected abstract TYPE_STRUCT readStruct(IN in, int ordinal);

    @Override
    protected void setValue(IN in, int ordinal) {
        TYPE_STRUCT struct = readStruct(in, ordinal);
        int i = 0;
        ((StructVector) valueVector).setIndexDefined(count);
        while (i < fieldsWriters.length) {
            fieldsWriters[i].write(struct, i);
            i += 1;
        }
    }

    @Override
    protected void setNull() {
        int i = 0;
        while (i < fieldsWriters.length) {
            fieldsWriters[i].setNull();
            fieldsWriters[i].count += 1;
            i += 1;
        }
        ((StructVector) valueVector).setNull(count);
    }

    @Override
    public void finish() {
        super.finish();
        for (ArrowFieldWriter<?> fieldsWriter : fieldsWriters) {
            fieldsWriter.finish();
        }
    }

    @Override
    public void reset() {
        super.reset();
        for (ArrowFieldWriter<?> fieldsWriter : fieldsWriters) {
            fieldsWriter.reset();
        }
    }
}