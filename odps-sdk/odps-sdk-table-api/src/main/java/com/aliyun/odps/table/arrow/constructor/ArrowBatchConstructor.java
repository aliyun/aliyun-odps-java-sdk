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
import org.apache.arrow.vector.VectorSchemaRoot;

public class ArrowBatchConstructor<IN> {

    private final VectorSchemaRoot root;
    private final ArrowFieldWriter<IN>[] fieldWriters;

    public ArrowBatchConstructor(VectorSchemaRoot root, ArrowFieldWriter<IN>[] fieldWriters) {
        this.root = Preconditions.checkNotNull(root, "Vector schema root");
        this.fieldWriters = Preconditions.checkNotNull(fieldWriters, "Field writers");
    }

    public void write(IN row) {
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(row, i);
        }
    }

    public void finish() {
        root.setRowCount(fieldWriters[0].getCount());
        for (ArrowFieldWriter<IN> fieldWriter : fieldWriters) {
            fieldWriter.finish();
        }
    }

    public void reset() {
        root.setRowCount(0);
        for (ArrowFieldWriter<IN> fieldWriter : fieldWriters) {
            fieldWriter.reset();
        }
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return root;
    }
}
