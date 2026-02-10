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

import org.apache.arrow.vector.BitVector;

/**
 * {@link ArrowFieldWriter} for Bit.
 */
public abstract class ArrowBitWriter<IN> extends ArrowFieldWriter<IN> {

    public ArrowBitWriter(BitVector bitVector) {
        super(bitVector);
    }

    @Override
    protected void setNull() {
        ((BitVector) valueVector).setNull(count);
    }

    @Override
    protected void setValue(IN row, int ordinal) {
        ((BitVector) valueVector).setSafe(count, readBoolean(row, ordinal) ? 1 : 0);
    }

    abstract protected boolean readBoolean(IN row, int ordinal);
}
