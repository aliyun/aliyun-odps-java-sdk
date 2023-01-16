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

import org.apache.arrow.vector.BigIntVector;

/**
 * {@link ArrowFieldWriter} for Bigint.
 */
public abstract class ArrowBigIntWriter<IN> extends ArrowFieldWriter<IN> {

    public ArrowBigIntWriter(BigIntVector bigIntVector) {
        super(bigIntVector);
    }

    @Override
    protected void setNull() {
        ((BigIntVector) valueVector).setNull(count);
    }

    @Override
    protected void setValue(IN row, int ordinal) {
        ((BigIntVector) valueVector).setSafe(count, readLong(row, ordinal));
    }

    abstract protected long readLong(IN row, int ordinal);
}
