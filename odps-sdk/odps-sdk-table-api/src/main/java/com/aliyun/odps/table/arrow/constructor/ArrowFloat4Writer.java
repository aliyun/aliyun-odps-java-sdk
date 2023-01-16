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

import org.apache.arrow.vector.Float4Vector;

/**
 * {@link ArrowFieldWriter} for Float4.
 */
public abstract class ArrowFloat4Writer<IN> extends ArrowFieldWriter<IN> {

    public ArrowFloat4Writer(Float4Vector float4Vector) {
        super(float4Vector);
    }

    @Override
    protected void setNull() {
        ((Float4Vector) valueVector).setNull(count);
    }

    @Override
    protected void setValue(IN row, int ordinal) {
        ((Float4Vector) valueVector).setSafe(count, readFloat(row, ordinal));
    }

    abstract protected float readFloat(IN row, int ordinal);
}
