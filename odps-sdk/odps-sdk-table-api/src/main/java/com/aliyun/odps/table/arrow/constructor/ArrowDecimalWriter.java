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

import org.apache.arrow.vector.DecimalVector;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * {@link ArrowFieldWriter} for Decimal.
 */
public abstract class ArrowDecimalWriter<IN> extends ArrowFieldWriter<IN> {

    protected final int precision;
    protected final int scale;

    public ArrowDecimalWriter(DecimalVector decimalVector, int precision, int scale) {
        super(decimalVector);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    protected void setNull() {
        ((DecimalVector) valueVector).setNull(count);
    }

    @Override
    protected void setValue(IN row, int ordinal) {
        ((DecimalVector) valueVector).setSafe(count,
                readBigDecimal(row, ordinal).setScale(scale, RoundingMode.HALF_UP));
    }

    abstract protected BigDecimal readBigDecimal(IN row, int ordinal);
}
