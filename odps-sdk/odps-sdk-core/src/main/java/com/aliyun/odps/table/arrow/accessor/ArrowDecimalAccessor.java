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

import java.math.BigDecimal;

import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;

/**
 * Arrow column vector accessor for decimal.
 */
public class ArrowDecimalAccessor extends ArrowVectorAccessor {

    protected final DecimalVector decimalVector;

    public ArrowDecimalAccessor(DecimalVector decimalVector) {
        super(decimalVector);
        this.decimalVector = decimalVector;
    }

    public BigDecimal getDecimal(int rowId) {
        return decimalVector.getObject(rowId);
    }
}