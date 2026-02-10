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

import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Arrow column vector accessor for timestamp.
 */
public class ArrowTimestampAccessor extends ArrowVectorAccessor {

    protected final TimeStampVector timeStampVector;

    public ArrowTimestampAccessor(TimeStampVector timeStampVector) {
        super(timeStampVector);
        this.timeStampVector = timeStampVector;
    }

    public long getEpochTime(int rowId) {
        return this.timeStampVector.get(rowId);
    }

    public ArrowType.Timestamp getType() {
        return (ArrowType.Timestamp) timeStampVector.getField().getType();
    }
}
