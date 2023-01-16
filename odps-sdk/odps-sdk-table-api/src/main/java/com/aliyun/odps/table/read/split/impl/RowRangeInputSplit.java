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

package com.aliyun.odps.table.read.split.impl;

import com.aliyun.odps.table.read.split.InputSplitWithRowRange;
import com.aliyun.odps.table.read.split.RowRange;
import com.google.common.base.Preconditions;

public class RowRangeInputSplit implements InputSplitWithRowRange {

    private static final long serialVersionUID = 1L;

    private final String sessionId;

    /**
     * The row range of this split.
     */
    private final RowRange rowRange;

    public RowRangeInputSplit(
            String sessionId,
            long startIndex,
            long numRecord) {
        Preconditions.checkArgument(startIndex >= 0, "startIndex must be >= 0");
        Preconditions.checkArgument(numRecord >= 0, "numRecord must be >= 0");
        this.sessionId = Preconditions.checkNotNull(sessionId);
        this.rowRange = RowRange.of(startIndex, numRecord);
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public RowRange getRowRange() {
        return rowRange;
    }

    @Override
    public int hashCode() {
        return this.rowRange.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof RowRangeInputSplit) {
            RowRangeInputSplit other = (RowRangeInputSplit) obj;
            return other.rowRange.equals(this.rowRange) &&
                    other.sessionId.equals(this.sessionId);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("RowRangeInputSplit: SessionId=%s RowRange=%s",
                sessionId, rowRange.toString());
    }
}
