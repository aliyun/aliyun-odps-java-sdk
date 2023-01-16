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

package com.aliyun.odps.table.read.split;

import java.io.Serializable;

/**
 * Helper struct to represent a range of row indexes `[startIndex, startIndex + numRecord)`.
 */
public class RowRange implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long startIndex;
    private final long numRecord;

    public static RowRange of(long startIndex, long numRecord) {
        return new RowRange(startIndex, numRecord);
    }

    public RowRange(long startIndex, long numRecord) {
        this.startIndex = startIndex;
        this.numRecord = numRecord;
    }

    public long getNumRecord() {
        return numRecord;
    }

    public long getStartIndex() {
        return startIndex;
    }

    @Override
    public String toString() {
        return String.format("RowRange: [%d, %d)", startIndex, numRecord);
    }
}