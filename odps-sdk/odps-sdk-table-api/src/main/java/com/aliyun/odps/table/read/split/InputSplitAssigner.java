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
 * An input split assigner distributes the {@link InputSplit} within a table read session.
 * Each {@link InputSplit} represents a data split that can be processed by one Split reader
 */
public interface InputSplitAssigner extends Serializable {

    /**
     * Returns the number of splits.
     */
    default int getSplitsCount() {
        throw new UnsupportedOperationException("Get splits count not supported");
    }

    /**
     * Return the split by index.
     */
    default InputSplit getSplit(int index) {
        throw new UnsupportedOperationException("Get get input split by index not supported");
    }

    /**
     * Return all input splits of the input split assigner.
     */
    default InputSplit[] getAllSplits() {
        throw new UnsupportedOperationException("Get all splits not supported");
    }

    /**
     * Returns the number of rows.
     */
    default long getTotalRowCount() {
        throw new UnsupportedOperationException("Get total row count not supported");
    }

    /**
     * Returns the split by row offset.
     */
    default InputSplit getSplitByRowOffset(long startIndex, long numRecord) {
        throw new UnsupportedOperationException("Get get input split by row offset not supported");
    }
}
