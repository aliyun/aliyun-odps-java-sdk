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

package com.aliyun.odps.table.read;

import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.read.split.InputSplit;

import java.io.Closeable;
import java.io.IOException;

/**
 * A split reader returned by {@link TableBatchReadSession#createRecordReader(InputSplit, ReaderOptions)}
 * or {@link TableBatchReadSession#createArrowReader(InputSplit, ReaderOptions)}
 * It's responsible for outputting data for an input split.
 */
public interface SplitReader<T> extends Closeable {

    /**
     * Proceed to next record, returns false if there is no more records.
     *
     * @throws IOException if failure happens during network IO.
     */
    boolean hasNext() throws IOException;

    /**
     * Return the current record. This method should return same value until `hasNext` is called.
     */
    T get();

    /**
     * Returns metrics for this reader
     */
    default Metrics currentMetricsValues() {
        return new Metrics();
    }

}
