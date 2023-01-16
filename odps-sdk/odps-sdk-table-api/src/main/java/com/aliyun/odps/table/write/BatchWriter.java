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

package com.aliyun.odps.table.write;

import com.aliyun.odps.table.metrics.Metrics;

import java.io.Closeable;
import java.io.IOException;

/**
 * A batch data writer returned by {@link TableBatchWriteSession} and is
 * responsible for writing data for an data partition.
 */
public interface BatchWriter<T> extends Closeable {

    /**
     * Returns an new element for {@link BatchWriter#write}
     *
     * @return
     */
    T newElement();

    /**
     * Writes one element.
     * <p>
     * If this method fails (by throwing an exception), {@link #abort()} will be called
     *
     * @throws IOException if failure happens during IO.
     */
    void write(T record) throws IOException;

    /**
     * Aborts this writer if it is failed.
     */
    void abort() throws IOException;

    /**
     * Commits this writer after all records are written successfully, returns a commit message which
     * will be sent back to table sink
     */
    WriterCommitMessage commit() throws IOException;

    /**
     * Returns metrics for this writer
     */
    default Metrics currentMetricsValues() {
        return new Metrics();
    }

}
