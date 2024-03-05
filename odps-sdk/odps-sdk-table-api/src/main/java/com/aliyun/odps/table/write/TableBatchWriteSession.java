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

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.SessionType;
import com.aliyun.odps.table.configuration.WriterOptions;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

/**
 * An interface that defines how to write the data to odps for batch processing.
 */
public interface TableBatchWriteSession extends TableWriteSession {

    /**
     * Commits this writing job with a list of commit messages. The commit messages are collected from
     * successful data writers and are produced by {@link BatchWriter#commit()}.
     * TODO: commit with settings
     */
    void commit(WriterCommitMessage[] messages) throws IOException;

    /**
     * Clean up this writing job because some data writers are failed and keep failing when retry,
     */
    void cleanup();

    /**
     * Returns a record data writer to do the actual writing work. Within the same table sink,
     * no two writer will share the same writer id).
     */
    default BatchWriter<ArrayRecord> createRecordWriter(long blockNumber,
                                                        WriterAttemptId attemptId,
                                                        WriterOptions options) throws IOException {
        throw new UnsupportedOperationException("Cannot create record writer.");
    }

    /**
     * Returns a arrow data writer to do the actual writing work. Within the same table sink,
     * no two writer will share the same writer id).
     */
    default BatchWriter<VectorSchemaRoot> createArrowWriter(long blockNumber,
                                                            WriterAttemptId attemptId,
                                                            WriterOptions options) throws IOException {
        throw new UnsupportedOperationException("Cannot create arrow reader.");
    }

    @Override
    default SessionType getType() {
        return SessionType.BATCH_WRITE;
    }
}
