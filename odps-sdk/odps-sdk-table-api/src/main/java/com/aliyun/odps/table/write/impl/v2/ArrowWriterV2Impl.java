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

package com.aliyun.odps.table.write.impl.v2;

import java.io.IOException;

import com.aliyun.odps.storage.write.BlockWriteResult;
import com.aliyun.odps.storage.write.TableBlockWriter;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.impl.v2.V2ClientHolder;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import javax.annotation.Nullable;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Table-api batch writer delegating to the Storage API v2 SDK's {@link TableBlockWriter}
 * (created via {@code TableWriteSession.createBlockWriter(blockNumber, attemptNumber)}),
 * which handles quota reservation and the {@code TableWrite} block/attempt data plane.
 *
 * <p>The SDK block writer streams batches with bounded buffering. A failed attempt is
 * retried by creating a new writer with the next attempt number, matching v1 semantics.
 */
class ArrowWriterV2Impl implements BatchWriter<VectorSchemaRoot> {

    private final TableBlockWriter delegate;

    private final V2ClientHolder ownedClient;

    private IOException failure;

    private final long blockNumber;

    private final WriterAttemptId attemptId;

    private final boolean enhanceWriteCheck;

    private long recordCount;

    private boolean closed;

    private boolean aborted;

    private BlockWriteResult result;

    private Metrics metrics;

    private BytesCount bytesCount;

    private RecordCount recordCounter;

    ArrowWriterV2Impl(TableBlockWriter delegate, TableIdentifier identifier, String sessionId,
                      long blockNumber, WriterAttemptId attemptId, WriterOptions writerOptions,
                      DataSchema schema, boolean enhanceWriteCheck, V2ClientHolder ownedClient) {
        this.ownedClient = ownedClient;
        this.delegate = delegate;
        Preconditions.checkArgument(delegate.getBlockNumber() == blockNumber,
                "Block writer mismatch: requested block " + blockNumber + ", got "
                        + delegate.getBlockNumber());
        Preconditions.checkArgument(delegate.getAttemptNumber() == attemptId.getAttemptNumber(),
                "Block writer mismatch: requested attempt " + attemptId.getAttemptNumber()
                        + ", got " + delegate.getAttemptNumber());
        this.blockNumber = blockNumber;
        this.attemptId = attemptId;
        this.enhanceWriteCheck = enhanceWriteCheck;
        initMetrics();
    }

    private void initMetrics() {
        this.bytesCount = new BytesCount();
        this.recordCounter = new RecordCount();
        this.metrics = new Metrics();
        metrics.register(bytesCount);
        metrics.register(recordCounter);
    }

    @Override
    public VectorSchemaRoot newElement() {
        return delegate.createVectorSchemaRoot();
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        if (closed || aborted) {
            throw new IOException("Arrow writer is closed");
        }
        try {
            delegate.writeBatch(root);
            recordCount += root.getRowCount();
            recordCounter.inc(root.getRowCount());
            bytesCount.setValue(delegate.bytesWritten());
        } catch (IOException e) {
            failure = e;
            abort();
            throw e;
        }
    }

    @Override
    @Nullable
    public WriterCommitMessage commit() throws IOException {
        close();
        if (result == null) {
            throw new IOException("Block writer was aborted and cannot be committed");
        }
        return result.isEmpty() ? null : new WriterCommitMessageV2Impl(result);
    }

    @Override
    public void abort() throws IOException {
        aborted = true;
        delegate.abort();
        closed = true;
        closeOwnedClient();
    }

    @Override
    public void flush() throws IOException {
        try {
            delegate.flush();
            bytesCount.setValue(delegate.bytesWritten());
        } catch (IOException e) {
            failure = e;
            abort();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        if (failure != null) {
            throw failure;
        }
        if (closed) {
            return;
        }
        if (aborted) {
            closed = true;
            return;
        }
        try {
            this.result = delegate.commit();
            bytesCount.setValue(delegate.bytesWritten());
            if (enhanceWriteCheck && !result.isEmpty()) {
                V2CommitMessages.check(result.getCommitMessage(), (int) blockNumber,
                        attemptId.getAttemptNumber());
            }
        } catch (IOException e) {
            failure = e;
            throw e;
        } finally {
            closed = true;
            closeOwnedClient();
        }
    }

    private void closeOwnedClient() {
        if (ownedClient != null) {
            ownedClient.close();
        }
    }

    @Override
    public Metrics currentMetricsValues() {
        return metrics;
    }
}
