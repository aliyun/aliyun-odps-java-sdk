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

package com.aliyun.odps.table.read.impl.batch;

import java.io.IOException;

import org.apache.arrow.vector.VectorSchemaRoot;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.record.ColumnarBatchRecord;

public class SplitRecordReaderImpl implements SplitReader<ArrayRecord> {

    private final DataSchema readSchema;
    private final ReaderOptions readerOptions;
    private final SplitReader<VectorSchemaRoot> arrowBatchReader;
    private int rowsInBatch;
    private int nextRow;
    private ColumnarBatchRecord batchRecord;
    private VectorSchemaRoot currentBatchRoot;
    private VectorSchemaRoot pendingBatchRoot;
    private Throwable terminalFailure;

    public SplitRecordReaderImpl(SplitReader<VectorSchemaRoot> arrowReader,
                                 DataSchema schema,
                                 ReaderOptions options) {
        this.rowsInBatch = 0;
        this.nextRow = 0;
        this.arrowBatchReader = arrowReader;
        this.readSchema = schema;
        this.readerOptions = options;
    }

    @Override
    public boolean hasNext() throws IOException {
        throwTerminalFailure();
        if (nextRow < rowsInBatch) {
            return true;
        }
        nextRow = 0;
        boolean moreRows;
        try {
            do {
                moreRows = arrowBatchReader.hasNext();
                if (moreRows) {
                    rowsInBatch = fillRows();
                } else {
                    rowsInBatch = 0;
                }
                // If we filled rows, we can break out of the loop
            } while (moreRows && rowsInBatch == 0);
            return moreRows;
        } catch (IOException | RuntimeException | Error e) {
            terminalFailure = e;
            throw e;
        }
    }

    @Override
    public ArrayRecord get() {
        throwTerminalFailureUnchecked();
        batchRecord.setRowId(this.nextRow++);
        return batchRecord;
    }

    protected int fillRows() {
        releasePendingBatchRoot();
        VectorSchemaRoot root = arrowBatchReader.get();
        if (!readerOptions.isReuseBatch()) {
            pendingBatchRoot = root;
        }
        if (batchRecord == null || !readerOptions.isReuseBatch() || currentBatchRoot != root) {
            if (batchRecord != null && !readerOptions.isReuseBatch()) {
                try {
                    batchRecord.close();
                } catch (RuntimeException | Error e) {
                    closePendingBatchRoot(e);
                    throw e;
                }
                batchRecord = null;
                currentBatchRoot = null;
            }
            try {
                batchRecord = new ColumnarBatchRecord(
                        root, readSchema.getColumns().toArray(new Column[0]));
                currentBatchRoot = root;
                pendingBatchRoot = null;
            } catch (RuntimeException | Error e) {
                if (!readerOptions.isReuseBatch()) {
                    closePendingBatchRoot(e);
                }
                throw e;
            }
        }
        return root.getRowCount();
    }

    @Override
    public void close() throws IOException {
        Throwable failure = null;
        if (batchRecord != null && !readerOptions.isReuseBatch()) {
            try {
                batchRecord.close();
                batchRecord = null;
                currentBatchRoot = null;
            } catch (RuntimeException | Error e) {
                failure = e;
            }
        } else {
            batchRecord = null;
            currentBatchRoot = null;
        }
        failure = closePendingBatchRoot(failure);

        try {
            arrowBatchReader.close();
        } catch (Throwable closeError) {
            if (failure == null) {
                failure = closeError;
            } else if (failure != closeError) {
                failure.addSuppressed(closeError);
            }
        }

        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure != null) {
            throw new IOException("Failed to close record reader", failure);
        }
    }

    private void releasePendingBatchRoot() {
        if (pendingBatchRoot == null) {
            return;
        }
        pendingBatchRoot.close();
        pendingBatchRoot = null;
    }

    private Throwable closePendingBatchRoot(Throwable failure) {
        if (pendingBatchRoot == null) {
            return failure;
        }
        try {
            pendingBatchRoot.close();
            pendingBatchRoot = null;
        } catch (RuntimeException | Error closeError) {
            if (failure == null) {
                failure = closeError;
            } else if (failure != closeError) {
                failure.addSuppressed(closeError);
            }
        }
        return failure;
    }

    private void throwTerminalFailure() throws IOException {
        if (terminalFailure == null) {
            return;
        }
        if (terminalFailure instanceof IOException) {
            throw (IOException) terminalFailure;
        }
        if (terminalFailure instanceof RuntimeException) {
            throw (RuntimeException) terminalFailure;
        }
        if (terminalFailure instanceof Error) {
            throw (Error) terminalFailure;
        }
        throw new IOException("Record reader cannot continue after a prior failure",
                terminalFailure);
    }

    private void throwTerminalFailureUnchecked() {
        if (terminalFailure == null) {
            return;
        }
        if (terminalFailure instanceof RuntimeException) {
            throw (RuntimeException) terminalFailure;
        }
        if (terminalFailure instanceof Error) {
            throw (Error) terminalFailure;
        }
        throw new IllegalStateException(
                "Record reader cannot expose data after a prior failure",
                terminalFailure);
    }

    @Override
    public Metrics currentMetricsValues() {
        return arrowBatchReader.currentMetricsValues();
    }
}
