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

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.record.ColumnarBatchRecord;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

public class SplitRecordReaderImpl implements SplitReader<ArrayRecord> {

    private final DataSchema readSchema;
    private final ReaderOptions readerOptions;
    private final SplitReader<VectorSchemaRoot> arrowBatchReader;
    private int rowsInBatch;
    private int nextRow;
    private ColumnarBatchRecord batchRecord;

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
        if (nextRow >= rowsInBatch) {
            boolean moreRows = arrowBatchReader.hasNext();
            if (moreRows) {
                nextRow = 0;
                rowsInBatch = fillRows();
            }
            return moreRows;
        }
        return true;
    }

    @Override
    public ArrayRecord get() {
        batchRecord.setRowId(this.nextRow++);
        return batchRecord;
    }

    protected int fillRows() {
        VectorSchemaRoot root = arrowBatchReader.get();
        if (batchRecord == null || !readerOptions.isReuseBatch()) {
            if (batchRecord != null) {
                batchRecord.close();
            }
            batchRecord = new ColumnarBatchRecord(root,
                    readSchema.getColumns().toArray(new Column[0]));
        }
        return root.getRowCount();
    }

    @Override
    public void close() throws IOException {
        arrowBatchReader.close();

        if (batchRecord != null) {
            batchRecord.close();
            batchRecord = null;
        }
    }

    @Override
    public Metrics currentMetricsValues() {
        return arrowBatchReader.currentMetricsValues();
    }
}