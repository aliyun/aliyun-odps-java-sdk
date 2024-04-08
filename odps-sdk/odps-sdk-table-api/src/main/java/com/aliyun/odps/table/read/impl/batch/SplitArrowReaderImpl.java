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
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.vector.VectorSchemaRoot;

import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.arrow.ArrowReaderFactory;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitWithIndex;
import com.aliyun.odps.table.read.split.InputSplitWithRowRange;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.table.utils.HttpUtils;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;

public class SplitArrowReaderImpl implements SplitReader<VectorSchemaRoot> {

    private final ArrowReader reader;
    private Connection connection;
    private boolean isClosed;
    private Metrics metrics;
    private BytesCount bytesCount;
    private RecordCount recordCount;

    public SplitArrowReaderImpl(TableIdentifier identifier,
                                InputSplit split,
                                ReaderOptions options) throws IOException {
        openReaderConnection(identifier, split, options);
        initMetrics();
        this.isClosed = false;
        this.reader = ArrowReaderFactory.getRecordBatchReader(connection.getInputStream(), options);
    }

    @Override
    public boolean hasNext() throws IOException {
        return this.reader.nextBatch();
    }

    @Override
    public VectorSchemaRoot get() {
        VectorSchemaRoot root = reader.getCurrentValue();
        recordCount.inc(root.getRowCount());
        bytesCount.setValue(reader.bytesRead());
        return root;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            if (reader != null) {
                reader.close();
            }
            if (connection != null) {
                connection.disconnect();
            }
            isClosed = true;
        }
    }

    @Override
    public Metrics currentMetricsValues() {
        return this.metrics;
    }

    private void initMetrics() {
        this.bytesCount = new BytesCount();
        this.recordCount = new RecordCount();
        this.metrics = new Metrics();
        metrics.register(bytesCount);
        metrics.register(recordCount);
    }

    private void openReaderConnection(TableIdentifier identifier,
                                      InputSplit split,
                                      ReaderOptions options) throws IOException {
        RestClient restClient = ExecutionEnvironment.create(options.getSettings())
                .createHttpClient(identifier.getProject());

        Map<String, String> headers = HttpUtils.createCommonHeader();
        if (options.getCompressionCodec().equals(CompressionCodec.ZSTD)) {
            headers.put(Headers.ACCEPT_ENCODING, CompressionCodec.ZSTD.toString());
        } else if (options.getCompressionCodec().equals(CompressionCodec.LZ4_FRAME)) {
            headers.put(Headers.ACCEPT_ENCODING, CompressionCodec.LZ4_FRAME.toString());
        }

        Map<String, String> params = new HashMap<String, String>();
        params.put(ConfigConstants.SESSION_ID, split.getSessionId());
        if (options.getSettings() != null && options.getSettings().getQuotaName().isPresent()) {
            params.put(TunnelConstants.PARAM_QUOTA_NAME, options.getSettings().getQuotaName().get());
        }

        if (split instanceof InputSplitWithRowRange) {
            InputSplitWithRowRange rowRangeInputSplit = (InputSplitWithRowRange) split;
            params.put(ConfigConstants.ROW_INDEX,
                    String.valueOf(rowRangeInputSplit.getRowRange().getStartIndex()));
            params.put(ConfigConstants.ROW_COUNT,
                    String.valueOf(rowRangeInputSplit.getRowRange().getNumRecord()));
        } else if (split instanceof InputSplitWithIndex) {
            InputSplitWithIndex indexedInputSplit = (InputSplitWithIndex) split;
            params.put(ConfigConstants.SPLIT_INDEX,
                    String.valueOf(indexedInputSplit.getSplitIndex()));
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }

        params.put(ConfigConstants.MAX_BATCH_ROWS,
                String.valueOf(options.getBatchRowCount()));
        if (options.getBatchRawSize() != 0L) {
            params.put(ConfigConstants.MAX_BATCH_RAW_SIZE, String.valueOf(options.getBatchRawSize()));
        }
        params.put(ConfigConstants.DATA_FORMAT_TYPE,
                options.getDataFormat().getType().toString());
        params.put(ConfigConstants.DATA_FORMAT_VERSION,
                options.getDataFormat().getVersion().toString());
        Connection conn;
        try {
            String resource = ResourceBuilder.buildTableDataResource(
                    ConfigConstants.VERSION_1,
                    identifier.getProject(),
                    identifier.getSchema(),
                    identifier.getTable());

            conn = restClient.connect(resource, "GET", params, headers);

            Response resp = conn.getResponse();
            if (!resp.isOK()) {
                TunnelException err = new TunnelException(conn.getInputStream());
                err.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
                throw err;
            }
            this.connection = conn;
        } catch (Exception e) {
            if (connection != null) {
                connection.disconnect();
            }
            throw new IOException(e.getMessage(), e);
        }
    }
}
