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

package com.aliyun.odps.table.write.impl.batch;

import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.table.arrow.ArrowWriterFactory;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.table.utils.HttpUtils;
import com.aliyun.odps.table.utils.SchemaUtils;
import com.aliyun.odps.table.utils.TableRetryHandler;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static com.aliyun.odps.table.utils.ConfigConstants.VERSION_1;
import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

public class ArrowWriterImpl implements BatchWriter<VectorSchemaRoot> {

    private static final Logger logger = LoggerFactory.getLogger(ArrowWriterImpl.class.getName());

    private boolean isClosed;
    private final long blockNumber;
    private final WriterOptions writerOptions;
    private final Schema arrowSchema;
    private final String sessionId;
    private final TableIdentifier identifier;
    private final WriterAttemptId attemptId;
    private final boolean enhanceWriteCheck;

    private Connection connection;
    private ArrowWriter batchWriter;
    private WriterCommitMessage commitMessage;

    private Metrics metrics;
    private BytesCount bytesCount;
    private RecordCount recordCount;

    public ArrowWriterImpl(String sessionId,
                           TableIdentifier identifier,
                           DataSchema schema,
                           long blockNumber,
                           WriterAttemptId attemptId,
                           WriterOptions writerOptions,
                           ArrowOptions arrowOptions,
                           boolean enhanceWriteCheck) {
        this.sessionId = sessionId;
        this.identifier = identifier;
        this.blockNumber = blockNumber;
        this.attemptId = attemptId;
        this.writerOptions = writerOptions;
        this.arrowSchema = SchemaUtils.toArrowSchema(schema.getColumns(), arrowOptions);
        this.isClosed = false;
        this.enhanceWriteCheck = enhanceWriteCheck;
        initMetrics();
    }

    @Override
    public VectorSchemaRoot newElement() {
        return VectorSchemaRoot.create(arrowSchema, writerOptions.getBufferAllocator());
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        if (isClosed) {
            throw new IOException("Arrow writer is closed");
        }

        if (batchWriter == null) {
            batchWriter = ArrowWriterFactory.getRecordBatchWriter(
                    openWriterConnection(sessionId, identifier, blockNumber, attemptId),
                    writerOptions);
        }
        try {
            batchWriter.writeBatch(root);
            recordCount.inc(root.getRowCount());
            bytesCount.setValue(batchWriter.bytesWritten());
        } catch (IOException e) {
            Response response = connection.getResponse();
            if (response != null && !response.isOK()) {
                TunnelException exception = new TunnelException(response.getHeader(HEADER_ODPS_REQUEST_ID),
                        connection.getInputStream(),
                        response.getStatus());
                throw new IOException(exception.getMessage(), exception);
            } else {
                throw new IOException("ArrowHttpOutputStream Serialize Exception", e);
            }
        }
    }

    @Override
    public void abort() throws IOException {
        disconnect();
    }

    @Override
    @Nullable
    public WriterCommitMessage commit() throws IOException {
        close();
        return commitMessage;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            try {
                if (batchWriter != null) {
                    batchWriter.close();
                    Response response = connection.getResponse();
                    if (!response.isOK()) {
                        TunnelException exception = new TunnelException(response.getHeader(HEADER_ODPS_REQUEST_ID),
                                connection.getInputStream(),
                                response.getStatus());
                        throw new IOException(exception.getMessage(), exception);
                    } else {
                        commitMessage = new WriterCommitMessageImpl(blockNumber,
                                loadResultFromJson(connection.getInputStream()));
                    }
                }
            } finally {
                disconnect();
                isClosed = true;
            }
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
        this.metrics.register(bytesCount);
        this.metrics.register(recordCount);
    }

    private OutputStream openWriterConnection(String sessionId,
                                              TableIdentifier identifier,
                                              long blockNumber,
                                              WriterAttemptId attemptId)
            throws IOException {
        Map<String, String> headers = new HashMap<>();
        headers.put(Headers.TRANSFER_ENCODING, Headers.CHUNKED);
        headers.put(Headers.CONTENT_TYPE, "application/octet-stream");
        if (writerOptions.getSettings() != null && writerOptions.getSettings().getTags()
                .isPresent()) {
            headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS,
                    String.join(",", writerOptions.getSettings().getTags().get()));
        }
        // TODO: compress

        Map<String, String> params = HttpUtils.createCommonParams(writerOptions.getSettings());
        params.put(ConfigConstants.BLOCK_NUMBER, Long.toString(blockNumber));
        params.put(ConfigConstants.ATTEMPT_NUMBER, Integer.toString(attemptId.getAttemptNumber()));
        params.put(ConfigConstants.DATA_FORMAT_TYPE,
                writerOptions.getDataFormat().getType().toString());
        params.put(ConfigConstants.DATA_FORMAT_VERSION,
                writerOptions.getDataFormat().getVersion().toString());

        String resource = ResourceBuilder.buildTableSessionDataResource(
                VERSION_1,
                identifier.getProject(),
                identifier.getSchema(),
                identifier.getTable(),
                sessionId);

        try {
            RestClient restClient = ExecutionEnvironment.create(writerOptions.getSettings())
                    .createHttpClient(identifier.getProject());
            restClient.setChunkSize(writerOptions.getChunkSize());
            restClient.setRetryLogger(new RestClient.RetryLogger() {
                @Override
                public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
                    logger.warn(String.format("Writer retry for session: %s, " +
                                    "retryCount: %d, will retry in %d seconds.",
                            sessionId, retryCount, retrySleepTime / 1000), e);
                }
            });

            TunnelRetryHandler retryHandler = new TableRetryHandler(restClient);

            return retryHandler.executeWithRetry(() -> {
                try {
                    this.connection = restClient.connect(resource, "POST", params, headers);
                    return connection.getOutputStream();
                } catch (Exception e) {
                    disconnect();
                    throw e;
                }
            });
        } catch (Exception e) {
            disconnect();
            logger.error("Open writer failed", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    private String loadResultFromJson(InputStream is) throws IOException {
        String result = "";
        try {
            String json = IOUtils.readStreamAsString(is);
            JsonObject tree = new JsonParser().parse(json).getAsJsonObject();
            if (tree.has("CommitMessage")) {
                result = tree.get("CommitMessage").getAsString();
                if (enhanceWriteCheck) {
                    checkCommitMessage(result);
                }
            }
        } catch (Exception e) {
            throw new IOException("Parse writer commit response failed", e);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        return result;
    }

    private void disconnect() throws IOException {
        if (connection != null) {
            connection.disconnect();
        }
    }

    private void checkCommitMessage(String message) throws IOException {
        JsonObject tree = new JsonParser().parse(message).getAsJsonObject();

        if (tree.has("BlockNumber")) {
            long blockNumberRes = tree.get("BlockNumber").getAsLong();
            if (blockNumberRes != blockNumber) {
                throw new IOException("Unexpected block number, "
                        + "expected = " + blockNumber + ", but " + blockNumberRes);
            }
        }

        if (tree.has("AttemptNumber")) {
            int attemptNumberRes = tree.get("AttemptNumber").getAsInt();
            if (attemptNumberRes != attemptId.getAttemptNumber()) {
                throw new IOException("Unexpected attempt number, "
                        + "expected = " + attemptId.getAttemptNumber()
                        + ", but " + attemptNumberRes);
            }
        }

        if (tree.has("WriterStats")) {
            JsonObject stats = tree.get("WriterStats").getAsJsonObject();

            if (stats.has("RecordNum")) {
                long recordNumRes = stats.get("RecordNum").getAsLong();
                if (recordNumRes != recordCount.getCount()) {
                    throw new IOException("Unexpected record count, "
                            + "expected = " + recordCount.getCount()
                            + ", but " + recordNumRes);
                }
            }
        }
    }
}
