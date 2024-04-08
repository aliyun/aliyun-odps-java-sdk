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

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplitAssigner;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.table.utils.HttpUtils;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.utils.SchemaUtils;
import com.aliyun.odps.table.utils.SessionUtils;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

public class TableBatchReadSessionImpl extends TableBatchReadSessionBase {

    private static final Logger logger = LoggerFactory.getLogger(TableBatchReadSessionImpl.class.getName());

    private transient RestClient restClient;

    public TableBatchReadSessionImpl(TableIdentifier identifier,
                                     String sessionId,
                                     EnvironmentSettings settings) throws IOException {
        super(identifier, sessionId, settings);
    }

    public TableBatchReadSessionImpl(TableIdentifier identifier,
                                     List<PartitionSpec> requiredPartitions,
                                     List<String> requiredDataColumns,
                                     List<String> requiredPartitionColumns,
                                     List<Integer> bucketIds,
                                     SplitOptions splitOptions,
                                     ArrowOptions arrowOptions,
                                     EnvironmentSettings settings) throws IOException {
        super(identifier, requiredPartitions, requiredDataColumns,
                requiredPartitionColumns, bucketIds, splitOptions, arrowOptions, settings);
    }

    @Override
    public SplitReader<VectorSchemaRoot> createArrowReader(InputSplit split, ReaderOptions options) throws IOException {
        Preconditions.checkNotNull(identifier, "Table read identifier");
        Preconditions.checkNotNull(split, "Input split");
        Preconditions.checkNotNull(options, "Reader options");
        return new SplitArrowReaderImpl(identifier, split, options);
    }

    @Override
    public SplitReader<ArrayRecord> createRecordReader(InputSplit split, ReaderOptions options) throws IOException {
        SplitReader<VectorSchemaRoot> arrowReader = createArrowReader(split, options);
        return new SplitRecordReaderImpl(arrowReader, readSchema, options);
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        if (supportDataFormats != null) {
            return supportDataFormats.contains(dataFormat);
        }
        return false;
    }

    @Override
    protected void planInputSplits() throws IOException {
        ensureClientInitialized();

        Map<String, String> headers = HttpUtils.createCommonHeader();
        headers.put(Headers.CONTENT_TYPE, "application/json");

        Map<String, String> params = new HashMap<>();
        params.put(ConfigConstants.SESSION_TYPE, getType().toString());
        if (settings != null && settings.getQuotaName().isPresent()) {
            params.put(TunnelConstants.PARAM_QUOTA_NAME, settings.getQuotaName().get());
        }

        try {
            String request = generateReadSessionRequest();
            logger.debug(String.format("Read table '%s'.\n"
                    + "Session request:\n"
                    + "%s", identifier.toString(), request));

            Response resp = restClient.stringRequest(
                    ResourceBuilder.buildTableSessionResource(
                            ConfigConstants.VERSION_1,
                            identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            null),
                    "POST", params, headers, request);
            String response;
            if (resp.isOK()) {
                response = new String(resp.getBody());
                loadResultFromJson(response);
            } else {
                throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID),
                        new ByteArrayInputStream(resp.getBody()), resp.getStatus());
            }

            if (sessionStatus != SessionStatus.NORMAL) {
                long asyncIntervalInMills = HttpUtils.getAsyncIntervalInMills(settings);
                long asyncTimeoutInMills = HttpUtils.getAsyncTimeoutInSeconds(settings) * 1000L;
                long startTime = System.currentTimeMillis();

                while (sessionStatus == SessionStatus.INIT) {
                    Thread.sleep(asyncIntervalInMills);

                    logger.trace(String.format("Async read table: '%s', session id: %s",
                            identifier.toString(), sessionId));

                    response = reloadInputSplits();

                    if (System.currentTimeMillis() - startTime >= asyncTimeoutInMills) {
                        throw new IOException(
                                String.format(
                                        "Create table read session timeout.\n"
                                                + "Table identifier: %s.\n"
                                                + "Session status: %s.\n"
                                                + "Session id: %s.\n"
                                                + "Error message: %s.",
                                        identifier.toString(),
                                        sessionStatus,
                                        sessionId,
                                        errorMessage));
                    }
                }
            }

            if (sessionStatus != SessionStatus.NORMAL) {
                throw new IOException(
                        String.format(
                                "Create table read session failed.\n"
                                        + "Table identifier: %s.\n"
                                        + "Session status: %s.\n"
                                        + "Session id: %s.\n"
                                        + "Error message: %s.",
                                identifier.toString(),
                                sessionStatus,
                                sessionId,
                                errorMessage));
            } else {
                logger.debug(String.format("Read table '%s'.\n"
                        + "Session response:\n"
                        + "%s", identifier.toString(), response));
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        } finally {
            // nothing
        }
    }

    @Override
    protected String reloadInputSplits() throws IOException {
        ensureClientInitialized();

        Preconditions.checkString(sessionId, "Table read session id");

        Map<String, String> headers = HttpUtils.createCommonHeader();

        Map<String, String> params = new HashMap<>();
        params.put(ConfigConstants.SESSION_TYPE, getType().toString());
        if (settings != null && settings.getQuotaName().isPresent()) {
            params.put(TunnelConstants.PARAM_QUOTA_NAME, settings.getQuotaName().get());
        }

        Connection conn = null;
        try {
            conn = restClient.connect(ResourceBuilder.buildTableSessionResource(
                    ConfigConstants.VERSION_1,
                    identifier.getProject(),
                    identifier.getSchema(),
                    identifier.getTable(),
                    sessionId),
                    "GET", params, headers);

            Response resp = conn.getResponse();

            if (resp.isOK()) {
                String response = IOUtils.readStreamAsString(conn.getInputStream());
                loadResultFromJson(response);
                return response;
            } else {
                throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID),
                        conn.getInputStream(), resp.getStatus());
            }
        } catch (IOException e) {
            throw new IOException("Failed to reload table read session with endpoint: "
                    + restClient.getEndpoint(), e);
        } catch (OdpsException e) {
            throw new IOException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private void ensureClientInitialized() {
        if (this.restClient == null) {
            this.restClient = ExecutionEnvironment.create(settings)
                    .createHttpClient(identifier.getProject());
        }
    }

    private String generateReadSessionRequest() {
        JsonObject request = new JsonObject();

        JsonArray dataColumns = new JsonArray();
        requiredDataColumns.stream().map(JsonPrimitive::new).forEach(dataColumns::add);
        request.add("RequiredDataColumns", dataColumns);

        JsonArray partitionColumns = new JsonArray();
        requiredPartitionColumns.stream().map(JsonPrimitive::new).forEach(partitionColumns::add);
        request.add("RequiredPartitionColumns", partitionColumns);

        JsonArray partitionFilters = new JsonArray();
        requiredPartitions.stream()
                .map(partitionSpec -> partitionSpec.toString(false, true))
                .map(JsonPrimitive::new)
                .forEach(partitionFilters::add);
        request.add("RequiredPartitions", partitionFilters);

        JsonArray bucketIds = new JsonArray();
        requiredBucketIds.stream().map(JsonPrimitive::new).forEach(bucketIds::add);
        request.add("RequiredBucketIds", bucketIds);

        JsonObject jsonSplitOptions = new JsonObject();
        jsonSplitOptions.addProperty("SplitMode", splitOptions.getSplitMode().toString());
        jsonSplitOptions.addProperty("SplitNumber", splitOptions.getSplitNumber());
        jsonSplitOptions.addProperty("CrossPartition", splitOptions.isCrossPartition());
        request.add("SplitOptions", jsonSplitOptions);
        request.addProperty("SplitMaxFileNum", splitOptions.getSplitMaxFileNum());

        JsonObject jsonArrowOptions = new JsonObject();
        jsonArrowOptions.addProperty("TimestampUnit", arrowOptions.getTimestampUnit().toString());
        jsonArrowOptions.addProperty("DatetimeUnit", arrowOptions.getDateTimeUnit().toString());
        request.add("ArrowOptions", jsonArrowOptions);

        // TODO: FilterPredicate
        request.add("FilterPredicate", new JsonPrimitive(""));

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(request);
    }

    private void loadResultFromJson(String json) throws TunnelException {
        try {
            JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

            // session id
            if (tree.has("SessionId")) {
                sessionId = tree.get("SessionId").getAsString();
            }

            // ExpirationTime
            if (tree.has("ExpirationTime")) {
                expirationTime = tree.get("ExpirationTime").getAsLong();
            }

            if (tree.has("SessionType")) {
                String sessionType = tree.get("SessionType").getAsString();
                if (!getType().toString().equals(sessionType.toLowerCase())) {
                    throw new UnsupportedOperationException("Unsupported session type: " + sessionType);
                }
            }

            // status
            if (tree.has("SessionStatus")) {
                String status = tree.get("SessionStatus").getAsString().toUpperCase();
                sessionStatus = SessionStatus.valueOf(status);
            }

            // error message
            if (tree.has("Message")) {
                errorMessage = tree.get("Message").getAsString();
            }

            // schema
            if (tree.has("DataSchema")) {
                JsonObject dataSchema = tree.get("DataSchema").getAsJsonObject();
                List<Column> schemaColumns = new ArrayList<>();
                List<String> partitionKeys = new ArrayList<>();
                if (dataSchema.has("DataColumns")) {
                    JsonArray dataColumns = dataSchema.get("DataColumns").getAsJsonArray();
                    for (int i = 0; i < dataColumns.size(); ++i) {
                        JsonObject column = dataColumns.get(i).getAsJsonObject();
                        schemaColumns.add(SchemaUtils.parseColumn(column));
                    }
                }

                if (dataSchema.has("PartitionColumns")) {
                    JsonArray partitionColumns = dataSchema.get("PartitionColumns").getAsJsonArray();
                    for (int i = 0; i < partitionColumns.size(); ++i) {
                        JsonObject column = partitionColumns.get(i).getAsJsonObject();
                        Column partitionCol = SchemaUtils.parseColumn(column);
                        schemaColumns.add(partitionCol);
                        partitionKeys.add(partitionCol.getName());
                    }
                }

                readSchema = DataSchema.newBuilder()
                        .columns(schemaColumns)
                        .partitionBy(partitionKeys)
                        .build();
            }

            // data format
            if (tree.has("SupportedDataFormat")) {
                supportDataFormats = new HashSet<>();
                JsonArray formats = tree.get("SupportedDataFormat").getAsJsonArray();
                formats.forEach(format -> supportDataFormats.add(
                        SessionUtils.parseDataFormat(format.getAsJsonObject())));
            }

            // record count
            if (tree.has("RecordCount")) {
                long recordCount = tree.get("RecordCount").getAsLong();
                if (recordCount >= 0
                        && splitOptions.getSplitMode().equals(SplitOptions.SplitMode.ROW_OFFSET)) {
                    inputSplitAssigner = new RowRangeInputSplitAssigner(sessionId, recordCount);
                }
            }

            // splits count
            if (tree.has("SplitsCount")) {
                int splitsCount = tree.get("SplitsCount").getAsInt();
                if (splitsCount >= 0) {
                    if (splitOptions.getSplitMode().equals(SplitOptions.SplitMode.BUCKET)) {
                        // TODO: for BucketInputSplits
                    } else {
                        inputSplitAssigner = new IndexedInputSplitAssigner(sessionId, splitsCount);
                    }
                }
            }
        } catch (Exception e) {
            throw new TunnelException("Invalid session response: \n" + json, e);
        }
    }
}
