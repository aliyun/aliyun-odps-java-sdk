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
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.table.read.SessionStats;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.TableSnapshotSpec;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.BucketInputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplitAssigner;
import com.aliyun.odps.table.utils.ConfigConstants;
import com.aliyun.odps.table.utils.HttpUtils;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.utils.SchemaUtils;
import com.aliyun.odps.table.utils.SessionUtils;
import com.aliyun.odps.table.utils.TableRetryHandler;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import com.aliyun.odps.utils.JsonUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

public class TableBatchReadSessionImpl extends TableBatchReadSessionBase {

    private static final Logger logger = LoggerFactory.getLogger(TableBatchReadSessionImpl.class.getName());

    private transient RestClient restClient;
    private transient TunnelRetryHandler retryHandler;

    private transient String details;

    public TableBatchReadSessionImpl(TableReadSessionBuilder builder) throws IOException {
        super(builder);
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

        Map<String, String> headers = HttpUtils.createCommonHeader(settings);
        headers.put(Headers.CONTENT_TYPE, "application/json");

        Map<String, String> params = HttpUtils.createCommonParams(settings);
        params.put(ConfigConstants.SESSION_TYPE, getType().toString());

        try {
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            JsonObject request = generateReadSessionRequest();
            String requestStr = gson.toJson(request);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Read table '%s'.\n"
                        + "Session request:\n"
                        + "%s", identifier.toString(), requestStr));
            }

            String response = retryHandler.executeWithRetry(() -> {
                Response resp = restClient.stringRequest(
                        ResourceBuilder.buildTableSessionResource(
                                ConfigConstants.VERSION_1,
                                identifier.getProject(),
                                identifier.getSchema(),
                                identifier.getTable(),
                                null),
                        "POST", params, headers, requestStr);
                String body;
                if (resp.isOK()) {
                    body = new String(resp.getBody());
                    loadResultFromJson(parseResponse(body));
                    return body;
                } else {
                    throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID),
                            new ByteArrayInputStream(resp.getBody()), resp.getStatus());
                }
            });

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
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Read table '%s'.\n"
                            + "Session response:\n"
                            + "%s", identifier.toString(), response));
                }
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

        Map<String, String> headers = HttpUtils.createCommonHeader(settings);

        Map<String, String> params = HttpUtils.createCommonParams(settings);
        params.put(ConfigConstants.SESSION_TYPE, getType().toString());
        params.put(ConfigConstants.SESSION_REFRESH, Boolean.toString(sessionRefresh));

        try {
            Response resp = restClient.request(
                    ResourceBuilder.buildTableSessionResource(
                            ConfigConstants.VERSION_1,
                            identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            sessionId),
                    "GET", params, headers, null);
            if (resp.isOK()) {
                String response = new String(resp.getBody());
                loadResultFromJson(parseResponse(response));
                return response;
            } else {
                throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID),
                        new ByteArrayInputStream(resp.getBody()), resp.getStatus());
            }
        } catch (Exception e) {
            throw new IOException("Failed to reload table read session with endpoint: "
                    + restClient.getEndpoint(), e);
        } finally {
            // nothing
        }
    }

    private void ensureClientInitialized() {
        if (this.restClient == null) {
            this.restClient = ExecutionEnvironment.create(settings)
                    .createHttpClient(identifier.getProject());
            this.restClient.setRetryLogger(new RestClient.RetryLogger() {
                @Override
                public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
                    logger.warn(String.format("Start retry for table read: %s, " +
                                    "retryCount: %d, will retry in %d seconds.",
                            identifier.toString(), retryCount, retrySleepTime / 1000), e);
                }
            });
        }

        if (this.retryHandler == null) {
            this.retryHandler = new TableRetryHandler(restClient);
        }
    }

    private JsonObject parseResponse(String resp) throws TunnelException {
        try {
            this.details = resp;
            return new JsonParser().parse(resp).getAsJsonObject();
        } catch (Exception e) {
            throw new TunnelException("Invalid session response: \n" + resp, e);
        }
    }

    protected JsonObject generateReadSessionRequest() {
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

        request.add("FilterPredicate", new JsonPrimitive(filterPredicate.toString()));
        request.add("EnableEstimateStats", new JsonPrimitive(enableEstimateStats));
        request.add("FilterPredicateFallback", new JsonPrimitive(allowFilterPredicateFallback));
        request.add("EnableExtendedArrowIPC", new JsonPrimitive(enableExtendedArrowIPC));

        return request;
    }

    protected void loadResultFromJson(JsonObject tree) {
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
            List<String> systemColumnKeys = new ArrayList<>();
            List<String> partitionKeys = new ArrayList<>();
            if (dataSchema.has("DataColumns")) {
                JsonArray dataColumns = dataSchema.get("DataColumns").getAsJsonArray();
                for (int i = 0; i < dataColumns.size(); ++i) {
                    JsonObject column = dataColumns.get(i).getAsJsonObject();
                    schemaColumns.add(SchemaUtils.parseColumn(column));
                }
            }

            if (dataSchema.has("SystemColumns")) {
                JsonArray systemColumns = dataSchema.get("SystemColumns").getAsJsonArray();
                for (int i = 0; i < systemColumns.size(); ++i) {
                    JsonObject column = systemColumns.get(i).getAsJsonObject();
                    Column systemCol = SchemaUtils.parseColumn(column);
                    schemaColumns.add(systemCol);
                    systemColumnKeys.add(systemCol.getName());
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
                    .systemColumnKeys(systemColumnKeys)
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
            if (recordCount >= 0) {
                inputSplitAssigner = new RowRangeInputSplitAssigner(sessionId, recordCount);
            }
        }

        SplitOptions.SplitMode splitMode =
            SplitOptions.SplitMode.fromString(tree.get("SplitMode").getAsString());
        // splits count，splitMode = size / bucket
        if (tree.has("SplitsCount")) {
            int splitsCount = tree.get("SplitsCount").getAsInt();
            if (splitsCount >= 0) {
                if (splitMode == SplitOptions.SplitMode.SIZE) {
                    inputSplitAssigner = new IndexedInputSplitAssigner(sessionId, splitsCount);
                } else if (splitMode == SplitOptions.SplitMode.BUCKET) {
                    JsonArray splitBucketIdJson = tree.get("SplitBucketId").getAsJsonArray();
                    List<Integer> splitBucketId = new ArrayList<>(splitsCount);
                    splitBucketIdJson.forEach(bucketId -> splitBucketId.add(
                        Integer.parseInt(bucketId.getAsString())));
                    inputSplitAssigner =
                        new BucketInputSplitAssigner(sessionId, splitsCount, splitBucketId);
                }
            }
        }

        if (tree.has("LatestVersion")) {
            long latestVersion = tree.get("LatestVersion").getAsLong();
            snapshotSpec = TableSnapshotSpec.TableAsOfVersion.create(latestVersion);
        }

        if (tree.has("SessionStats")) {
            JsonObject statsJson = tree.getAsJsonObject("SessionStats");
            this.estimateStats = new SessionStats();
            this.estimateStats.setEstimatedSize(statsJson.get("EstimatedSize").getAsLong());
            this.estimateStats.setEstimatedRowCount(statsJson.get("EstimatedRowCount").getAsLong());
        }

        if (tree.has("ExtendedArrowIPCEnabled")) {
            this.extendedArrowIPCEnabled = tree.get("ExtendedArrowIPCEnabled").getAsBoolean();
        }
    }

    @Override
    public String toJson() {
        Map<String, String> map = new HashMap<>();
        map.put("identifier", JsonUtils.toJson(identifier));
        map.put("details", details);
        return JsonUtils.toJson(map);
    }

    protected void initializeFromJson(String jsonString) {
        Map<String, String> map = (Map<String, String>) JsonUtils.fromJson(jsonString, Map.class);
        this.identifier = JsonUtils.fromJson(map.get("identifier"), TableIdentifier.class);
        loadResultFromJson(JsonUtils.parseJsonObject(map.get("details")));
    }

    @Override
    public SessionStats getEstimatedStats() {
        return estimateStats;
    }
}
