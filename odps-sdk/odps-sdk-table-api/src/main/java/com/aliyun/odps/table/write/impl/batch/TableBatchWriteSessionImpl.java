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

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.table.*;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.distribution.Distribution;
import com.aliyun.odps.table.distribution.Distributions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.table.order.NullOrdering;
import com.aliyun.odps.table.order.SortDirection;
import com.aliyun.odps.table.order.SortOrder;
import com.aliyun.odps.table.utils.*;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableWriteCapabilities;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.tunnel.TunnelException;
import com.google.gson.*;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.odps.table.utils.ConfigConstants.VERSION_1;
import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

public class TableBatchWriteSessionImpl extends TableBatchWriteSessionBase {

    private static final Logger logger = LoggerFactory.getLogger(TableBatchWriteSessionImpl.class.getName());

    private transient ExecutionEnvironment executionEnvironment;

    private transient RestClient restClient;

    public TableBatchWriteSessionImpl(TableIdentifier identifier,
                                      PartitionSpec partitionSpec,
                                      boolean overwrite,
                                      DynamicPartitionOptions dynamicPartitionOptions,
                                      ArrowOptions arrowOptions,
                                      TableWriteCapabilities capabilities,
                                      EnvironmentSettings settings) throws IOException {
        super(identifier, partitionSpec, overwrite,
                dynamicPartitionOptions, arrowOptions, capabilities, settings);
    }

    public TableBatchWriteSessionImpl(TableIdentifier identifier,
                                      String sessionId,
                                      EnvironmentSettings settings) throws IOException {
        super(identifier, sessionId, settings);
    }

    @Override
    protected void initSession() throws IOException {
        ensureInitialized();

        Map<String, String> headers = HttpUtils.createCommonHeader();
        headers.put(Headers.CONTENT_TYPE, "application/json");

        Map<String, String> params = new HashMap<>();
        params.put(ConfigConstants.SESSION_TYPE, getType().toString());

        try {
            String req = generateWriteSessionRequest();
            logger.debug(String.format("Write table '%s'.\n"
                    + "Session request:\n"
                    + "%s", identifier.toString(), req));

            Response resp = restClient.stringRequest(
                    ResourceBuilder.buildTableSessionResource(
                            ConfigConstants.VERSION_1,
                            identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            null),
                    "POST", params, headers, req);

            if (resp.isOK()) {
                String response = new String(resp.getBody());
                logger.debug(String.format("Write table '%s'.\n"
                        + "Session response:\n"
                        + "%s", identifier.toString(), response));
                loadResultFromJson(response);
            } else {

                throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID),
                        new ByteArrayInputStream(resp.getBody()), resp.getStatus());
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        } finally {
            // nothing
        }
    }

    @Override
    protected String reloadSession() throws IOException {
        ensureInitialized();

        Preconditions.checkString(this.sessionId, "Table write session id");

        Map<String, String> headers = HttpUtils.createCommonHeader();

        Map<String, String> params = new HashMap<>();
        params.put(ConfigConstants.SESSION_TYPE, getType().toString());

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
            throw new IOException("Failed to reload table write session with endpoint: "
                    + restClient.getEndpoint(), e);
        } catch (OdpsException e) {
            throw new IOException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (IOException e) {
                }
            }
        }
    }

    @Override
    public BatchWriter<VectorSchemaRoot> createArrowWriter(long blockNumber,
                                                           WriterAttemptId attemptId,
                                                           WriterOptions options) throws IOException {
        Preconditions.checkString(sessionId, "Table write session id");
        Preconditions.checkNotNull(identifier, "Table write session id");
        Preconditions.checkLong(blockNumber, 0, "Block number");
        Preconditions.checkNotNull(attemptId, "Attempt id");
        Preconditions.checkNotNull(options, "Writer options");

        if (maxBlockNumber().isPresent() && blockNumber >= maxBlockNumber().get()) {
            throw new IOException("Max block writer number:" + maxBlockNumber().get());
        }

        return new ArrowWriterImpl(
                sessionId, identifier, requiredSchema, blockNumber, attemptId, options, arrowOptions);
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        if (supportDataFormats != null) {
            return supportDataFormats.contains(dataFormat);
        }
        return false;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) throws IOException {
        ensureInitialized();

        if (messages == null) {
            throw new IllegalArgumentException("Invalid argument: messages.");
        }

        Preconditions.checkString(sessionId, "Table write session id");
        Map<String, String> headers = HttpUtils.createCommonHeader();
        headers.put(Headers.CONTENT_TYPE, "application/json");

        Map<String, String> params = new HashMap<String, String>();
        params.put(ConfigConstants.SESSION_ID, sessionId);

        try {
            String commitRequest = generateCommitRequest(messages);
            logger.debug(String.format("Commit table '%s'.\n"
                    + "Session request:\n"
                    + "%s", identifier.toString(), commitRequest));

            Response resp = restClient.stringRequest(ResourceBuilder.buildTableCommitResource(
                    VERSION_1,
                    identifier.getProject(),
                    identifier.getSchema(),
                    identifier.getTable()),
                    "POST", params, headers, commitRequest);

            String response;
            if (!resp.isOK()) {
                throw new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID),
                        new ByteArrayInputStream(resp.getBody()), resp.getStatus());
            } else {
                response = new String(resp.getBody());
                loadResultFromJson(response);
            }

            if (sessionStatus != SessionStatus.COMMITTED) {
                long asyncIntervalInMills = HttpUtils.getAsyncIntervalInMills(settings);
                long asyncTimeoutInMills = HttpUtils.getAsyncTimeoutInSeconds(settings) * 1000L;
                long startTime = System.currentTimeMillis();

                while (sessionStatus == SessionStatus.NORMAL ||
                        sessionStatus == SessionStatus.COMMITTING) {
                    Thread.sleep(asyncIntervalInMills);

                    logger.trace(String.format("Async commit table: '%s', session id: %s",
                            identifier.toString(), sessionId));

                    response = reloadSession();

                    if (System.currentTimeMillis() - startTime >= asyncTimeoutInMills) {
                        throw new IOException(
                                String.format(
                                        "Commit table write session timeout.\n"
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

            if (sessionStatus != SessionStatus.COMMITTED) {
                throw new IOException(
                        String.format(
                                "Commit table write session failed.\n"
                                        + "Table identifier: %s.\n"
                                        + "Session status: %s.\n"
                                        + "Session id: %s.\n"
                                        + "Error message: %s.",
                                identifier.toString(),
                                sessionStatus,
                                sessionId,
                                errorMessage));
            } else {
                logger.debug(String.format("Commit table '%s' success.\n"
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
    public void cleanup() {
        // TODO: clean up
    }

    private void ensureInitialized() {
        if (this.executionEnvironment == null) {
            this.executionEnvironment = ExecutionEnvironment.create(settings);
        }

        if (this.restClient == null) {
            this.restClient = this.executionEnvironment.createHttpClient(identifier.getProject());
        }
    }

    private String generateWriteSessionRequest() {
        JsonObject request = new JsonObject();
        request.add("PartitionSpec", new JsonPrimitive(
                targetPartitionSpec.toString(false, true)));
        request.add("Overwrite", new JsonPrimitive(overwrite));

        JsonObject jsonArrowOptions = new JsonObject();
        jsonArrowOptions.addProperty("TimestampUnit", arrowOptions.getTimestampUnit().toString());
        jsonArrowOptions.addProperty("DatetimeUnit", arrowOptions.getDateTimeUnit().toString());
        request.add("ArrowOptions", jsonArrowOptions);

        JsonObject dynamicOptions = new JsonObject();
        dynamicOptions.addProperty("InvalidStrategy", dynamicPartitionOptions.getInvalidStrategy().toString());
        dynamicOptions.addProperty("InvalidLimit", dynamicPartitionOptions.getInvalidLimit());
        dynamicOptions.addProperty("DynamicPartitionLimit", dynamicPartitionOptions.getDynamicPartitionLimit());
        request.add("DynamicPartitionOptions", dynamicOptions);

        if (writeCapabilities.supportHashBuckets()
                || writeCapabilities.supportRangeBuckets()) {
            request.add("SupportWriteCluster", new JsonPrimitive(true));
        } else {
            request.add("SupportWriteCluster", new JsonPrimitive(false));
        }

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(request);
    }

    private String generateCommitRequest(WriterCommitMessage[] messages) {
        JsonObject request = new JsonObject();
        JsonArray messageArray = new JsonArray();
        for (WriterCommitMessage commitMessage : messages) {
            if (commitMessage == null) {
                continue;
            }
            WriterCommitMessageImpl msg = (WriterCommitMessageImpl) commitMessage;
            messageArray.add(new JsonPrimitive(msg.getCommitMessage()));
        }
        request.add("CommitMessages", messageArray);
        return request.toString();
    }

    private void loadResultFromJson(String json) throws TunnelException {
        try {
            JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

            // session id
            if (tree.has("SessionId")) {
                sessionId = tree.get("SessionId").getAsString();
            }

            // expiration time
            if (tree.has("ExpirationTime")) {
                expirationTime = tree.get("ExpirationTime").getAsLong();
            }

            // session type
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

                requiredSchema = DataSchema.newBuilder()
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

            // max block num
            if (tree.has("MaxBlockNumber")) {
                maxBlockNumber = tree.get("MaxBlockNumber").getAsLong();
            }

            // required ordering
            if (tree.has("RequiredOrdering")) {
                JsonArray orders = tree.get("RequiredOrdering").getAsJsonArray();
                List<SortOrder> sortOrders = new ArrayList<>();
                orders.forEach(order -> sortOrders.add(parseOrders(order.getAsJsonObject())));
                requiredSortOrders = sortOrders.toArray(new SortOrder[0]);
            }

            // required distribution
            if (tree.has("RequiredDistribution")) {
                JsonObject distribution = tree.get("RequiredDistribution").getAsJsonObject();
                Distribution.Type type = Distribution.Type.UNSPECIFIED;
                if (distribution.has("Type")) {
                    type = Distribution.Type.valueOf(distribution.get("Type").getAsString().toUpperCase());
                }
                if (type.equals(Distribution.Type.UNSPECIFIED)) {
                    requiredDistribution = Distributions.unspecified();
                } else if (type.equals(Distribution.Type.HASH) ||
                        type.equals(Distribution.Type.RANGE)) {
                    List<String> clusterKeys = new ArrayList<>();
                    if (distribution.has("ClusterKeys")) {
                        JsonArray keys = distribution.get("ClusterKeys").getAsJsonArray();
                        keys.forEach(key -> clusterKeys.add(key.getAsString()));
                    }
                    int bucketsNumber = -1;
                    if (distribution.has("BucketsNumber")) {
                        bucketsNumber = distribution.get("BucketsNumber").getAsInt();
                    }
                    requiredDistribution = Distributions.clustered(clusterKeys, type, bucketsNumber);
                }
            }
        } catch (Exception e) {
            throw new TunnelException("Invalid session response: \n" + json, e);
        }
    }

    private SortOrder parseOrders(JsonObject order) {
        Preconditions.checkArgument(order.has("Name"), "Sort order must has name");
        String name = order.get("Name").getAsString();
        SortDirection sortDirection = order.has("SortDirection") ?
                SortDirection.valueOf(order.get("SortDirection").getAsString().toUpperCase())
                : SortDirection.NONE;
        NullOrdering nullOrdering = order.has("NullOrdering") ?
                NullOrdering.valueOf(order.get("NullOrdering").getAsString().toUpperCase())
                : NullOrdering.ANY;
        return new SortOrder(name, sortDirection, nullOrdering);
    }
}
