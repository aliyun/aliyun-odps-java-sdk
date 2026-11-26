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

package com.aliyun.odps.table.read.impl.v2;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.util.Optional;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.impl.v2.V2ClientHolder;
import com.aliyun.odps.storage.internal.SessionDescriptorCodec;
import com.aliyun.odps.table.impl.v2.SessionProxy;
import com.aliyun.odps.table.read.impl.batch.SplitRecordReaderImpl;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SessionStats;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.TableSnapshotSpec;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.utils.JsonUtils;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Batch read session backed by the Storage API v2 client ({@code odps-sdk-storage-api}).
 *
 * <p>Session planning delegates to {@link V2SessionPlanner}; data-plane reading uses
 * {@link SplitArrowReaderV2Impl}. Sessions round-trip through {@link #toJson()} within the
 * v2 protocol only.
 */
public class TableBatchReadSessionV2Impl implements TableBatchReadSession {

    private static final long serialVersionUID = 1L;

    protected TableIdentifier identifier;

    protected String sessionId;

    protected SessionStatus sessionStatus = SessionStatus.UNKNOWN;

    /** Raw create-session response JSON, kept for {@link #toJson()} round-trips. */
    protected String details;

    protected transient V2ClientHolder clientHolder;

    protected String descriptorRouteToken;

    protected transient volatile V2NormalSession normalSession;

    public TableBatchReadSessionV2Impl(TableReadSessionBuilder builder) throws IOException {
        if (builder.getDetailsJson() != null) {
            initializeFromJson(builder.getDetailsJson());
        } else {
            this.identifier = Preconditions.checkNotNull(builder.getIdentifier(),
                    "Table identifier", "required");
            this.sessionId = builder.getSessionId();
        }
        EnvironmentSettings settings = builder.getSettings();
        if (settings != null) {
            this.clientHolder = new V2ClientHolder(identifier, settings);
            this.clientHolder.updateRouteToken(descriptorRouteToken);
        } else if (builder.getDetailsJson() == null) {
            throw new IllegalArgumentException("Environment settings required to create or reload a session");
        }

        if (builder.getDetailsJson() == null && builder.getSessionId() == null) {
            planInputSplits(builder);
        } else if (builder.getDetailsJson() == null) {
            reloadInputSplits(builder.isSessionRefresh());
        }
    }

    protected void planInputSplits(TableReadSessionBuilder builder) throws IOException {
        ensureClientHolder();
        V2SessionResult result = V2SessionPlanner.create(clientHolder, builder);
        loadResult(result);

        long asyncIntervalInMills = V2SessionPlanner.asyncIntervalMillis(builder.getSettings());
        long asyncTimeoutInMills = V2SessionPlanner.asyncTimeoutMillis(builder.getSettings());
        long startTime = System.currentTimeMillis();
        while (sessionStatus == SessionStatus.INIT) {
            sleep(asyncIntervalInMills);
            reloadInputSplits(builder.isSessionRefresh());
            if (System.currentTimeMillis() - startTime >= asyncTimeoutInMills) {
                throw new IOException(failureMessage("timed out"));
            }
        }
        if (sessionStatus != SessionStatus.NORMAL) {
            throw new IOException(failureMessage("failed"));
        }
    }

    protected String reloadInputSplits(boolean sessionRefresh) throws IOException {
        Preconditions.checkString(sessionId, "Table read session id");
        ensureClientHolder();
        V2SessionResult result = V2SessionPlanner.reload(clientHolder, sessionId, sessionRefresh);
        loadResult(result);
        return result.rawJson();
    }

    protected void loadResult(V2SessionResult result) {
        this.details = result.rawJson();
        if (result.sessionId() != null) {
            this.sessionId = result.sessionId();
        }
        this.sessionStatus = result.sessionStatus();
        this.normalSession = null;
    }

    private void ensureClientHolder() {
        if (clientHolder == null) {
            throw new IllegalStateException(
                    "Storage API v2 client is not initialized for session: " + sessionId);
        }
    }

    private V2NormalSession normal() {
        if (normalSession == null) {
            synchronized (this) {
                if (normalSession == null) {
                    Preconditions.checkString(details, "Table read session details");
                    normalSession = V2NormalSession.from(
                            new V2SessionResult(JsonUtils.parseJsonObject(details)));
                }
            }
        }
        return normalSession;
    }

    private void sleep(long millis) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for v2 read session: " + sessionId, e);
        }
    }

    private String failureMessage(String verb) {
        return String.format(
                "Create table read session %s.\n"
                        + "Table identifier: %s.\n"
                        + "Session status: %s.\n"
                        + "Session id: %s.\n"
                        + "Error message: %s.",
                verb, identifier, sessionStatus, sessionId,
                JsonUtils.parseJsonObject(details).has("Message")
                        ? JsonUtils.parseJsonObject(details).get("Message").getAsString() : null);
    }

    @Override
    public String getId() {
        Preconditions.checkString(sessionId, "Table read session id");
        return sessionId;
    }

    @Override
    public TableIdentifier getTableIdentifier() {
        return identifier;
    }

    @Override
    public SessionStatus getStatus() {
        return sessionStatus;
    }

    @Override
    public DataSchema readSchema() {
        return normal().getDataSchema();
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner() {
        return normal().getAssigner();
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        java.util.Set<DataFormat> formats = normal().getSupportedFormats();
        return formats != null && formats.contains(dataFormat);
    }

    @Override
    public SplitReader<VectorSchemaRoot> createArrowReader(InputSplit split, ReaderOptions options)
            throws IOException {
        Preconditions.checkNotNull(identifier, "Table read identifier");
        Preconditions.checkNotNull(split, "Input split");
        Preconditions.checkNotNull(options, "Reader options");
        boolean extended = normal().isExtendedArrowIPCEnabled();
        EnvironmentSettings settings = options.getSettings() != null ? options.getSettings()
                : clientHolder == null ? null : clientHolder.settings();
        Preconditions.checkNotNull(settings, "ReaderOptions.settings", "required for a detached v2 session");
        V2ClientHolder worker = new V2ClientHolder(identifier, settings);
        worker.updateRouteToken(routeToken());
        return new SplitArrowReaderV2Impl(worker, identifier, split, options, extended);
    }

    @Override
    public SplitReader<ArrayRecord> createRecordReader(
            InputSplit split, ReaderOptions options) throws IOException {
        return new SplitRecordReaderImpl(
                createArrowReader(split, options), readSchema(), options);
    }

    @Override
    public String toJson() {
        String kind = this instanceof TableIncrementalReadSessionV2Impl
                ? SessionDescriptorCodec.INCREMENTAL : SessionDescriptorCodec.READ;
        return SessionDescriptorCodec.create(kind, identifier, descriptorMetadata(),
                routeToken(), null).toJson();
    }

    protected com.google.gson.JsonObject descriptorMetadata() {
        return JsonUtils.parseJsonObject(details);
    }

    private String routeToken() {
        return clientHolder == null ? descriptorRouteToken : clientHolder.routeToken();
    }

    protected TableBatchReadSessionV2Impl(SessionDescriptorCodec descriptor) {
        this.identifier = descriptor.table();
        this.descriptorRouteToken = descriptor.routeToken();
        loadResult(new V2SessionResult(descriptor.metadata()));
        normal(); // Validate metadata before accepting the detached object, without I/O.
    }

    public static TableBatchReadSessionV2Impl fromDescriptor(SessionDescriptorCodec descriptor) {
        if (SessionDescriptorCodec.INCREMENTAL.equals(descriptor.kind()))
            return new TableIncrementalReadSessionV2Impl(descriptor);
        if (!SessionDescriptorCodec.READ.equals(descriptor.kind()))
            throw new IllegalArgumentException("Expected a read session descriptor");
        return new TableBatchReadSessionV2Impl(descriptor);
    }

    protected Object writeReplace() { return new SessionProxy(toJson()); }

    private void readObject(ObjectInputStream input) throws IOException {
        throw new InvalidObjectException("Legacy v2 Java session streams are unsupported; use a versioned descriptor");
    }

    @Override
    public SessionStats getEstimatedStats() {
        return normal().getStats();
    }

    @Override
    public TableSnapshotSpec getSnapshot() {
        long latestVersion = normal().getLatestVersion();
        return latestVersion > 0 ? TableSnapshotSpec.TableAsOfVersion.create(latestVersion)
                : TableSnapshotSpec.TableVersionUnspecified.create();
    }

    @Override
    public Optional<String> getLatestTxnId() {
        return Optional.ofNullable(normal().getLatestTxnId());
    }

    protected void initializeFromJson(String jsonString) {
        com.google.gson.JsonObject root = JsonUtils.parseJsonObject(jsonString);
        if (root.has("version")) {
            SessionDescriptorCodec descriptor = SessionDescriptorCodec.parse(jsonString);
            boolean incremental = this instanceof TableIncrementalReadSessionV2Impl;
            if (!(incremental ? SessionDescriptorCodec.INCREMENTAL : SessionDescriptorCodec.READ)
                    .equals(descriptor.kind())) throw new IllegalArgumentException("Read session kind mismatch");
            this.identifier = descriptor.table();
            this.descriptorRouteToken = descriptor.routeToken();
            loadResult(new V2SessionResult(descriptor.metadata()));
            return;
        }
        java.util.Map<String, String> map = JsonUtils.fromJson(jsonString, java.util.Map.class);
        this.identifier = JsonUtils.fromJson(map.get("identifier"), TableIdentifier.class);
        this.details = map.get("details");
        com.google.gson.JsonObject tree = JsonUtils.parseJsonObject(details);
        loadResult(new V2SessionResult(tree));
    }
}
