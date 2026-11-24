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
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.impl.v2.V2ClientHolder;
import com.aliyun.odps.storage.internal.SessionDescriptorCodec;
import com.aliyun.odps.table.impl.v2.SessionProxy;
import com.aliyun.odps.storage.internal.models.BatchCompatibleSessionResponse;
import com.aliyun.odps.storage.write.BlockWriteResult;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.distribution.Distribution;
import com.aliyun.odps.table.distribution.Distributions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.order.SortOrder;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.utils.HttpUtils;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.utils.JsonUtils;
import com.aliyun.odps.utils.StringUtils;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch write session over the Storage API v2 protocol, using the SDK's
 * {@code BATCH_COMPATIBLE} mode so block/attempt semantics match the v1 provider.
 *
 * <p>Streaming and upsert writes are intentionally not offered here: the table-api
 * interfaces for them are unimplemented placeholders, and connectors keep using their
 * dedicated tunnel providers. See the module documentation for the capability matrix.
 */
public class TableBatchWriteSessionV2Impl implements TableBatchWriteSession {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(TableBatchWriteSessionV2Impl.class);

    protected TableIdentifier identifier;

    protected String sessionId;

    protected SessionStatus sessionStatus = SessionStatus.UNKNOWN;

    protected DataSchema requiredSchema;

    protected SortOrder[] requiredSortOrders = new SortOrder[]{};

    protected Distribution requiredDistribution = Distributions.unspecified();

    protected long maxBlockNumber;

    protected Set<DataFormat> supportDataFormats;

    protected boolean enhanceWriteCheck;

    protected ArrowOptions arrowOptions;

    /** Raw create-session response JSON for {@link #toJson()} round-trips. */
    protected String details;

    protected transient V2WriteClientHolder clientHolder;

    private String descriptorRouteToken;

    /** Live v2 SDK session used for block writers and commits. */
    protected transient com.aliyun.odps.storage.write.TableWriteSession v2WriteSession;

    // Once commit has been sent, cleanup must not abort a possibly successful operation.
    private volatile boolean commitStarted;

    public TableBatchWriteSessionV2Impl(TableWriteSessionBuilder builder) throws IOException {
        this.identifier = Preconditions.checkNotNull(builder.getIdentifier(),
                "Table identifier", "required");
        EnvironmentSettings settings = Preconditions.checkNotNull(builder.getSettings(),
                "Environment settings", "required");
        this.clientHolder = new V2WriteClientHolder(identifier, settings);
        this.arrowOptions = com.aliyun.odps.storage.write.BatchCompatibleOptions.newBuilder()
                .withArrowOptions(builder.getArrowOptions() == null
                        ? ArrowOptions.createDefault() : builder.getArrowOptions()).build().getArrowOptions();

        if (builder.getSessionId() == null) {
            initSession(builder);
        } else {
            this.sessionId = builder.getSessionId();
            Preconditions.checkArgument(StringUtils.isNotBlank(sessionId),
                    "Table write session id is blank");
            this.v2WriteSession = clientHolder.writeSession(sessionId, arrowOptions);
            loadSessionMetadata();
        }
    }

    protected void initSession(TableWriteSessionBuilder builder) throws IOException {
        this.v2WriteSession = clientHolder.writeSession(builder);
        loadSessionMetadata();

        if (sessionStatus != SessionStatus.NORMAL && sessionStatus != SessionStatus.COMMITTED) {
            throw new IOException(String.format(
                    "Create table write session failed.\n"
                            + "Table identifier: %s.\n"
                            + "Session status: %s.\n"
                            + "Error message: %s.",
                    identifier, sessionStatus, lastMessage()));
        }
    }

    private void loadSessionMetadata() throws IOException {
        BatchCompatibleSessionResponse response = v2WriteSession.getBatchCompatibleResponse();
        if (response != null) {
            loadResult(response);
        } else {
            this.sessionId = v2WriteSession.getId();
            reloadSession(false);
        }
    }

    protected String reloadSession(boolean sessionRefresh) throws IOException {
        Preconditions.checkString(sessionId, "Table write session id");
        ensureClientHolder();
        BatchCompatibleSessionResponse response = clientHolder.stub()
                .getBatchCompatibleSession(identifier, sessionId, clientHolder.routeToken());
        clientHolder.updateRouteToken(response.getRouteToken());
        loadResult(response);
        return details;
    }

    private void loadResult(BatchCompatibleSessionResponse response) {
        com.google.gson.JsonObject tree = clientHolder.stub().getGson()
                .toJsonTree(response).getAsJsonObject();
        loadMetadata(tree);
    }

    private void loadMetadata(com.google.gson.JsonObject tree) {
        this.details = tree.toString();
        if (tree.has("SessionId")) {
            this.sessionId = tree.get("SessionId").getAsString();
        }
        if (tree.has("SessionStatus")) {
            this.sessionStatus =
                    SessionStatus.valueOf(tree.get("SessionStatus").getAsString().toUpperCase());
        }
        if (tree.has("DataSchema")) {
            this.requiredSchema = V2WriteResponses.parseDataSchema(
                    tree.getAsJsonObject("DataSchema"));
        }
        if (tree.has("MaxBlockNumber") && !tree.get("MaxBlockNumber").isJsonNull()) {
            this.maxBlockNumber = tree.get("MaxBlockNumber").getAsLong();
        }
        if (tree.has("SupportedDataFormat")) {
            this.supportDataFormats = V2WriteResponses.parseSupportedDataFormats(tree);
        }
        if (tree.has("EnhanceWriteCheck") && !tree.get("EnhanceWriteCheck").isJsonNull()) {
            this.enhanceWriteCheck = tree.get("EnhanceWriteCheck").getAsBoolean();
        }
        this.requiredSortOrders = V2WriteResponses.parseRequiredOrdering(tree);
        this.requiredDistribution = V2WriteResponses.parseRequiredDistribution(tree);
    }

    private void ensureClientHolder() {
        if (clientHolder == null) {
            throw new IllegalStateException(
                    "Detached v2 session cannot coordinate commit; reload with sessionId and settings: " + sessionId);
        }
    }

    private String lastMessage() {
        if (details == null) {
            return null;
        }
        com.google.gson.JsonObject tree = JsonUtils.parseJsonObject(details);
        return tree.has("Message") ? tree.get("Message").getAsString() : null;
    }

    @Override
    public String getId() {
        Preconditions.checkString(sessionId, "Table write session id");
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
    public DataSchema requiredSchema() {
        return requiredSchema;
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        return supportDataFormats != null && supportDataFormats.contains(dataFormat);
    }

    @Override
    public Optional<Long> maxBlockNumber() {
        return maxBlockNumber > 0 ? Optional.of(maxBlockNumber) : Optional.empty();
    }

    @Override
    public Distribution requiredDistribution() {
        return requiredDistribution;
    }

    @Override
    public SortOrder[] requiredOrdering() {
        return requiredSortOrders;
    }

    @Override
    public BatchWriter<VectorSchemaRoot> createArrowWriter(long blockNumber, WriterAttemptId attemptId,
                                                           WriterOptions options) throws IOException {
        Preconditions.checkNotNull(attemptId, "Writer attempt id");
        Preconditions.checkNotNull(options, "Writer options");
        Preconditions.checkNotNull(requiredSchema, "Table write required schema");
        if (blockNumber < 0 || blockNumber > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid block number: " + blockNumber);
        }
        if (options.maxBlockNumber().isPresent()
                && blockNumber >= options.maxBlockNumber().get()) {
            throw new IOException("User defined max block writer number: "
                    + options.maxBlockNumber().get());
        }
        EnvironmentSettings settings = options.getSettings() != null ? options.getSettings()
                : clientHolder == null ? null : clientHolder.settings();
        Preconditions.checkNotNull(settings, "WriterOptions.settings", "required for a detached v2 session");
        if (sessionStatus != SessionStatus.NORMAL || commitStarted)
            throw new IllegalStateException("Cannot open a writer after session commit has started");
        V2ClientHolder dataClient = new V2ClientHolder(identifier, settings);
        dataClient.updateRouteToken(routeToken());
        try {
            com.aliyun.odps.storage.write.TableWriteSession workerSession =
                    new com.aliyun.odps.storage.write.TableWriteSessionBuilder(
                            dataClient.stub(), options.getBufferAllocator(), identifier)
                            .withUploadBufferSize(options.getUploadBufferSize())
                            .withSessionDescriptor(com.aliyun.odps.storage.models.SessionDescriptor
                                    .fromJson(toDescriptorJson()))
                            .build();
            com.aliyun.odps.storage.write.TableBlockWriter v2Writer =
                    workerSession.createBlockWriter((int) blockNumber, attemptId.getAttemptNumber(),
                            options.getBufferAllocator(), dataClient.stub(), options.getChunkSize(),
                            options.getCompressionCodec() == CompressionCodec.LZ4_FRAME_NATIVE
                                    ? CompressionCodec.LZ4_FRAME_NATIVE : CompressionCodec.ZSTD);
            return new ArrowWriterV2Impl(v2Writer, identifier, sessionId, blockNumber, attemptId,
                    options, requiredSchema, enhanceWriteCheck,
                    dataClient);
        } catch (RuntimeException | Error e) {
            dataClient.close();
            throw e;
        }
    }

    @Override
    public void commit(WriterCommitMessage[] messages) throws IOException {
        commit(messages, null);
    }

    @Override
    public void commit(WriterCommitMessage[] messages, int waitFlyingWritersTimeoutSeconds)
            throws IOException {
        commit(messages, Integer.valueOf(waitFlyingWritersTimeoutSeconds));
    }

    private void commit(WriterCommitMessage[] messages, Integer waitFlyingWritersTimeoutSeconds)
            throws IOException {
        ensureClientHolder();
        Preconditions.checkString(sessionId, "Table write session id");
        if (messages == null) {
            throw new IllegalArgumentException("Invalid argument: messages.");
        }
        if (waitFlyingWritersTimeoutSeconds != null
                && (waitFlyingWritersTimeoutSeconds < 1 || waitFlyingWritersTimeoutSeconds > 600)) {
            throw new IllegalArgumentException(
                    "WaitFlyingWritersTimeoutSeconds must be between 1 and 600 seconds");
        }

        List<BlockWriteResult> results = new ArrayList<>();
        for (WriterCommitMessage message : messages) {
            if (message == null) {
                continue;
            }
            if (!(message instanceof WriterCommitMessageV2Impl)) {
                throw new IllegalArgumentException(
                        "Expected a v2 writer commit message, got " + message.getClass().getName());
            }
            results.add(((WriterCommitMessageV2Impl) message).blockWriteResult());
        }

        if (sessionStatus == SessionStatus.COMMITTED) {
            return;
        }
        if ((commitStarted || sessionStatus == SessionStatus.COMMITTING)
                && recoverCommit(null)) {
            return;
        }

        for (int attempt = 0; attempt < 2; attempt++) {
            boolean previouslyStarted = commitStarted;
            commitStarted = true;
            try {
                commitResults(results, waitFlyingWritersTimeoutSeconds);
                sessionStatus = SessionStatus.COMMITTED;
                return;
            } catch (IllegalArgumentException invalidResult) {
                // Native argument validation failed before any commit request was sent.
                commitStarted = previouslyStarted;
                throw invalidResult;
            } catch (Exception failure) {
                if (recoverCommit(failure)) {
                    return;
                }
                // Only NORMAL permits a new commit request. Never resend while COMMITTING.
                if (attempt == 1) {
                    throw new IOException("Commit failed; session remains NORMAL: " + sessionId,
                            failure);
                }
            }
        }
    }

    /** Returns false only when a fresh response confirms NORMAL and a retry is allowed. */
    private boolean recoverCommit(Exception originalFailure) throws IOException {
        long started = System.nanoTime();
        long timeoutNanos = TimeUnit.SECONDS.toNanos(
                HttpUtils.getAsyncTimeoutInSeconds(clientHolder.settings()));
        long intervalMillis = Math.max(1L,
                HttpUtils.getAsyncIntervalInMills(clientHolder.settings()));
        while (true) {
            try {
                reloadSession(false);
            } catch (Exception reloadFailure) {
                IOException unknown = unknownCommit("could not reload session state", reloadFailure);
                if (originalFailure != null) {
                    unknown.addSuppressed(originalFailure);
                }
                throw unknown;
            }
            if (sessionStatus == SessionStatus.COMMITTED) {
                return true;
            }
            if (sessionStatus == SessionStatus.NORMAL) {
                return false;
            }
            if (sessionStatus == SessionStatus.UNKNOWN) {
                throw unknownCommit("service returned UNKNOWN", originalFailure);
            }
            if (sessionStatus != SessionStatus.COMMITTING) {
                throw new IOException("Commit table write session failed. Session status: "
                        + sessionStatus + ". Session id: " + sessionId
                        + ". Error message: " + lastMessage(), originalFailure);
            }
            long remaining = timeoutNanos - (System.nanoTime() - started);
            if (remaining <= 0) {
                throw unknownCommit("timed out waiting for COMMITTING", originalFailure);
            }
            try {
                TimeUnit.NANOSECONDS.sleep(Math.min(remaining,
                        TimeUnit.MILLISECONDS.toNanos(intervalMillis)));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw unknownCommit("interrupted while waiting for COMMITTING", interrupted);
            }
        }
    }

    private IOException unknownCommit(String reason, Exception cause) {
        return new IOException("Commit result unknown for session " + sessionId + ": " + reason
                + ". Recover this session's status before retrying in a new session.", cause);
    }

    private void commitResults(List<BlockWriteResult> results,
            Integer waitFlyingWritersTimeoutSeconds) throws IOException {
        ensureWriteSession();
        if (waitFlyingWritersTimeoutSeconds == null) {
            v2WriteSession.commit(results);
        } else {
            v2WriteSession.commit(results, waitFlyingWritersTimeoutSeconds);
        }
    }

    private void ensureWriteSession() throws IOException {
        if (v2WriteSession == null) {
            Preconditions.checkString(sessionId, "Table write session id");
            v2WriteSession = clientHolder.writeSession(sessionId, arrowOptions);
        }
    }

    @Override
    public void cleanup() {
        if (clientHolder == null || sessionId == null || commitStarted
                || sessionStatus != SessionStatus.NORMAL) {
            return;
        }
        try {
            clientHolder.stub().abortTableWriteSession(identifier, sessionId,
                    clientHolder.routeToken(),
                    com.aliyun.odps.storage.write.WriteMode.BATCH_COMPATIBLE);
            sessionStatus = SessionStatus.EXPIRED;
        } catch (Exception e) {
            logger.warn("Failed to abort v2 write session {}", sessionId, e);
        }
    }

    private String routeToken() {
        return clientHolder == null ? descriptorRouteToken : clientHolder.routeToken();
    }

    /** Export the detached data-plane description, never driver credentials or commit authority. */
    public String toDescriptorJson() {
        com.google.gson.JsonObject metadata = JsonUtils.parseJsonObject(details);
        metadata.addProperty("SessionStatus", sessionStatus.name());
        metadata.addProperty("CommitStarted", commitStarted);
        return SessionDescriptorCodec.create(SessionDescriptorCodec.WRITE, identifier, metadata,
                routeToken(), arrowOptions).toJson();
    }

    private TableBatchWriteSessionV2Impl(SessionDescriptorCodec descriptor) {
        this.identifier = descriptor.table();
        this.descriptorRouteToken = descriptor.routeToken();
        this.arrowOptions = descriptor.arrowOptions();
        com.google.gson.JsonObject metadata = descriptor.metadata();
        loadMetadata(metadata);
        this.commitStarted = metadata.has("CommitStarted") && metadata.get("CommitStarted").getAsBoolean();
    }

    public static TableBatchWriteSessionV2Impl fromDescriptor(SessionDescriptorCodec descriptor) {
        if (!SessionDescriptorCodec.WRITE.equals(descriptor.kind()))
            throw new IllegalArgumentException("Expected a write session descriptor");
        return new TableBatchWriteSessionV2Impl(descriptor);
    }

    protected Object writeReplace() { return new SessionProxy(toDescriptorJson()); }

    private void readObject(ObjectInputStream input) throws IOException {
        throw new InvalidObjectException("Legacy v2 Java session streams are unsupported; use a versioned descriptor");
    }

    @Override
    public String toString() {
        return "TableBatchWriteSessionV2Impl{sessionId=" + sessionId
                + ", identifier=" + identifier
                + ", status=" + sessionStatus + "}";
    }
}
