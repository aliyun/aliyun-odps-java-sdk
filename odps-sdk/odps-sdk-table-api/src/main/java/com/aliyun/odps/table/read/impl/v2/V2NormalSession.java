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

import com.aliyun.odps.table.impl.v2.V2ClientHolder;

import java.io.IOException;
import java.io.InputStream;

import com.aliyun.odps.storage.internal.models.CreateTableReadStreamRequest;
import com.aliyun.odps.storage.models.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.read.SessionStats;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;

/**
 * A Storage API v2 read session assembled purely from an already-normal create-session
 * response, without any further server round-trip.
 *
 * <p>This lets the table-api v2 provider build sessions from serialized JSON: split
 * planning mirrors {@code TableReadSession}'s SIZE/ROW_OFFSET/BUCKET handling, while the
 * extended Arrow IPC flag and latest txn id (which the standalone v2 client does not
 * surface as accessors) are taken straight from the wire tree. Data-plane streams still go
 * through {@code StorageStub.createTableReadStream} so framing stays intact.
 */
final class V2NormalSession {

    private final String sessionId;

    private final String routeToken;

    private final DataSchema dataSchema;

    private final InputSplitAssigner assigner;

    private final java.util.Set<com.aliyun.odps.table.DataFormat> supportedFormats;

    private final SessionStats stats;

    private final boolean extendedArrowIPCEnabled;

    private final long latestVersion;

    private final String latestTxnId;

    private final long expirationTime;

    static V2NormalSession from(V2SessionResult result) {
        return new V2NormalSession(result);
    }

    private V2NormalSession(V2SessionResult result) {
        this.sessionId = result.sessionId();
        this.routeToken = result.optionalString("RouteToken").orElse(null);
        this.dataSchema = V2SessionModelParser.parseDataSchema(result.tree());
        this.assigner = V2SessionModelParser.parseInputSplitAssigner(result.tree(), sessionId);
        if (assigner == null) throw new IllegalArgumentException("Missing read session split metadata");
        this.supportedFormats = V2SessionModelParser.parseSupportedDataFormats(result.tree());
        this.stats = V2SessionModelParser.parseSessionStats(result.tree());
        this.extendedArrowIPCEnabled = result.extendedArrowIPCEnabled();
        this.latestVersion = result.tree().has("LatestVersion")
                ? result.tree().get("LatestVersion").getAsLong() : 0L;
        this.latestTxnId = result.optionalString("LatestTxnId").filter(v -> !v.isEmpty()).orElse(null);
        this.expirationTime = result.expirationTime();
    }

    String getSessionId() {
        return sessionId;
    }

    String getRouteToken() {
        return routeToken;
    }

    DataSchema getDataSchema() {
        return dataSchema;
    }

    InputSplitAssigner getAssigner() {
        return assigner;
    }

    java.util.Set<com.aliyun.odps.table.DataFormat> getSupportedFormats() {
        return supportedFormats;
    }

    SessionStats getStats() {
        return stats;
    }

    boolean isExtendedArrowIPCEnabled() {
        return extendedArrowIPCEnabled;
    }

    long getLatestVersion() {
        return latestVersion;
    }

    String getLatestTxnId() {
        return latestTxnId;
    }

    long getExpirationTime() {
        return expirationTime;
    }

    /**
     * Opens the raw Arrow stream for one split via the v2 SDK stub. Compression is
     * fixed by the native v2 HTTP Accept-Encoding header; the extended IPC framing is decided
     * by the session and surfaced per-response through the reader options.
     */
    static InputStream openStream(V2ClientHolder holder, TableIdentifier tableId,
                                  InputSplit split, ReaderOptions options) throws IOException {
        CreateTableReadStreamRequest request = new CreateTableReadStreamRequest();
        request.setMaxBatchRows(options.getBatchRowCount());
        if (options.getBatchRawSize() != 0L) {
            request.setMaxBatchRawSize(options.getBatchRawSize());
        }
        request.setDataFormat(DataFormat.newBuilder()
                .withType(options.getDataFormat().getType().toString())
                .withVersion(options.getDataFormat().getVersion().toString())
                .build());
        return holder.stub().createTableReadStream(tableId, split, request,
                holder.routeToken());
    }
}
