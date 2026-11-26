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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.Column;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.read.SessionStats;
import com.aliyun.odps.table.read.TableSnapshotSpec;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.BucketInputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplitAssigner;
import com.aliyun.odps.table.utils.SchemaUtils;
import com.aliyun.odps.table.utils.SessionUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Parses Storage API v2 read-session response JSON into the shared table-api session
 * model. Field names follow the server-side {@code CreateReadSessionResponse}.
 */
final class V2SessionModelParser {

    private static final Long DEFAULT_VERSION = -1L;

    private static final String DEFAULT_TIMESTAMP = "-1";

    private V2SessionModelParser() {
    }

    static DataSchema parseDataSchema(JsonObject tree) {
        if (tree == null || !tree.has("DataSchema")) {
            return null;
        }
        return toDataSchema(tree.get("DataSchema").getAsJsonObject());
    }

    /** Parses a bare DataSchema object (as returned by the v2 create-session response). */
    static DataSchema parseDataSchemaObject(JsonObject dataSchema) {
        if (dataSchema == null) {
            return null;
        }
        return toDataSchema(dataSchema);
    }

    private static DataSchema toDataSchema(JsonObject dataSchema) {
        List<Column> schemaColumns = new ArrayList<>();
        List<String> systemColumnKeys = new ArrayList<>();
        List<String> partitionKeys = new ArrayList<>();

        if (dataSchema.has("DataColumns")) {
            JsonArray dataColumns = dataSchema.get("DataColumns").getAsJsonArray();
            for (int i = 0; i < dataColumns.size(); ++i) {
                schemaColumns.add(SchemaUtils.parseColumn(dataColumns.get(i).getAsJsonObject()));
            }
        }
        if (dataSchema.has("SystemColumns")) {
            JsonArray systemColumns = dataSchema.get("SystemColumns").getAsJsonArray();
            for (int i = 0; i < systemColumns.size(); ++i) {
                Column systemCol = SchemaUtils.parseColumn(systemColumns.get(i).getAsJsonObject());
                schemaColumns.add(systemCol);
                systemColumnKeys.add(systemCol.getName());
            }
        }
        if (dataSchema.has("PartitionColumns")) {
            JsonArray partitionColumns = dataSchema.get("PartitionColumns").getAsJsonArray();
            for (int i = 0; i < partitionColumns.size(); ++i) {
                Column partitionCol =
                        SchemaUtils.parseColumn(partitionColumns.get(i).getAsJsonObject());
                schemaColumns.add(partitionCol);
                partitionKeys.add(partitionCol.getName());
            }
        }

        return DataSchema.newBuilder()
                .columns(schemaColumns)
                .partitionBy(partitionKeys)
                .systemColumnKeys(systemColumnKeys)
                .build();
    }

    static Set<DataFormat> parseSupportedDataFormats(JsonObject tree) {
        if (!tree.has("SupportedDataFormat")) {
            return null;
        }
        Set<DataFormat> formats = new HashSet<>();
        tree.get("SupportedDataFormat").getAsJsonArray()
                .forEach(format -> formats.add(
                        SessionUtils.parseDataFormat(format.getAsJsonObject())));
        return formats;
    }

    /**
     * Rebuilds the split assigner from the serialized response. RowOffset mode exposes
     * RecordCount; Size/Bucket expose SplitsCount (Bucket additionally pins each split's
     * bucket id via SplitBucketId).
     */
    static InputSplitAssigner parseInputSplitAssigner(JsonObject tree, String sessionId) {
        SplitOptions.SplitMode mode = tree.has("SplitMode")
                && !tree.get("SplitMode").isJsonNull()
                && !tree.get("SplitMode").getAsString().isEmpty()
                ? SplitOptions.SplitMode.fromString(tree.get("SplitMode").getAsString()) : null;
        if ((mode == null || mode == SplitOptions.SplitMode.ROW_OFFSET)
                && tree.has("RecordCount") && !tree.get("RecordCount").isJsonNull()) {
            long recordCount = tree.get("RecordCount").getAsLong();
            if (recordCount >= 0) {
                return new RowRangeInputSplitAssigner(sessionId, recordCount);
            }
        }
        if (tree.has("SplitsCount") && !tree.get("SplitsCount").isJsonNull()) {
            int splitsCount = tree.get("SplitsCount").getAsInt();
            if (splitsCount < 0) {
                return null;
            }
            boolean bucketMode = false;
            if (tree.has("SplitMode") && !tree.get("SplitMode").getAsString().isEmpty()) {
                bucketMode = SplitOptions.SplitMode.fromString(
                        tree.get("SplitMode").getAsString()) == SplitOptions.SplitMode.BUCKET;
            } else if (tree.has("SplitBucketId")) {
                // Older servers omit SplitMode; a non-empty SplitBucketId implies BUCKET.
                bucketMode = tree.get("SplitBucketId").getAsJsonArray().size() > 0;
            }
            if (bucketMode) {
                JsonArray bucketIds = tree.get("SplitBucketId").getAsJsonArray();
                List<Integer> splitBucketId = new ArrayList<>(splitsCount);
                bucketIds.forEach(bucketId -> splitBucketId.add(
                        Integer.parseInt(bucketId.getAsString())));
                return new BucketInputSplitAssigner(sessionId, splitsCount, splitBucketId);
            }
            return new IndexedInputSplitAssigner(sessionId, splitsCount);
        }
        return null;
    }

    static SessionStats parseSessionStats(JsonObject tree) {
        if (!tree.has("SessionStats") || tree.get("SessionStats").isJsonNull()) {
            return null;
        }
        JsonObject statsJson = tree.getAsJsonObject("SessionStats");
        // Server omits the object when estimation is disabled; treat an empty payload as
        // "no stats" instead of NPE-ing on missing members.
        if (statsJson.entrySet().isEmpty()) {
            return null;
        }
        SessionStats stats = new SessionStats();
        stats.setEstimatedSize(statsJson.get("EstimatedSize").getAsLong());
        stats.setEstimatedRowCount(statsJson.get("EstimatedRowCount").getAsLong());
        return stats;
    }

    static TableSnapshotSpec parseSnapshot(JsonObject tree) {
        if (tree.has("LatestVersion") && !tree.get("LatestVersion").isJsonNull()) {
            long latestVersion = tree.get("LatestVersion").getAsLong();
            if (latestVersion > 0) {
                return TableSnapshotSpec.TableAsOfVersion.create(latestVersion);
            }
        }
        return null;
    }

    static IncrementalOptions parseIncrementalOptions(JsonObject tree) {
        if (!tree.has("IncrementalReadOptions")) {
            return null;
        }
        JsonObject options = tree.getAsJsonObject("IncrementalReadOptions");
        IncrementalOptions.Builder builder = IncrementalOptions.newBuilder();

        if (options.has("StartTimeStamp")) {
            String startTimestamp = options.get("StartTimeStamp").getAsString();
            if (!startTimestamp.equals(DEFAULT_TIMESTAMP)) {
                builder.startTimeStamp(startTimestamp);
            }
        }
        if (options.has("EndTimeStamp")) {
            String endTimeStamp = options.get("EndTimeStamp").getAsString();
            if (!endTimeStamp.equals(DEFAULT_TIMESTAMP)) {
                builder.endTimeStamp(endTimeStamp);
            }
        }
        if (options.has("StartVersion")) {
            Long startVersion = options.get("StartVersion").getAsLong();
            if (!startVersion.equals(DEFAULT_VERSION)) {
                builder.startVersion(startVersion);
            }
        }
        if (options.has("EndVersion")) {
            Long endVersion = options.get("EndVersion").getAsLong();
            if (!endVersion.equals(DEFAULT_VERSION)) {
                builder.endVersion(endVersion);
            }
        }
        if (options.has("Mode")) {
            builder.withIncrementalMode(
                    IncrementalOptions.IncrementalMode.fromString(options.get("Mode").getAsString()));
        }
        if (options.has("StartTxnId") && !options.get("StartTxnId").getAsString().isEmpty()) {
            builder.startTxnId(options.get("StartTxnId").getAsString());
        }
        return builder.build();
    }
}
