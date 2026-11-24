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
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.odps.storage.internal.models.CreateTableReadSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.storage.settings.IncrementalReadOptions;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.TableSnapshotSpec;
import com.aliyun.odps.table.utils.HttpUtils;
import com.google.gson.JsonObject;

/**
 * Maps table-api read builders onto the Storage API v2 client and drives the
 * create/poll session lifecycle, returning the raw response JSON for session persistence.
 */
final class V2SessionPlanner {

    private V2SessionPlanner() {
    }

    static long asyncIntervalMillis(EnvironmentSettings settings) {
        return HttpUtils.getAsyncIntervalInMills(settings);
    }

    static long asyncTimeoutMillis(EnvironmentSettings settings) {
        return HttpUtils.getAsyncTimeoutInSeconds(settings) * 1000L;
    }

    static V2SessionResult create(V2ClientHolder holder, TableReadSessionBuilder builder)
            throws IOException {
        CreateTableReadSessionRequest request = toWireRequest(builder);
        CreateTableReadSessionResponse response =
                holder.stub().createTableReadSession(holder.identifier(), request);
        holder.updateRouteToken(response.getRouteToken());
        return toResult(holder, response);
    }

    static V2SessionResult reload(V2ClientHolder holder, String sessionId, boolean sessionRefresh)
            throws IOException {
        CreateTableReadSessionResponse response =
                holder.stub().getTableReadSession(holder.identifier(), sessionId, sessionRefresh);
        holder.updateRouteToken(response.getRouteToken());
        return toResult(holder, response);
    }

    /**
     * Re-serializes the typed response back to wire JSON using the stub's configured Gson
     * (which registers the SplitMode/ArrowOptions/TableSchema adapters), so sessions can be
     * saved via {@code toJson()} and restored without a server round-trip.
     */
    private static V2SessionResult toResult(V2ClientHolder holder,
                                            CreateTableReadSessionResponse response) {
        JsonObject tree = holder.stub().getGson()
                .toJsonTree(response).getAsJsonObject();
        return new V2SessionResult(tree);
    }

    static CreateTableReadSessionRequest toWireRequest(TableReadSessionBuilder builder) {
        SplitOptions tableSplitOptions = builder.getSplitOptions() == null
                ? SplitOptions.createDefault() : builder.getSplitOptions();
        if (tableSplitOptions.getSplitMode() == SplitOptions.SplitMode.PARALLELISM) {
            throw new UnsupportedOperationException(
                    "Storage API v2 read provider does not support PARALLELISM split mode yet");
        }

        CreateTableReadSessionRequest request = new CreateTableReadSessionRequest();
        request.setRequiredDataColumns(nullSafe(builder.getRequiredDataColumns()));
        request.setRequiredPartitionColumns(nullSafe(builder.getRequiredPartitionColumns()));
        request.setRequiredPartitions(builder.getRequiredPartitions() == null
                ? java.util.Collections.emptyList()
                : builder.getRequiredPartitions().stream()
                        .map(spec -> spec.toString(false, true))
                        .collect(Collectors.toList()));
        request.setRequiredBucketIds(nullSafe(builder.getRequiredBucketIds()));

        com.aliyun.odps.storage.settings.SplitOptions.SplitOptionsBuilder v2Split =
                com.aliyun.odps.storage.settings.SplitOptions.newBuilder()
                        .withSplitMode(com.aliyun.odps.storage.models.SplitMode.fromString(
                                tableSplitOptions.getSplitMode().toString()))
                        .withCrossPartition(tableSplitOptions.isCrossPartition())
                        .withBucketSort(tableSplitOptions.isBucketSort());
        switch (tableSplitOptions.getSplitMode()) {
            case ROW_OFFSET:
                v2Split.withSplitRowCount(tableSplitOptions.getSplitNumber());
                break;
            case SIZE:
            default:
                v2Split.withSplitSize(tableSplitOptions.getSplitNumber());
                break;
        }
        request.setSplitOptions(v2Split.build());
        request.setSplitMaxFileNum(tableSplitOptions.getSplitMaxFileNum());

        if (builder.getArrowOptions() != null) {
            request.setArrowOptions(builder.getArrowOptions());
        }

        Predicate filterPredicate = builder.getFilterPredicate();
        request.setFilterPredicate(filterPredicate == null
                ? Predicate.NO_PREDICATE.toString() : filterPredicate.toString());
        request.setFilterPredicateFallback(builder.isAllowFilterPredicateFallback());
        request.setEnableEstimateStats(builder.isEnableEstimateStats());
        request.setEnableExtendedArrowIPC(builder.isEnableExtendedArrowIPC());
        request.setUniqueMapKey(builder.isEnableUniqueMapKey());

        IncrementalOptions incrementalOptions = builder.getIncrementalOptions();
        if (incrementalOptions != null
                && !incrementalOptions.equals(IncrementalOptions.NO_INCREMENTAL_OPTION)) {
            request.setIncrementalRead(true);
            request.setIncrementalReadOptions(toWireIncrementalOptions(incrementalOptions));
        }
        return request;
    }

    static IncrementalReadOptions toWireIncrementalOptions(IncrementalOptions options) {
        IncrementalReadOptions.Builder builder = IncrementalReadOptions.newBuilder()
                .withMode(options.getIncrementalMode().toString());
        if (options.getType() == TableSnapshotSpec.Type.VERSION) {
            builder.withStartVersion(options.getStartVersion()
                    .flatMap(v -> v.getVersion()).orElse(-1L));
            builder.withEndVersion(options.getEndVersion()
                    .flatMap(v -> v.getVersion()).orElse(-1L));
        } else if (options.getType() == TableSnapshotSpec.Type.TIMESTAMP) {
            builder.withStartTimeStamp(options.getStartTimestamp()
                    .flatMap(t -> t.getTimestamp()).orElse("-1"));
            builder.withEndTimeStamp(options.getEndTimestamp()
                    .flatMap(t -> t.getTimestamp()).orElse("-1"));
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported table snapshot spec: " + options.getType());
        }
        options.getStartTxnId().ifPresent(builder::withStartTxnId);
        return builder.build();
    }

    private static <T> List<T> nullSafe(List<T> list) {
        return list == null ? java.util.Collections.emptyList() : list;
    }
}
