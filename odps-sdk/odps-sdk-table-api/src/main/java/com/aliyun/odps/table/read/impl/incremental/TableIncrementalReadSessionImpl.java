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

package com.aliyun.odps.table.read.impl.incremental;

import com.aliyun.odps.table.SessionType;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.read.TableIncrementalReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.TableSnapshotSpec;
import com.aliyun.odps.table.read.impl.batch.TableBatchReadSessionImpl;

import com.aliyun.odps.table.utils.Preconditions;
import com.google.gson.JsonObject;

import java.io.IOException;

public class TableIncrementalReadSessionImpl extends TableBatchReadSessionImpl
        implements TableIncrementalReadSession {

    private static final Long DEFAULT_VERSION = -1L;

    private static final String DEFAULT_TIMESTAMP = "-1";

    protected IncrementalOptions incrementalOptions;

    public TableIncrementalReadSessionImpl(TableReadSessionBuilder builder) throws IOException {
        super(builder);
    }

    @Override
    protected JsonObject generateReadSessionRequest() {
        JsonObject sessionRequest = super.generateReadSessionRequest();
        this.incrementalOptions = sessionBuilder.getIncrementalOptions();

        Preconditions.checkArgument(incrementalOptions != null &&
                !incrementalOptions.equals(IncrementalOptions.NO_INCREMENTAL_OPTION),
                "Incremental options required");

        JsonObject incrementalRequest = new JsonObject();
        if (incrementalOptions.getType().equals(TableSnapshotSpec.Type.VERSION)) {
            incrementalRequest.addProperty("StartVersion",
                    incrementalOptions.getStartVersion()
                            .map(version -> version.getVersion().orElse(DEFAULT_VERSION))
                            .orElse(DEFAULT_VERSION));

            incrementalRequest.addProperty("EndVersion",
                    incrementalOptions.getEndVersion()
                            .map(version -> version.getVersion().orElse(DEFAULT_VERSION))
                            .orElse(DEFAULT_VERSION));

        } else if (incrementalOptions.getType().equals(TableSnapshotSpec.Type.TIMESTAMP)) {
            incrementalRequest.addProperty("StartTimeStamp",
                    incrementalOptions.getStartTimestamp()
                            .map(timestamp -> timestamp.getTimestamp().orElse(DEFAULT_TIMESTAMP))
                            .orElse(DEFAULT_TIMESTAMP));

            incrementalRequest.addProperty("EndTimeStamp",
                    incrementalOptions.getEndTimestamp()
                            .map(timestamp -> timestamp.getTimestamp().orElse(DEFAULT_TIMESTAMP))
                            .orElse(DEFAULT_TIMESTAMP));

        } else {
            throw new UnsupportedOperationException(
                    "Unsupported table snapshot spec: " + incrementalOptions.getType());
        }
        incrementalRequest.addProperty("Mode", incrementalOptions.getIncrementalMode().toString());

        sessionRequest.add("IncrementalReadOptions", incrementalRequest);

        return sessionRequest;
    }

    @Override
    protected void loadResultFromJson(JsonObject tree) {
        super.loadResultFromJson(tree);

        if (tree.has("IncrementalReadOptions")) {
            JsonObject options = tree.get("IncrementalReadOptions").getAsJsonObject();

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
                String mode = options.get("Mode").getAsString();
                IncrementalOptions.IncrementalMode incrementalMode =
                        IncrementalOptions.IncrementalMode.fromString(mode);
                builder.withIncrementalMode(incrementalMode);
            }

            this.incrementalOptions = builder.build();
        }
    }

    @Override
    public SessionType getType() {
        return SessionType.INCREMENTAL_READ;
    }

    @Override
    public IncrementalOptions getIncrementalOptions() {
        return this.incrementalOptions;
    }
}
