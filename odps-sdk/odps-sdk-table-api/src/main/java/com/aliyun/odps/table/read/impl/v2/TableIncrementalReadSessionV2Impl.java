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

import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.read.TableIncrementalReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.TableSnapshotSpec;
import com.google.gson.JsonObject;

/**
 * Incremental read session over the Storage API v2 protocol. CDC reads include the
 * server's system columns ({@code __operation} and friends) in the session schema, which
 * {@link V2SessionModelParser} preserves.
 */
public class TableIncrementalReadSessionV2Impl extends TableBatchReadSessionV2Impl
        implements TableIncrementalReadSession {

    private static final long serialVersionUID = 1L;

    protected IncrementalOptions incrementalOptions;

    public TableIncrementalReadSessionV2Impl(TableReadSessionBuilder builder) throws IOException {
        super(builder);
        if (incrementalOptions == null && builder.getIncrementalOptions() != null) {
            this.incrementalOptions = builder.getIncrementalOptions();
        }
    }

    TableIncrementalReadSessionV2Impl(com.aliyun.odps.storage.internal.SessionDescriptorCodec descriptor) {
        super(descriptor);
    }

    @Override
    protected JsonObject descriptorMetadata() {
        JsonObject metadata = super.descriptorMetadata();
        if (incrementalOptions != null && !incrementalOptions.equals(IncrementalOptions.NO_INCREMENTAL_OPTION)) {
            metadata.add("IncrementalReadOptions", new com.google.gson.Gson().toJsonTree(
                    V2SessionPlanner.toWireIncrementalOptions(incrementalOptions)));
        }
        return metadata;
    }

    @Override
    public TableSnapshotSpec getSnapshot() {
        throw new UnsupportedOperationException("Cannot get snapshot for incremental read.");
    }

    @Override
    public IncrementalOptions getIncrementalOptions() {
        return incrementalOptions;
    }

    @Override
    protected void loadResult(V2SessionResult result) {
        super.loadResult(result);
        JsonObject tree = result.tree();
        if (tree.has("IncrementalReadOptions")) {
            this.incrementalOptions = V2SessionModelParser.parseIncrementalOptions(tree);
        }
    }
}
