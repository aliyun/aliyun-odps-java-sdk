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

import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableStreamingWriteSession;
import com.aliyun.odps.table.write.TableUpsertSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.TableWriteSessionProvider;

/**
 * Write session provider for the Storage API v2 protocol ({@code /api/storage/v3} wire),
 * built on the {@code odps-sdk-storage-api} client's batch-compatible write mode.
 *
 * <p>Select it with {@code TableWriteSessionBuilder#withSessionProvider("v2")}. Only batch
 * writes are offered: streaming and upsert throw {@link UnsupportedOperationException}
 * because the table-api interfaces for them have no v1 implementation either and connectors
 * keep using their dedicated tunnel providers. See the module documentation for the full
 * capability matrix.
 */
public class TableWriteSessionProviderImplV2 implements TableWriteSessionProvider {

    @Override
    public String identifier() {
        return "v2";
    }

    @Override
    public TableBatchWriteSession createBatchWriteSession(TableWriteSessionBuilder builder)
            throws IOException {
        return new TableBatchWriteSessionV2Impl(builder);
    }

    @Override
    public TableStreamingWriteSession createStreamingWriteSession(TableWriteSessionBuilder builder)
            throws IOException {
        throw new UnsupportedOperationException(
                "Storage API v2 write provider does not support streaming writes yet");
    }

    @Override
    public TableUpsertSession createUpsertSession(TableWriteSessionBuilder builder)
            throws IOException {
        throw new UnsupportedOperationException(
                "Storage API v2 write provider does not support upsert writes yet");
    }
}
