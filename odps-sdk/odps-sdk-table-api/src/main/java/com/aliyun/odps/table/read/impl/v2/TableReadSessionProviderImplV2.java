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

import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableIncrementalReadSession;
import com.aliyun.odps.table.read.TableReadSessionProvider;
import com.aliyun.odps.table.read.TableReadSessionBuilder;

/**
 * Read session provider for the Storage API v2 protocol ({@code /api/storage/v3} wire),
 * built on the {@code odps-sdk-storage-api} client.
 *
 * <p>Select it with
 * {@code TableReadSessionBuilder#withSessionProvider("v2")}. The default provider
 * {@code "v1"} keeps using the legacy Storage API REST sessions and is unaffected.
 *
 * <p>Unsupported today: PARALLELISM split mode (fails fast at session build). See the
 * module documentation for the full capability matrix.
 */
public class TableReadSessionProviderImplV2 implements TableReadSessionProvider {

    @Override
    public String identifier() {
        return "v2";
    }

    @Override
    public TableBatchReadSession createBatchReadSession(TableReadSessionBuilder builder)
            throws IOException {
        return new TableBatchReadSessionV2Impl(builder);
    }

    @Override
    public TableIncrementalReadSession createIncrementalReadSession(TableReadSessionBuilder builder)
            throws IOException {
        return new TableIncrementalReadSessionV2Impl(builder);
    }
}
