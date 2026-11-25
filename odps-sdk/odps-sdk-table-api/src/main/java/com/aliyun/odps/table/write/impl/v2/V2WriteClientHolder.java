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

import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.storage.write.BatchCompatibleOptions;
import com.aliyun.odps.storage.write.TableWriteSessionBuilder;
import com.aliyun.odps.storage.write.WriteMode;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.impl.v2.V2ClientHolder;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.write.TableWriteCapabilities;

/**
 * Lazily-built Storage API v2 client for one write session. Mirrors the read-side holder:
 * credentials are bridged from the table-api {@code Account} so STS rotation keeps working,
 * and the route token issued by session calls is cached for block writers and commits.
 */
class V2WriteClientHolder extends V2ClientHolder {

    V2WriteClientHolder(TableIdentifier identifier, EnvironmentSettings settings) {
        super(identifier, settings);
    }

    com.aliyun.odps.storage.write.TableWriteSession writeSession(
            com.aliyun.odps.table.write.TableWriteSessionBuilder builder) throws IOException {
        DynamicPartitionOptions dynamicOptions =
                builder.getDynamicPartitionOptions();
        if (dynamicOptions == null) {
            dynamicOptions = DynamicPartitionOptions.createDefault();
        }
        TableWriteCapabilities capabilities = builder.getWriteCapabilities() == null
                ? TableWriteCapabilities.createDefault() : builder.getWriteCapabilities();
        TableWriteSessionBuilder v2Builder =
                new TableWriteSessionBuilder(stub(), null, identifier())
                        .withWriteMode(WriteMode.BATCH_COMPATIBLE)
                        .withOverwrite(builder.isOverwrite());
        if (builder.getTargetPartitionSpec() != null
                && !builder.getTargetPartitionSpec().isEmpty()) {
            v2Builder.withPartition(builder.getTargetPartitionSpec());
        }
        BatchCompatibleOptions options = BatchCompatibleOptions.newBuilder()
                .withSupportWriteCluster(capabilities.supportHashBuckets()
                        || capabilities.supportRangeBuckets())
                .withDynamicPartitionLimit(dynamicOptions.getDynamicPartitionLimit())
                .withInvalidStrategy(BatchCompatibleOptions.InvalidStrategy.valueOf(
                        dynamicOptions.getInvalidStrategy().name()))
                .withInvalidLimit((int) dynamicOptions.getInvalidLimit())
                .withMaxFieldSize(builder.getMaxFieldSize())
                .withEnhanceWriteCheck(builder.isEnhanceWriteCheck())
                .withArrowOptions(builder.getArrowOptions() == null
                        ? ArrowOptions.createDefault() : builder.getArrowOptions())
                .build();
        v2Builder.withBatchCompatibleOptions(options);
        com.aliyun.odps.storage.write.TableWriteSession session = v2Builder.build();
        updateRouteToken(session.getBatchCompatibleResponse() == null ? null
                : session.getBatchCompatibleResponse().getRouteToken());
        return session;
    }

    com.aliyun.odps.storage.write.TableWriteSession writeSession(String sessionId,
            ArrowOptions arrowOptions)
            throws IOException {
        TableWriteSessionBuilder v2Builder =
                new TableWriteSessionBuilder(stub(), null, identifier())
                        .withWriteMode(WriteMode.BATCH_COMPATIBLE)
                        .withSessionId(sessionId)
                        .withBatchCompatibleOptions(BatchCompatibleOptions.newBuilder()
                                .withArrowOptions(arrowOptions).build());
        com.aliyun.odps.storage.write.TableWriteSession session = v2Builder.build();
        updateRouteToken(session.getBatchCompatibleResponse() == null ? null
                : session.getBatchCompatibleResponse().getRouteToken());
        return session;
    }
}
