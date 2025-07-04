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

package com.aliyun.odps.table.write.impl.batch;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.distribution.Distribution;
import com.aliyun.odps.table.distribution.UnspecifiedDistribution;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.order.SortOrder;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteCapabilities;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;

public abstract class TableBatchWriteSessionBase implements TableBatchWriteSession {

    protected transient EnvironmentSettings settings;

    protected PartitionSpec targetPartitionSpec;

    protected boolean overwrite;

    protected DynamicPartitionOptions dynamicPartitionOptions;

    protected ArrowOptions arrowOptions;

    protected TableWriteCapabilities writeCapabilities;

    protected String sessionId;

    protected TableIdentifier identifier;

    protected SessionStatus sessionStatus;

    protected long expirationTime;

    protected String errorMessage;

    protected DataSchema requiredSchema;

    protected SortOrder[] requiredSortOrders;

    protected Distribution requiredDistribution;

    protected long maxBlockNumber;

    protected Set<DataFormat> supportDataFormats;

    protected long maxFieldSize;

    protected boolean enhanceWriteCheck;

    public TableBatchWriteSessionBase(TableWriteSessionBuilder builder) throws IOException {
        this.identifier = Preconditions.checkNotNull(builder.getIdentifier(), "Table identifier", "required");
        this.settings = Preconditions.checkNotNull(builder.getSettings(), "Environment settings", "required");
        if (builder.getSessionId() == null) {
            this.overwrite = builder.isOverwrite();
            this.enhanceWriteCheck = builder.isEnhanceWriteCheck();
            sanitize(builder.getTargetPartitionSpec(),
                    builder.getDynamicPartitionOptions(),
                    builder.getArrowOptions(),
                    builder.getWriteCapabilities(),
                    builder.getMaxFieldSize());
            initSession();
        } else {
            this.sessionId = builder.getSessionId();
            reloadSession();
        }
    }

    protected abstract void initSession() throws IOException;

    protected abstract String reloadSession() throws IOException;

    private void sanitize(PartitionSpec partitionSpec,
                          DynamicPartitionOptions dynamicPartitionOptions,
                          ArrowOptions arrowOptions,
                          TableWriteCapabilities writeCapabilities,
                          Long maxFieldSize) {
        this.targetPartitionSpec = partitionSpec == null ?
                new PartitionSpec() : partitionSpec;
        this.dynamicPartitionOptions = dynamicPartitionOptions == null ?
                DynamicPartitionOptions.createDefault() : dynamicPartitionOptions;
        this.arrowOptions = arrowOptions == null ?
                ArrowOptions.createDefault() : arrowOptions;
        this.writeCapabilities = writeCapabilities == null ?
                TableWriteCapabilities.createDefault() : writeCapabilities;
        this.maxFieldSize = maxFieldSize == null ? 8 * 1024 * 1024L : maxFieldSize;
    }

    @Override
    public String getId() {
        if (this.sessionId != null) {
            return this.sessionId;
        } else {
            throw new IllegalStateException(
                    "The table sink has not been initialized yet");
        }
    }

    @Override
    public DataSchema requiredSchema() {
        if (this.requiredSchema != null) {
            return this.requiredSchema;
        } else {
            throw new IllegalStateException(
                    "The table sink has not been initialized yet");
        }
    }

    @Override
    public Optional<Long> maxBlockNumber() {
        return maxBlockNumber > 0 ?
                Optional.of(maxBlockNumber) : Optional.empty();
    }

    @Override
    public Distribution requiredDistribution() {
        return requiredDistribution != null ?
                requiredDistribution : new UnspecifiedDistribution();
    }

    @Override
    public SortOrder[] requiredOrdering() {
        return requiredSortOrders != null ?
                requiredSortOrders : new SortOrder[]{};
    }

    @Override
    public TableIdentifier getTableIdentifier() {
        return identifier;
    }

    @Override
    public SessionStatus getStatus() {
        return sessionStatus;
    }
}
