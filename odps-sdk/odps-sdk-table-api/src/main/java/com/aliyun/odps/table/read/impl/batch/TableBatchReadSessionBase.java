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

package com.aliyun.odps.table.read.impl.batch;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.utils.Preconditions;

public abstract class TableBatchReadSessionBase implements TableBatchReadSession {

    private static final long serialVersionUID = 1L;

    protected transient List<PartitionSpec> requiredPartitions;

    protected transient List<String> requiredDataColumns;

    protected transient List<String> requiredPartitionColumns;

    protected transient List<Integer> requiredBucketIds;

    protected transient SplitOptions splitOptions;

    protected transient ArrowOptions arrowOptions;

    protected transient EnvironmentSettings settings;

    protected String sessionId;

    protected TableIdentifier identifier;

    protected DataSchema readSchema;

    protected long expirationTime;

    protected SessionStatus sessionStatus;

    protected InputSplitAssigner inputSplitAssigner;

    protected String errorMessage;

    protected Set<DataFormat> supportDataFormats;
    protected Predicate filterPredicate;

    public TableBatchReadSessionBase(TableIdentifier identifier,
                                     List<PartitionSpec> requiredPartitions,
                                     List<String> requiredDataColumns,
                                     List<String> requiredPartitionColumns,
                                     List<Integer> bucketIds,
                                     SplitOptions splitOptions,
                                     ArrowOptions arrowOptions,
                                     EnvironmentSettings settings,
                                     Predicate filterPredicate) throws IOException {
        Preconditions.checkNotNull(identifier, "Table identifier", "required");
        Preconditions.checkNotNull(settings, "Environment settings", "required");
        this.identifier = identifier;
        this.settings = settings;
        this.sessionStatus = SessionStatus.UNKNOWN;
        sanitize(requiredPartitions, requiredDataColumns,
                requiredPartitionColumns, bucketIds, splitOptions, arrowOptions, filterPredicate);

        planInputSplits();
    }

    public TableBatchReadSessionBase(TableIdentifier identifier,
                                     String sessionId,
                                     EnvironmentSettings settings) throws IOException {
        Preconditions.checkNotNull(identifier, "Table identifier", "required");
        Preconditions.checkNotNull(sessionId, "Table read session id", "required");
        Preconditions.checkNotNull(settings, "Environment settings", "required");
        this.identifier = identifier;
        this.sessionId = sessionId;
        this.settings = settings;
        this.sessionStatus = SessionStatus.UNKNOWN;

        reloadInputSplits();
    }

    protected abstract void planInputSplits() throws IOException;

    protected abstract String reloadInputSplits() throws IOException;

    private void sanitize(List<PartitionSpec> partitions,
                          List<String> dataColumns,
                          List<String> partitionColumns,
                          List<Integer> bucketIds,
                          SplitOptions splitOptions,
                          ArrowOptions arrowOptions,
                          Predicate filterPredicate) {
        this.requiredPartitions = partitions == null ? Collections.emptyList()
                : Collections.unmodifiableList(partitions);

        this.requiredDataColumns = dataColumns == null ? Collections.emptyList()
                : Collections.unmodifiableList(dataColumns);

        this.requiredPartitionColumns = partitionColumns == null ? Collections.emptyList()
                : Collections.unmodifiableList(partitionColumns);

        this.requiredBucketIds = bucketIds == null ? Collections.emptyList()
                : Collections.unmodifiableList(bucketIds);

        this.splitOptions = splitOptions == null ? SplitOptions.createDefault() : splitOptions;

        this.arrowOptions = arrowOptions == null ? ArrowOptions.createDefault() : arrowOptions;

        this.filterPredicate = filterPredicate == null ? Predicate.NO_PREDICATE : filterPredicate;

        Preconditions.checkIntList(
                this.requiredBucketIds, 0, 0, "requiredBucketIds");
        Preconditions.checkList(this.requiredPartitions, 0, "partitionsFilters");
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner() {
        if (this.inputSplitAssigner != null) {
            return this.inputSplitAssigner;
        } else {
            throw new IllegalStateException(
                    "The table read has not been initialized yet");
        }
    }

    @Override
    public String getId() {
        if (this.sessionId != null) {
            return this.sessionId;
        } else {
            throw new IllegalStateException(
                    "The table read has not been initialized yet");
        }
    }

    @Override
    public DataSchema readSchema() {
        if (this.readSchema != null) {
            return this.readSchema;
        } else {
            throw new IllegalStateException(
                    "The table read has not been initialized yet");
        }
    }

    @Override
    public TableIdentifier getTableIdentifier() {
        return this.identifier;
    }

    @Override
    public SessionStatus getStatus() {
        return this.sessionStatus;
    }
}
