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

package com.aliyun.odps.table.read;

import java.io.IOException;
import java.util.List;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.read.impl.TableReadSessionProviderImpl;
import com.aliyun.odps.table.utils.SessionUtils;

/**
 * An interface for building the {@link TableReadSession}.
 */
public class TableReadSessionBuilder {

    private TableIdentifier identifier;
    private List<String> requiredDataColumns;
    private List<String> requiredPartitionColumns;
    private List<PartitionSpec> requiredPartitions;
    private List<Integer> requiredBucketIds;
    private SplitOptions splitOptions;
    private ArrowOptions arrowOptions;
    private EnvironmentSettings settings;
    private String sessionId;
    private String sessionProvider;
    private Predicate filterPredicate;
    private String detailsJson;

    /**
     * Set the read session target to a specific table by {@link Table}.
     */
    public TableReadSessionBuilder table(Table table) {
        this.identifier = TableIdentifier.of(table.getProject(),
                table.getSchemaName(), table.getName());
        return this;
    }

    /**
     * Set the read session target to a specific table by {@link TableIdentifier}.
     */
    public TableReadSessionBuilder identifier(TableIdentifier identifier) {
        this.identifier = identifier;
        return this;
    }

    /**
     * Set read schema by specifying top-level column names.
     * TODO: For nested column-pruning
     */
    public TableReadSessionBuilder requiredDataColumns(List<String> requiredDataColumns) {
        this.requiredDataColumns = requiredDataColumns;
        return this;
    }

    /**
     * Set read schema by partition column names.
     */
    public TableReadSessionBuilder requiredPartitionColumns(List<String> requiredPartitionColumns) {
        this.requiredPartitionColumns = requiredPartitionColumns;
        return this;
    }

    /**
     * Specify input partitions by string. Part spec format: ds=1/pt=a.
     */
    public TableReadSessionBuilder requiredPartitions(List<PartitionSpec> requiredPartitions) {
        this.requiredPartitions = requiredPartitions;
        return this;
    }

    /**
     * Set read buckets for clustering table.
     */
    public TableReadSessionBuilder requiredBucketIds(List<Integer> requiredBucketIds) {
        this.requiredBucketIds = requiredBucketIds;
        return this;
    }

    /**
     * Set the split options {@link SplitOptions}.
     */
    public TableReadSessionBuilder withSplitOptions(SplitOptions splitOptions) {
        this.splitOptions = splitOptions;
        return this;
    }

    /**
     * Set the arrow options {@link ArrowOptions}.
     */
    public TableReadSessionBuilder withArrowOptions(ArrowOptions arrowOptions) {
        this.arrowOptions = arrowOptions;
        return this;
    }

    /**
     * Set the table environment settings {@link EnvironmentSettings}.
     */
    public TableReadSessionBuilder withSettings(EnvironmentSettings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Set the table read session id.
     */
    public TableReadSessionBuilder withSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    /**
     * Set the table session provider
     */
    public TableReadSessionBuilder withSessionProvider(String provider) {
        this.sessionProvider = provider;
        return this;
    }

    /**
     * predicate use to push down
     */
    public TableReadSessionBuilder withFilterPredicate(Predicate filterPredicate) {
        this.filterPredicate = filterPredicate;
        return this;
    }

    /**
     * Initialize the session from json string
     */
    public TableReadSessionBuilder fromJson(String jsonString) {
        this.detailsJson = jsonString;
        return this;
    }

    public String getSessionId() {
        return sessionId;
    }

    public SplitOptions getSplitOptions() {
        return splitOptions;
    }

    public TableIdentifier getIdentifier() {
        return identifier;
    }

    public List<Integer> getRequiredBucketIds() {
        return requiredBucketIds;
    }

    public List<String> getRequiredDataColumns() {
        return requiredDataColumns;
    }

    public ArrowOptions getArrowOptions() {
        return arrowOptions;
    }

    public EnvironmentSettings getSettings() {
        return settings;
    }

    public List<PartitionSpec> getRequiredPartitions() {
        return requiredPartitions;
    }

    public List<String> getRequiredPartitionColumns() {
        return requiredPartitionColumns;
    }

    public Predicate getFilterPredicate() {
        return filterPredicate;
    }

    public String getDetailsJson() {
        return detailsJson;
    }

    /**
     * Returns a logical {@link TableBatchReadSession}.
     */
    public TableBatchReadSession buildBatchReadSession() throws IOException {
        return getProvider().createBatchReadSession(this);
    }

    private TableReadSessionProvider getProvider() throws IOException {
        try {
            return sessionProvider != null
                    ? SessionUtils.discoverSessionProvider(
                    TableReadSessionBuilder.class.getClassLoader(),
                    TableReadSessionProvider.class,
                    sessionProvider)
                    : new TableReadSessionProviderImpl();
        } catch (ClassNotFoundException e) {
            throw new IOException(
                    String.format(
                            "Unable to create a table batch read session for reading table '%s'.\n\n"
                                    + "Session provider are:\n\n"
                                    + "%s",
                            identifier.toString(), sessionProvider),
                    e);
        }
    }
}
