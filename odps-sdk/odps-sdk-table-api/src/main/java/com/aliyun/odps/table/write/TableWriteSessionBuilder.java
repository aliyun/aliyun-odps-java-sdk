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

package com.aliyun.odps.table.write;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.utils.SessionUtils;
import com.aliyun.odps.table.write.impl.TableWriteSessionProviderImpl;

import java.io.IOException;

/**
 * An interface for building the {@link TableWriteSession}.
 */
public class TableWriteSessionBuilder {

    private TableIdentifier identifier;
    private PartitionSpec targetPartitionSpec;
    private boolean overwrite = false;
    private DynamicPartitionOptions dynamicPartitionOptions;
    private ArrowOptions arrowOptions;
    private TableWriteCapabilities writeCapabilities;
    private EnvironmentSettings settings;
    private String sessionId;
    private String sessionProvider;

    /**
     * Set the write target to a specific table by {@link Table}.
     */
    public TableWriteSessionBuilder table(Table table) {
        this.identifier = TableIdentifier.of(table.getProject(),
                table.getSchemaName(), table.getName());
        return this;
    }

    /**
     * Set the write target to a specific table by {@link TableIdentifier}.
     */
    public TableWriteSessionBuilder identifier(TableIdentifier identifier) {
        this.identifier = identifier;
        return this;
    }

    /**
     * Set the write target to a specific/dynamic partition by {@link PartitionSpec}.
     */
    public TableWriteSessionBuilder partition(PartitionSpec partitionSpec) {
        this.targetPartitionSpec = partitionSpec;
        return this;
    }

    /**
     * Configures a write to replace all existing data with data committed in the write.
     */
    public TableWriteSessionBuilder overwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    /**
     * Set the dynamic partition options {@link DynamicPartitionOptions}.
     */
    public TableWriteSessionBuilder withDynamicPartitionOptions(DynamicPartitionOptions options) {
        this.dynamicPartitionOptions = options;
        return this;
    }

    /**
     * Set the arrow options {@link ArrowOptions}.
     */
    public TableWriteSessionBuilder withArrowOptions(ArrowOptions options) {
        this.arrowOptions = options;
        return this;
    }

    /**
     * Set the table write capabilities
     */
    public TableWriteSessionBuilder withCapabilities(TableWriteCapabilities capabilities) {
        this.writeCapabilities = capabilities;
        return this;
    }

    /**
     * Set the table environment settings {@link EnvironmentSettings}.
     */
    public TableWriteSessionBuilder withSettings(EnvironmentSettings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Set the table write session id.
     */
    public TableWriteSessionBuilder withSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    /**
     * Set the table write session provider.
     */
    public TableWriteSessionBuilder withSessionProvider(String provider) {
        this.sessionProvider = provider;
        return this;
    }

    /**
     * Returns a logical {@link TableBatchWriteSession}.
     */
    public TableBatchWriteSession buildBatchWriteSession() throws IOException {
        return getProvider().createBatchWriteSession(this);
    }

    /**
     * Returns a logical {@link TableStreamingWriteSession}.
     */
    public TableStreamingWriteSession buildStreamingWriteSession() throws IOException {
        return getProvider().createStreamingWriteSession(this);
    }

    /**
     * Returns a logical {@link TableUpsertSession}.
     */
    public TableUpsertSession buildUpsertSession() throws IOException {
        return getProvider().createUpsertSession(this);
    }

    public TableIdentifier getIdentifier() {
        return identifier;
    }

    public PartitionSpec getTargetPartitionSpec() {
        return targetPartitionSpec;
    }

    public String getSessionId() {
        return sessionId;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public EnvironmentSettings getSettings() {
        return settings;
    }

    public ArrowOptions getArrowOptions() {
        return arrowOptions;
    }

    public DynamicPartitionOptions getDynamicPartitionOptions() {
        return dynamicPartitionOptions;
    }

    public TableWriteCapabilities getWriteCapabilities() {
        return writeCapabilities;
    }

    private TableWriteSessionProvider getProvider() throws IOException {
        try {
            return sessionProvider != null
                    ? SessionUtils.discoverSessionProvider(
                    TableWriteSessionBuilder.class.getClassLoader(),
                    TableWriteSessionProvider.class,
                    sessionProvider)
                    : new TableWriteSessionProviderImpl();
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
