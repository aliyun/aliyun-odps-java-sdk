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

package com.aliyun.odps.table.enviroment;

import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.utils.Preconditions;

import java.io.Serializable;
import java.util.Optional;

/**
 * Environment settings for table read/write
 */
public class EnvironmentSettings {

    private final ExecutionMode executionMode;

    private final String quotaName;

    private final Credentials credentials;

    private final String defaultProject;

    private final String defaultSchema;

    private final String serviceEndpoint;

    private final String tunnelEndpoint;

    private final RestOptions restOptions;

    private final Long slotNum;

    // TODO: Environment Credentials

    private EnvironmentSettings(Builder builder) {
        this.executionMode = builder.executionMode;
        this.quotaName = builder.quotaName;
        this.credentials = builder.credentials;
        this.defaultProject = builder.defaultProject;
        this.defaultSchema = builder.defaultSchema;
        this.serviceEndpoint = builder.serviceEndPoint;
        this.tunnelEndpoint = builder.tunnelEndpoint;
        this.restOptions = builder.restOptions;
        this.slotNum = builder.slotNum;
    }

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public Optional<String> getQuotaName() {
        return Optional.ofNullable(quotaName);
    }

    public Optional<String> getDefaultProject() {
        return Optional.ofNullable(defaultProject);
    }

    public Optional<String> getDefaultSchema() {
        return Optional.ofNullable(defaultSchema);
    }

    public Optional<String> getServiceEndpoint() {
        return Optional.ofNullable(serviceEndpoint);
    }

    public Optional<String> getTunnelEndpoint() {
        return Optional.ofNullable(tunnelEndpoint);
    }

    public Optional<RestOptions> getRestOptions() {
        return Optional.ofNullable(restOptions);
    }

    public Optional<Long> getSlotNum() {
        return Optional.ofNullable(slotNum);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public enum ExecutionMode {
        REMOTE,
        LOCAL,
        AUTOMATIC
    }

    public static class Builder {
        private ExecutionMode executionMode = ExecutionMode.AUTOMATIC;
        private Credentials credentials;
        private String defaultProject;
        private String defaultSchema;
        private String quotaName;
        private String serviceEndPoint;
        private String tunnelEndpoint;
        private RestOptions restOptions;
        private Long slotNum;

        public Builder inLocalMode() {
            this.executionMode = ExecutionMode.LOCAL;
            return this;
        }

        public Builder inRemoteMode() {
            this.executionMode = ExecutionMode.REMOTE;
            return this;
        }

        public Builder inAutoMode() {
            this.executionMode = ExecutionMode.AUTOMATIC;
            return this;
        }

        public Builder withCredentials(Credentials credentials) {
            Preconditions.checkNotNull(credentials, "Credentials");
            this.credentials = credentials;
            return this;
        }

        public Builder withDefaultProject(String projectName) {
            Preconditions.checkString(projectName, "Project");
            this.defaultProject = projectName;
            return this;
        }

        public Builder withDefaultSchema(String schema) {
            Preconditions.checkString(schema, "Schema");
            this.defaultSchema = schema;
            return this;
        }

        public Builder withServiceEndpoint(String endPoint) {
            Preconditions.checkString(endPoint, "End point");
            this.serviceEndPoint = endPoint;
            return this;
        }

        public Builder withTunnelEndpoint(String endPoint) {
            Preconditions.checkString(endPoint, "Tunnel end point");
            this.tunnelEndpoint = endPoint;
            return this;
        }

        public Builder withQuotaName(String quotaName) {
            this.quotaName = quotaName;
            return this;
        }

        public Builder withRestOptions(RestOptions restOptions) {
            Preconditions.checkNotNull(restOptions, "Rest Options");
            this.restOptions = restOptions;
            return this;
        }

        public Builder withSlotNum(Long slotNum) {
            Preconditions.checkLong(slotNum, 1, "Slot num");
            this.slotNum = slotNum;
            return this;
        }

        public EnvironmentSettings build() {
            Preconditions.checkNotNull(credentials,
                    "Credentials", "required");
            return new EnvironmentSettings(this);
        }
    }
}
