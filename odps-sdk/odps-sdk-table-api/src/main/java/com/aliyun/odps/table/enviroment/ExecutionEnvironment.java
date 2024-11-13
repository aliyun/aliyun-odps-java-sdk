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

import com.aliyun.odps.Odps;
import com.aliyun.odps.commons.transport.DefaultTransport;
import com.aliyun.odps.rest.RestClient;

import java.net.URI;
import java.net.URISyntaxException;

import static com.aliyun.odps.table.utils.ConfigConstants.*;

public abstract class ExecutionEnvironment {

    protected final EnvironmentSettings settings;

    private boolean initialized = false;

    public ExecutionEnvironment(EnvironmentSettings settings) {
        this.settings = settings;
    }

    public static ExecutionEnvironment create(EnvironmentSettings settings) {
        switch (settings.getExecutionMode()) {
            case REMOTE:
                return new RemoteEnvironment(settings);
            case LOCAL:
                return new LocalEnvironment(settings);
            case AUTOMATIC:
                return createDefaultEnvironment(settings);
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Initialize if not done previously.
     **/
    protected void ensureInitialized() {
        if (!initialized) {
            initialize();
            initialized = true;
        }
    }

    abstract protected void initialize();

    /**
     * Get service endpoint
     *
     * @return String
     */
    public String getServiceEndpoint() {
        return settings.getServiceEndpoint().orElseThrow(() ->
                new IllegalStateException("Get empty service endpoint from execution environment!"));
    }

    /**
     * Get tunnel endpoint
     *
     * @return String
     */
    abstract public String getTunnelEndpoint(String targetProject);


    /**
     * Get environment settings
     *
     * @return {@link EnvironmentSettings}.
     */
    public EnvironmentSettings getSettings() {
        return settings;
    }

    /**
     * Create http client
     *
     * @return {@link RestClient}.
     */
    public RestClient createHttpClient(String targetProject) {
        ensureInitialized();

        Credentials credentials = settings.getCredentials();
        RestClient restClient = new RestClient(new DefaultTransport());
        restClient.setAccount(credentials.getAccount());
        credentials.getAppAccount().ifPresent(restClient::setAppAccount);
        credentials.getAppStsAccount().ifPresent(restClient::setAppStsAccount);
        try {
            URI endpoint = new URI(getTunnelEndpoint(targetProject));
            restClient.setEndpoint(endpoint.toString());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Execution environment get tunnel endpoint error:" + e.getMessage());
        }
        setRestOptions(restClient);
        return restClient;
    }

    /**
     * Create odps client
     *
     * @return {@link Odps}.
     */
    public Odps createOdpsClient() {
        Credentials credentials = settings.getCredentials();
        Odps odps = new Odps(credentials.getAccount());
        settings.getDefaultProject().ifPresent(odps::setDefaultProject);
        settings.getDefaultSchema().ifPresent(odps::setCurrentSchema);
        credentials.getAppAccount().ifPresent(odps::setAppAccount);
        credentials.getAppStsAccount().ifPresent(odps::setAppStsAccount);
        try {
            URI endpoint = new URI(getServiceEndpoint());
            odps.setEndpoint(endpoint.toString());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Execution environment get service endpoint error:" + e.getMessage());
        }
        setRestOptions(odps.getRestClient());
        return odps;
    }

    private void setRestOptions(RestClient restClient) {
        restClient.setUserAgent("StorageAPI");
        settings.getDefaultProject().ifPresent(restClient::setDefaultProject);
        settings.getDefaultSchema().ifPresent(restClient::setCurrentSchema);
        settings.getRestOptions().ifPresent(restOptions -> {
            restOptions.getConnectTimeout().ifPresent(restClient::setConnectTimeout);
            restOptions.getReadTimeout().ifPresent(restClient::setReadTimeout);
            restOptions.getUserAgent().ifPresent(restClient::setUserAgent);
            restOptions.getRetryTimes().ifPresent(restClient::setRetryTimes);
            restOptions.getRetryWaitTimeInSeconds().ifPresent(restClient::setRetryWaitTime);
            restOptions.isIgnoreCerts().ifPresent(restClient::setIgnoreCerts);
        });
    }

    private static ExecutionEnvironment createDefaultEnvironment(EnvironmentSettings settings) {
        String mode = System.getenv(MAX_STORAGE_MODE);
        if (MAX_STORAGE_VSHM_MODE.equals(mode) ||
                MAX_STORAGE_NET_MODE.equals(mode)) {
            return new LocalEnvironment(settings);
        }
        return new RemoteEnvironment(settings);
    }
}
